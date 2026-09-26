// Package mysqlname derives the names MySQL gives a foreign key a statement
// leaves unnamed, `<table>_ibfk_<n>`, and recognizes the name of the index the
// server builds for one.
//
// Two parts of Ptah need the same answer. The SQL schema reader names an
// unnamed foreign key the way the server does, so a file compares equal to the
// database its own SQL built. The schema comparison recognizes the key's
// backing index by the name the server gave it, so it does not plan to drop an
// index the key needs. One implementation serves both: two copies of the rule
// agree when the second is written and drift when the first changes.
//
// The rules were measured on MySQL 8.4.11, and the cut of a long index name on
// 9.7.2. MariaDB was not measured, and nothing here speaks for it.
package mysqlname

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// foreignKeyLabel sits between a table's name and the number of a foreign key
// the server named.
const foreignKeyLabel = "_ibfk_"

// IndexBaseBytes is how much of a column name the server keeps before the
// `_<n>` it appends to an index name that is taken: 61 bytes, whatever the
// suffix costs. Measured on MySQL 9.7.2, a 64-character column yields a
// 63-character `_2` and a 64-character `_10`.
const IndexBaseBytes = 61

// ForeignKey answers the name MySQL gives the foreign key numbered n of table:
// `<table>_ibfk_<n>`, with the table spelled as it was declared.
//
// Which number the server picks depends on the statement. In CREATE TABLE the
// unnamed keys are numbered from 1 in the order they are written, and a named
// key does not move the count: `CONSTRAINT c_ibfk_5 FOREIGN KEY (x) ...,
// FOREIGN KEY (y) ...` names the second `c_ibfk_1`. ALTER TABLE takes one more
// than the highest number [ForeignKeyNumber] finds among the table's keys.
//
// The server does not move the name out of the way of another constraint: an
// explicit `c_ibfk_1` beside an unnamed key of `c`, on the same table or on
// another, is `ERROR 1826 (HY000): Duplicate foreign key constraint name`. It
// does not shorten it either: a table name longer than 57 characters is
// `ERROR 1059 (42000): Identifier name ... is too long`.
func ForeignKey(table string, n int) string {
	return table + foreignKeyLabel + strconv.Itoa(n)
}

// ForeignKeyNumber answers the number of a foreign key name ALTER TABLE counts
// for table, and false for a name it does not count.
//
// It counts `<table>_ibfk_<n>` where the table is spelled exactly as declared
// and n is a positive number written without a leading zero. Measured, the
// name of an unnamed key added to a table that holds these keys:
//
//	table      keys held                            added key
//	a4         a4_ibfk_1, a4_ibfk_5                 a4_ibfk_6
//	a2         a2_ibfk_2, after a2_ibfk_1 dropped   a2_ibfk_3
//	a9         fk_a9                                a9_ibfk_1
//	MixedAlt   mixedalt_ibfk_3                      MixedAlt_ibfk_1
//	f1         f1_ibfk_x                            f1_ibfk_1
//	f2         f2_ibfk_07                           f2_ibfk_1
func ForeignKeyNumber(table, name string) (int, bool) {
	digits, found := strings.CutPrefix(name, table+foreignKeyLabel)
	if !found || !isNumber(digits) {
		return 0, false
	}
	n, err := strconv.Atoi(digits)
	if err != nil {
		return 0, false
	}
	return n, true
}

// IsForeignKeyName reports whether name is one the server could have given a
// foreign key of table. See [ForeignKeyNumber] for the shape it answers.
func IsForeignKeyName(table, name string) bool {
	_, ok := ForeignKeyNumber(table, name)
	return ok
}

// IsUnnamedKeyIndexName reports whether index is a name the server gives the
// index it builds for a foreign key written without a name, whose first column
// is column.
//
// Such an index is named after the column, and `_2`, `_3` and on follow the
// column's name, cut to 61 bytes, when the name is taken. Measured on 8.4.11:
// `FOREIGN KEY (p_id)` builds `p_id`, `FOREIGN KEY (a, b)` builds `a`, and a
// table already holding an index called `p_id` gets `p_id_2`. A key with a
// name builds an index under the key's name instead. same decides when two
// names are one, because the server compares index names without case.
func IsUnnamedKeyIndexName(column, index string, same func(a, b string) bool) bool {
	if column == "" {
		return false
	}
	if same(index, column) {
		return true
	}
	cut := strings.LastIndexByte(index, '_')
	if cut < 0 || !isNumber(index[cut+1:]) || index[cut+1:] == "1" {
		return false
	}
	return same(index[:cut], indexBase(column))
}

// indexBase is the part of a column name the server keeps before it appends a
// number, cut on a character boundary.
func indexBase(column string) string {
	if len(column) <= IndexBaseBytes {
		return column
	}
	cut := IndexBaseBytes
	for cut > 0 && !utf8.RuneStart(column[cut]) {
		cut--
	}
	return column[:cut]
}

// isNumber reports whether digits is a positive decimal number written without
// a leading zero.
func isNumber(digits string) bool {
	if digits == "" || digits[0] == '0' {
		return false
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}
