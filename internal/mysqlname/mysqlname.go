// Package mysqlname derives the names MySQL and MariaDB give a foreign key a
// statement leaves unnamed, `<table>_ibfk_<n>`, and recognizes the name of the
// index the server builds for one.
//
// Three parts of Ptah need the same answer. The SQL parser reads which object
// the name written inside a FOREIGN KEY clause names. The SQL schema reader
// names an unnamed foreign key the way the server does, so a file compares
// equal to the database its own SQL built. The schema comparison recognizes the
// key's backing index by the name the server gave it, so it does not plan to
// drop an index the key needs. One implementation serves all three: two copies
// of the rule agree when the second is written and drift when the first
// changes.
//
// The rules were measured on MySQL 8.4.11, MySQL 26.7.0 and MariaDB 11.8.9,
// and the cut of a long index name on MySQL 9.7.2. The engines agree on every
// rule here except the two [ClauseIndexNamesKey] and [IsUnnamedKeyIndexName]
// state. [NamesForeignKeys] says which dialects the package speaks for.
//
// MariaDB changed the name at 12.1 and kept the numbering. Measured on
// 10.11.19, 11.4.13, 11.8.9 and 12.0.2, an unnamed key is `<table>_ibfk_<n>`;
// on 12.1.2, 12.2.2 and 12.3.3 it is `<n>`, and the name belongs to the table
// rather than to the database. A schema file does not say which line it is for,
// so the reader derives the first name, and the comparison accepts the second
// for the same key; see [IsNumberedForeignKeyName].
package mysqlname

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"ptah.run/core/platform"
)

// foreignKeyLabel sits between a table's name and the number of a foreign key
// the server named.
const foreignKeyLabel = "_ibfk_"

// IndexBaseBytes is how much of a column name MySQL keeps before the `_<n>` it
// appends to an index name that is taken: 61 bytes, whatever the suffix costs.
// Measured on MySQL 9.7.2, a 64-character column yields a 63-character `_2`
// and a 64-character `_10`. MariaDB does not cut the name; see
// [IsUnnamedKeyIndexName].
const IndexBaseBytes = 61

// NamesForeignKeys reports whether a source of dialect names an unnamed foreign
// key and its backing index by the rules of this package: MySQL and MariaDB.
//
// The SQL reader and the schema comparison both ask it, so the set of engines
// the rule speaks for is decided once.
func NamesForeignKeys(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

// ClauseIndexNamesKey reports whether the identifier the MySQL family allows
// between FOREIGN KEY and its column list names the key itself on dialect.
//
// The two engines disagree. Measured, `FOREIGN KEY idx_c_p (p_id) REFERENCES
// p(id)` with no CONSTRAINT symbol, in CREATE TABLE and in ALTER TABLE ... ADD:
//
//	engine          key          index
//	MySQL 8.4.11    c_ibfk_1     idx_c_p
//	MySQL 26.7.0    c_ibfk_1     idx_c_p
//	MariaDB 11.8.9  idx_c_p      idx_c_p
//
// So on MariaDB the identifier is the key's name, and on MySQL it names only
// the index the server builds for the key, which is then named like any other
// unnamed key. With a symbol, `CONSTRAINT sym FOREIGN KEY idx_c_p (p_id)`, both
// engines name the key and its index `sym` and keep nothing of `idx_c_p`.
func ClauseIndexNamesKey(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.MariaDB
}

// ForeignKey answers the name the server gives the foreign key numbered n of
// table: `<table>_ibfk_<n>`, with the table spelled as it was declared.
//
// Which number the server picks depends on the statement. In CREATE TABLE the
// unnamed keys are numbered from 1 in the order they are written, and a named
// key does not move the count: `CONSTRAINT c_ibfk_5 FOREIGN KEY (x) ...,
// FOREIGN KEY (y) ...` names the second `c_ibfk_1`. A column-level
// `REFERENCES`, which MySQL 26.7 and MariaDB turn into a key and MySQL 8.4
// ignores, counts at the column's place in that order. ALTER TABLE numbers
// from [NextForeignKeyNumber].
//
// The server does not move the name out of the way of another constraint: an
// explicit `c_ibfk_1` beside an unnamed key of `c`, on the same table or on
// another, is refused. MySQL answers `ERROR 1826 (HY000): Duplicate foreign
// key constraint name`; MariaDB answers `ERROR 1005 (HY000)` with errno 150 on
// the same table and errno 121 on another. It does not shorten the name
// either: a table name longer than 57 characters on MySQL, and longer than 56
// in a MariaDB CREATE TABLE, is `ERROR 1059 (42000): Identifier name ... is
// too long`.
func ForeignKey(table string, n int) string {
	return table + foreignKeyLabel + strconv.Itoa(n)
}

// ForeignKeyNumber answers the number of a foreign key name ALTER TABLE counts
// for table, and false for a name it does not count.
//
// It counts `<table>_ibfk_<n>` where the table is spelled exactly as declared
// and n is a positive number written without a leading zero. Measured on every
// engine the package speaks for, the name of an unnamed key a later ALTER TABLE
// adds to a table that holds these keys:
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

// IsNumberedForeignKeyName reports whether name is one MariaDB 12.1 and later
// give a foreign key left unnamed: a positive number written without a leading
// zero, which is the only way the server writes it.
//
// Those lines number unnamed keys by the rules [ForeignKey] and
// [NextForeignKeyNumber] state and write only the number. Measured on 12.1.2,
// 12.2.2 and 12.3.3, with the name `<n>` where MariaDB 12.0.2 and earlier
// write `c_ibfk_<n>`:
//
//	CREATE TABLE c (... FOREIGN KEY (p_id) ...)                        1
//	CREATE TABLE c (... CONSTRAINT c_ibfk_5 FOREIGN KEY (x) ...), then
//	  ALTER TABLE c ADD FOREIGN KEY (y) ...                           1
//	CONSTRAINT `3` and an unnamed key, then ALTER TABLE ... ADD        1, then 4
//
// So the two schemes number one table's keys differently as soon as it holds
// a key named in the other scheme, and a name of one cannot be turned into
// the other. Unlike the older scheme, the name is the table's own: an
// unnamed key of `c` beside `CONSTRAINT c_ibfk_1` on another table, or on `c`
// itself, is accepted.
func IsNumberedForeignKeyName(dialect, name string) bool {
	return platform.NormalizeDialect(dialect) == platform.MariaDB && isNumber(name)
}

// IsServerForeignKeyName reports whether name is one the server of dialect
// could have given a foreign key of table that the statement left unnamed, in
// either scheme the package describes.
func IsServerForeignKeyName(dialect, table, name string) bool {
	if !NamesForeignKeys(dialect) {
		return false
	}
	return IsForeignKeyName(table, name) || IsNumberedForeignKeyName(dialect, name)
}

// NextForeignKeyNumber answers the number ALTER TABLE gives the first unnamed
// foreign key one statement adds to table: one more than the highest number
// [ForeignKeyNumber] finds among held, and 1 when it finds none. Each further
// unnamed key of the same statement takes the next number.
//
// held is every foreign key name the table holds before the statement runs.
// Measured on every engine the package speaks for, a key the statement drops
// still counts, and a key the statement adds under a name does not, whether it
// is written before the unnamed one or after it. Below, `FK (x)` stands for
// `FOREIGN KEY (x) REFERENCES p(id)`:
//
//	table holds   ALTER TABLE c ...                                   unnamed keys
//	c_ibfk_1      ADD CONSTRAINT c_ibfk_9 FK (a), ADD FK (b)          c_ibfk_2
//	c_ibfk_1      ADD CONSTRAINT FK (b), ADD CONSTRAINT c_ibfk_9 FK (a)   c_ibfk_2
//	c_ibfk_3      DROP FOREIGN KEY c_ibfk_3, ADD FK (b)               c_ibfk_4
//	c_ibfk_2      ADD COLUMN b INT REFERENCES p(id), ADD FK (a)       c_ibfk_3, c_ibfk_4
//
// The last row is MySQL 26.7 and MariaDB; MySQL 8.4 ignores the column-level
// REFERENCES and names the other key `c_ibfk_3`. A key a previous statement
// dropped does not count, because the table no longer holds it.
func NextForeignKeyNumber(table string, held []string) int {
	highest := 0
	for _, name := range held {
		if n, ok := ForeignKeyNumber(table, name); ok && n > highest {
			highest = n
		}
	}
	return highest + 1
}

// IsUnnamedKeyIndexName reports whether index is a name the server gives the
// index it builds for a foreign key written without a name, whose first column
// is column.
//
// Such an index is named after the column, and `_2`, `_3` and on follow the
// column's name when the name is taken. Measured on MySQL 8.4.11, 26.7.0 and
// MariaDB 11.8.9: `FOREIGN KEY (p_id)` builds `p_id`, `FOREIGN KEY (a, b)`
// builds `a`, and a table already holding an index called `p_id` gets `p_id_2`.
// MySQL cuts the column's name to 61 bytes before the number, and MariaDB does
// not cut it, so both spellings are the server's. A key with a name builds an
// index under the key's name instead. same decides when two names are one,
// because the server compares index names without case.
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
	return same(index[:cut], column) || same(index[:cut], indexBase(column))
}

// indexBase is the part of a column name MySQL keeps before it appends a
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
