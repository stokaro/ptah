// Package mysqlname derives the names MySQL and MariaDB give a foreign key or
// a CHECK a statement leaves unnamed, and recognizes the name of the index the
// server builds for a foreign key.
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
// and the cut of a long index name on MySQL 9.7.2. The package speaks for a
// dialect whose descriptor states descriptor.ForeignKeyNamingTableNumbered as
// its ForeignKeyNaming, and a caller reads the descriptor before it asks the
// package. Where the engines of that naming differ, the descriptor states the
// difference, and the caller passes what it states: the ForeignKeyNameRules,
// the IdentifierLimit and the IndexNaming's BaseBytes. Whether the name
// written between FOREIGN KEY and its columns names the key is the parser's
// to read, from Grammar.Keys.ForeignKeyIndexNameNamesKey.
//
// MariaDB changed the name at 12.1 and kept the numbering. Measured on
// 10.11.19, 11.4.13, 11.8.9 and 12.0.2, an unnamed key is `<table>_ibfk_<n>`;
// on 12.1.2, 12.2.2 and 12.3.3 it is `<n>`, and the name belongs to the table
// rather than to the database. A schema file does not say which line it is for,
// so the reader derives the first name, and the comparison accepts the second
// for the same key; see [IsNumberedForeignKeyName].
//
// The CHECK rules were measured on MySQL 8.4.11 and 26.7.0 and on MariaDB
// 11.8.9; see [Check] and [MariaDBCheck].
package mysqlname

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"ptah.run/core/platform/descriptor"
)

// foreignKeyLabel sits between a table's name and the number of a foreign key
// the server named.
const foreignKeyLabel = "_ibfk_"

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
// the same table and errno 121 on another. A name that is too long is refused
// or cut; see [ForeignKeyNameLimit].
func ForeignKey(table string, n int) string {
	return table + foreignKeyLabel + strconv.Itoa(n)
}

// Statement is the kind of statement that declares an unnamed foreign key. The
// longest name the server keeps for the key depends on it; see
// [ForeignKeyNameLimit].
type Statement int

const (
	// CreateTable is a key a CREATE TABLE body declares, on a column or on the
	// table.
	CreateTable Statement = iota
	// AlterTable is a key ALTER TABLE adds.
	AlterTable
)

// ForeignKeyNameLimit answers the longest name, in characters, that a server
// keeps as it derived it for an unnamed foreign key the statement declares:
// the CreateTableMaxChars of rules in CREATE TABLE where rules state one, and
// otherwise the Max of limit, the dialect's IdentifierLimit, which a
// descriptor of this naming states in characters.
//
// MySQL refuses a longer name in either statement with `ERROR 1059 (42000):
// Identifier name ... is too long`. MariaDB through 12.0 refuses a name of 64
// characters, its identifier limit, in CREATE TABLE with the same error, and
// in ALTER TABLE cuts a longer name to 64 without a warning. Measured over a
// utf8mb4 connection, with the length of the derived name:
//
//	statement                     name  MySQL 8.4.11, 26.7.0  MariaDB 11.8.9
//	CREATE TABLE, 56-char table   63    kept                  kept
//	CREATE TABLE, 56-char table   64    kept                  ERROR 1059
//	CREATE TABLE, 57-char table   64    kept                  ERROR 1059
//	ALTER TABLE, 57-char table    64    kept                  kept
//	ALTER TABLE, 58-char table    65    ERROR 1059            cut to 64
//
// MariaDB's CREATE TABLE refusal does not count characters: a 64-character
// name whose table is spelled in two-byte characters is kept. The limit here
// holds every such name to 63 characters, which refuses nothing MariaDB keeps
// among single-byte names.
//
// A name the server refuses is not a document, and a name it cuts is not the
// name the reader derives, so the reader refuses both. MariaDB 12.1 and later
// write the key's number instead of this name and keep the document; the
// reader cannot tell the lines apart, and the refusal holds on those lines
// too.
func ForeignKeyNameLimit(rules descriptor.ForeignKeyNameRules, limit descriptor.IdentifierLimit, statement Statement) int {
	if statement == CreateTable && rules.CreateTableMaxChars != 0 {
		return rules.CreateTableMaxChars
	}
	return limit.Max
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

// IsNumberedForeignKeyName reports whether name is one a server whose rules
// state BareNumbers gives a foreign key left unnamed: a positive number
// written without a leading zero, which is the only way the server writes it.
// MariaDB 12.1 and later name a key so.
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
func IsNumberedForeignKeyName(rules descriptor.ForeignKeyNameRules, name string) bool {
	return rules.BareNumbers && isNumber(name)
}

// IsServerForeignKeyName reports whether name is one a server with rules could
// have given a foreign key of table that the statement left unnamed, in either
// scheme the package describes. It answers for a server that names unnamed
// keys by this package's rules; the caller asks the dialect's descriptor
// whether its server does, and passes the descriptor's ForeignKeyNameRules.
func IsServerForeignKeyName(rules descriptor.ForeignKeyNameRules, table, name string) bool {
	return IsForeignKeyName(table, name) || IsNumberedForeignKeyName(rules, name)
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
// not cut it; see [IndexName]. baseBytes are the cuts to accept, each one an
// IndexNaming.BaseBytes, and the whole name is accepted beside them. A caller
// passes every cut its family states, because a database one engine of the
// family built can be read on another. A key with a name builds an index under
// the key's name instead. same decides when two names are one, because the
// server compares index names without case.
func IsUnnamedKeyIndexName(baseBytes []int, column, index string, same func(a, b string) bool) bool {
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
	if same(index[:cut], column) {
		return true
	}
	for _, bytes := range baseBytes {
		if same(index[:cut], indexBase(column, bytes)) {
			return true
		}
	}
	return false
}

// IndexName answers the name a server gives an index it names after base:
// base itself where no index of the table holds it, and otherwise base
// followed by `_2`, `_3` and on, the first that is free. taken reports whether
// the table holds a name, and compares as the server compares index names,
// without case. baseBytes is the dialect's IndexNaming.BaseBytes: the bytes of
// base the server keeps before the number, or zero to keep base whole.
//
// MySQL cuts base to 61 bytes before the number, on a character boundary;
// MariaDB keeps it whole. Measured on MySQL 9.7.2, a 64-character column
// yields a 63-character `_2` and a 64-character `_10`. The server cuts inside a
// character where the byte count falls there, and then refuses the name it
// built; this cut stops before the character, so the name is one the server
// accepts. The SQL reader names every index the server names by this, and the
// comparison names a column's own UNIQUE by it, so both answer the same.
func IndexName(baseBytes int, base string, taken func(string) bool) string {
	candidate := base
	prefix := indexBase(base, baseBytes)
	for suffix := 2; taken(candidate); suffix++ {
		candidate = prefix + "_" + strconv.Itoa(suffix)
	}
	return candidate
}

// indexBase is the part of a column name a server keeps before it appends a
// number: its first bytes, cut on a character boundary, or the whole name
// where bytes is zero or the name fits in that many bytes.
func indexBase(column string, bytes int) string {
	if bytes <= 0 || len(column) <= bytes {
		return column
	}
	cut := bytes
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
