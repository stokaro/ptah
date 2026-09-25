// Package pgname derives the names PostgreSQL gives the objects a statement
// leaves unnamed, such as the `<table>_<column>_key` of a column-level UNIQUE.
//
// Two parts of Ptah need the same answer. The SQL schema reader names
// an unnamed constraint the way the server did, so a file compares equal to
// the database its own SQL built. The planner writes the name when it adds
// such a constraint, so the object it creates is the one the same declaration
// creates inside CREATE TABLE. One implementation serves both: two copies of
// the rule agree when the second is written and drift when the first changes.
package pgname

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// maxIdentifierBytes is NAMEDATALEN - 1, the longest identifier PostgreSQL
// keeps, counted in bytes.
const maxIdentifierBytes = 63

// Object answers the name PostgreSQL's makeObjectName builds from two names
// and a label: `name1_name2_label`, fitted into 63 bytes.
//
// When the whole name does not fit, the longer of the two names loses one byte
// at a time, the second name on a tie, until both fit. Each is then cut back
// to a character boundary, so a multibyte name can come out shorter than 63
// bytes. The label is never shortened. Measured on PostgreSQL 18.6 against the
// names the server gives inline UNIQUE constraints.
//
// An empty name2 means none: the result is `name1_label`. An empty label means
// none as well.
//
// Object does not add the digit PostgreSQL appends when the name is already
// taken, because only the server knows what exists; a plan that writes the
// name explicitly fails on a taken name instead.
func Object(name1, name2, label string) string {
	overhead := 0
	name1Bytes := len(name1)
	name2Bytes := 0
	if name2 != "" {
		name2Bytes = len(name2)
		overhead++
	}
	if label != "" {
		overhead += len(label) + 1
	}
	available := maxIdentifierBytes - overhead
	for name1Bytes+name2Bytes > available {
		if name1Bytes > name2Bytes {
			name1Bytes--
		} else {
			name2Bytes--
		}
	}
	result := clip(name1, name1Bytes)
	if name2 != "" {
		result += "_" + clip(name2, name2Bytes)
	}
	if label != "" {
		result += "_" + label
	}
	return result
}

// Constraint answers the name PostgreSQL gives an unnamed constraint of one
// kind over columns of table: `<table>_<columns joined by _>_<label>`, fitted
// into 63 bytes as [Object] fits it, and numbered `<label>1`, `<label>2` and on
// while taken answers true. A nil taken treats every name as free. The table
// is the bare relation name, without its schema.
//
// The server cuts every identifier longer than 63 bytes before it derives a
// name, and ChooseForeignKeyConstraintNameAddition stops joining columns past
// 64 bytes. Neither is repeated here: the derived name keeps a prefix of at
// most 57 bytes of each part, and both cuts leave that prefix unchanged.
//
// Measured on PostgreSQL 18.6:
//
//	child (parent_id), FOREIGN KEY       child_parent_id_fkey
//	child (a, b), FOREIGN KEY            child_a_b_fkey
//	twice, two keys over (p)             twice_p_fkey, twice_p_fkey1
//	"MixedCase" ("ParentId")             MixedCase_ParentId_fkey
//	p (a, b), UNIQUE                     p_a_b_key
//	61-byte table, 53-byte column        a_table_name_that_is_quite_lo_a_column_name_that_is_also_r_fkey
//	                                     a_table_name_that_is_quite_lo_a_column_name_that_is_also_ra_key
//	ünï, a 62-byte column of ß           ünï_ñame_ß×23_fkey, 63 bytes and 37 characters
//	ünï, the same column after `a`       ünï_añame_ß×22_fkey, 62 bytes: the cut falls inside a ß
func Constraint(table string, columns []string, label string, taken func(string) bool) string {
	addition := strings.Join(columns, "_")
	numbered := label
	for pass := 1; ; pass++ {
		name := Object(table, addition, numbered)
		if taken == nil || !taken(name) {
			return name
		}
		numbered = label + strconv.Itoa(pass)
	}
}

// ColumnKey answers the name PostgreSQL gives a column-level UNIQUE on table
// when the name is free: `<table>_<column>_key`, as [Constraint] derives it.
// The table is the bare relation name, without its schema.
func ColumnKey(table, column string) string {
	return Constraint(table, []string{column}, "key", nil)
}

// clip returns the longest prefix of value that is at most maxBytes long and
// ends on a character boundary, as pg_mbcliplen does for UTF-8.
func clip(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}
