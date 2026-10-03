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
	"slices"
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

// Unique answers the name PostgreSQL gives an unnamed UNIQUE constraint on
// table: `<table>_<column names>_key`, fitted and numbered as [Constraint]
// does.
//
// The names are those of every column of the index the constraint builds, the
// key columns and then the INCLUDE columns, as ChooseIndexColumnNames gives
// them: a name an earlier column of the list already took is numbered.
// Measured on PostgreSQL 18.6:
//
//	UNIQUE (x) INCLUDE (y) on i1            i1_x_y_key
//	UNIQUE (x, y) INCLUDE (z, x) on i3      i3_x_y_z_x1_key
//	UNIQUE (x) INCLUDE (x) on i4            i4_x_x1_key
//	UNIQUE (x) INCLUDE ("Y") on i10         i10_x_Y_key
func Unique(table string, columns, include []string, taken func(string) bool) string {
	return Constraint(table, indexColumnNames(slices.Concat(columns, include)), "key", taken)
}

// PrimaryKey answers the name PostgreSQL gives an unnamed primary key on
// table: `<table>_pkey`, fitted and numbered as [Constraint] does. Measured on
// PostgreSQL 18.6, `id int PRIMARY KEY, x int, CONSTRAINT k6_pkey CHECK (x >
// 0)` on k6 names the key k6_pkey1, because the server adds a table's CHECK
// constraints before it builds the key's index.
func PrimaryKey(table string, taken func(string) bool) string {
	return Constraint(table, nil, "pkey", taken)
}

// ColumnKey answers the name PostgreSQL gives a column-level UNIQUE on table
// when the name is free: `<table>_<column>_key`, as [Constraint] derives it.
// The table is the bare relation name, without its schema.
func ColumnKey(table, column string) string {
	return Constraint(table, []string{column}, "key", nil)
}

// Sequence answers the name PostgreSQL gives the sequence it creates for a
// serial or an identity column of table: `<table>_<column>_seq`, fitted into 63
// bytes as [Object] fits it. The table is the bare relation name, without its
// schema.
//
// The server numbers the name, `<table>_<column>_seq1` and on, when a relation
// of the schema already holds it. That is not repeated here, for the reason
// [Object] gives. Measured on PostgreSQL 18.6:
//
//	"MixedCase" ("Id" serial)                  MixedCase_Id_seq
//	61-byte table, 53-byte bigserial column    a_table_name_that_is_quite_lo_a_column_name_that_is_also_ra_seq
//	the same table, identity column b          a_table_name_that_is_quite_long_for_testing_purposes_only_b_seq
//	"ünï", a 51-byte column                    ünï_ñameß×23_seq, 61 bytes, uncut
//	"tä", a 59-byte column of ß×29 then a      tä_ß×27_seq, 62 bytes: the cut falls inside a ß
func Sequence(table, column string) string {
	return Object(table, column, "seq")
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
