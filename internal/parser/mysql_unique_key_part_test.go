package parser_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

// uniqueKeyTable wraps one table-body declaration in a table with two string
// columns for it to index.
//
// Both columns are VARCHAR because a prefix length is legal on a string column
// alone. Measured on MySQL 26.7 and MariaDB 12.3, `UNIQUE KEY uq (a(3))` over
// an INT is answered by both with `ERROR 1089 Incorrect prefix key`, so an
// integer fixture would be asserting how Ptah reads DDL neither server takes.
func uniqueKeyTable(declaration string) string {
	return fmt.Sprintf(
		"CREATE TABLE t (a VARCHAR(50) NOT NULL, b VARCHAR(50) NOT NULL, %s);",
		declaration,
	)
}

// TestParse_MySQLUniqueKeyPartAttributesBecomeAUniqueIndex covers
// stokaro/ptah#2770.
//
// A UNIQUE key was read as a schemamodel constraint, which carries column
// NAMES and nothing else. The prefix length and the DESC direction were parsed
// into ColumnParts and then dropped by that conversion, so `UNIQUE KEY uq
// (a(7))` reached the renderer as `CONSTRAINT uq UNIQUE (a)` -- a weaker
// guarantee than the author declared, with nothing reported.
//
// The repair is the promotion stokaro/ptah#2793 established: a UNIQUE key the
// constraint model cannot hold is read as the unique INDEX both servers
// actually build, which ast.IndexNode already expresses -- Prefix and Desc have
// travelled on ast.IndexPart since stokaro/ptah#2713. So the predicate widens
// from "carries an expression" to "carries an expression, a prefix, or a
// direction", and no field is added to the model.
//
// Every declaration here was measured on MySQL 26.7 and MariaDB 12.3, and the
// two servers agree throughout. Each creates one index named `uq` with
// NON_UNIQUE 0, and information_schema.STATISTICS reports the prefix in
// SUB_PART and the direction in COLLATION:
//
//	UNIQUE KEY uq (a(7))       SUB_PART 7,    COLLATION A
//	UNIQUE KEY uq (a DESC)     SUB_PART NULL, COLLATION D
//	UNIQUE KEY uq (a(7) DESC)  SUB_PART 7,    COLLATION D
//	UNIQUE KEY uq (a, b(3))    two parts: SUB_PART NULL then 3
//
// The whole parts slice is compared rather than the attribute under test,
// because the defect is a value being dropped and an assertion that read only
// the field it expects would not notice a part gaining one it should not have.
// The last row carries a plain part beside a prefixed one for the same reason
// in the other direction: it separates a reader carrying the whole key from one
// that kept a single part, and a promotion that marked every part prefixed
// fails it.
//
// What reaches a live server is asserted in
// integration/mysql_unique_key_prefix_e2e_test.go, which is where the harm the
// issue names -- a duplicate row the authored schema forbids -- is measured
// rather than argued.
func TestParse_MySQLUniqueKeyPartAttributesBecomeAUniqueIndex(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		wantParts   []ast.IndexPart
	}{
		{
			name:        "a prefix length",
			declaration: "UNIQUE KEY uq (a(7))",
			wantParts:   []ast.IndexPart{{Name: "a", Prefix: "7"}},
		},
		{
			name:        "a descending part",
			declaration: "UNIQUE KEY uq (a DESC)",
			wantParts:   []ast.IndexPart{{Name: "a", Desc: true}},
		},
		{
			name:        "both on one part",
			declaration: "UNIQUE KEY uq (a(7) DESC)",
			wantParts:   []ast.IndexPart{{Name: "a", Prefix: "7", Desc: true}},
		},
		{
			name:        "a plain part beside a prefixed one",
			declaration: "UNIQUE KEY uq (a, b(3))",
			wantParts:   []ast.IndexPart{{Name: "a"}, {Name: "b", Prefix: "3"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, uniqueKeyTable(test.declaration))

			c.Assert(table.Constraints, qt.HasLen, 0)
			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Name, qt.Equals, "uq")
			c.Assert(table.Indexes[0].Unique, qt.IsTrue)
			c.Assert(table.Indexes[0].Parts, qt.DeepEquals, test.wantParts)
		})
	}
}

// TestParse_MySQLUniqueKeyWithoutPartAttributesStaysAConstraint is the control
// the promotion above cannot be read without.
//
// The predicate widened, and a widening has a far side: promoting every UNIQUE
// key would satisfy every assertion above while moving the ordinary
// declaration -- the one almost every table writes -- off the constraint path
// for no gain, and a UNIQUE constraint is what a foreign key, a comparison and
// a renderer each expect to find there.
//
// These are the same declarations with the prefix and the direction taken off,
// which is what makes them the control rather than a separate subject. Measured
// on MySQL 26.7 and MariaDB 12.3, both create the index `uq` with SUB_PART NULL
// and COLLATION A throughout -- exactly the information a column-name list
// carries, and therefore nothing a constraint loses.
func TestParse_MySQLUniqueKeyWithoutPartAttributesStaysAConstraint(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		wantColumns []string
	}{
		{
			name:        "one plain column",
			declaration: "UNIQUE KEY uq (a)",
			wantColumns: []string{"a"},
		},
		{
			name:        "two plain columns",
			declaration: "UNIQUE KEY uq (a, b)",
			wantColumns: []string{"a", "b"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, uniqueKeyTable(test.declaration))

			c.Assert(table.Indexes, qt.HasLen, 0)
			c.Assert(constraintTypes(table), qt.DeepEquals, []ast.ConstraintType{ast.UniqueConstraint})
			c.Assert(table.Constraints[0].Name, qt.Equals, "uq")
			c.Assert(table.Constraints[0].Columns, qt.DeepEquals, test.wantColumns)
		})
	}
}
