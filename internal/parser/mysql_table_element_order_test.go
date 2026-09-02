package parser_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

// foreignKeyThenIndex and indexThenForeignKey are the two documents
// stokaro/ptah#2773 was filed about. They differ in nothing but the order of
// the two elements in the table body.
//
// Measured on MySQL 26.7.0 and MariaDB 12.3.3, identically on both: the first
// builds `b(a)` and `b_2(b)`, and the second is
// `ERROR 1061 (42000): Duplicate key name 'b'`. The order is therefore not
// decoration -- it decides which names exist and whether the document can be
// created at all -- and a parse that does not carry it hands the naming pass
// two documents it has no way to tell apart.
const (
	foreignKeyThenIndex = "CREATE TABLE c (a INT, b INT, " +
		"CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id), KEY (b));"
	indexThenForeignKey = "CREATE TABLE c (a INT, b INT, " +
		"KEY (b), CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id));"
)

// tableElementPositions describes a recorded declaration order as data: for
// each element, where the node it points at sits in Constraints and where in
// Indexes, with -1 for the slice the element is not from.
//
// The position pair rather than a kind and a name, because the position is the
// invariant the conversion depends on: an element does not merely say that a
// constraint came first, it points at the very node the constraint slice holds,
// and the pass that reads the order walks both slices by position. A describing
// helper that branched on which field is set would also put a conditional in a
// test helper; slices.Index answers -1 for the nil pointer and needs no branch.
func tableElementPositions(table *ast.CreateTableNode) []string {
	positions := make([]string, 0, len(table.Elements))
	for _, element := range table.Elements {
		positions = append(positions, fmt.Sprintf("constraints[%d] indexes[%d]",
			slices.Index(table.Constraints, element.Constraint),
			slices.Index(table.Indexes, element.Index)))
	}
	return positions
}

// TestParse_ATableBodyRecordsTheOrderItWasDeclaredIn establishes that the parse
// carries the interleaving of a table body's constraints and indexes.
//
// The sequence is asserted rather than its length: a length alone is satisfied
// by a node that recorded both elements in whichever order it happened to
// append them, which is exactly the state the two slices were already in.
//
// The third row is the one that pins the pairing rule the conversion reads the
// order with -- the k-th element of a kind is the k-th member of that kind's
// slice. A document whose two kinds alternate fails it the moment an element is
// recorded out of step with its own slice, while the two rows above it would
// still pass.
func TestParse_ATableBodyRecordsTheOrderItWasDeclaredIn(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "a foreign key declared before an index",
			sql:  foreignKeyThenIndex,
			want: []string{"constraints[0] indexes[-1]", "constraints[-1] indexes[0]"},
		},
		{
			name: "the same two elements the other way round",
			sql:  indexThenForeignKey,
			want: []string{"constraints[-1] indexes[0]", "constraints[0] indexes[-1]"},
		},
		{
			name: "two of each kind, alternating",
			sql: "CREATE TABLE c (a INT, b INT, KEY (a), CONSTRAINT u1 UNIQUE (b), " +
				"KEY (b), CONSTRAINT u2 UNIQUE (a));",
			want: []string{
				"constraints[-1] indexes[0]",
				"constraints[0] indexes[-1]",
				"constraints[-1] indexes[1]",
				"constraints[1] indexes[-1]",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, test.sql)

			c.Assert(tableElementPositions(table), qt.DeepEquals, test.want)
		})
	}
}

// TestParse_ReorderingATableBodyChangesNothingButTheRecordedOrder is the
// measurement that says why the recorded order had to be added at all.
//
// Both documents parse to the same constraint and the same index, carrying the
// same names and the same columns, so everything downstream of the parse that
// reads only those two slices sees one document twice. Only Elements
// distinguishes them, which is what the two engines do when they refuse one and
// accept the other.
func TestParse_ReorderingATableBodyChangesNothingButTheRecordedOrder(t *testing.T) {
	c := qt.New(t)

	first := parsedTable(c, foreignKeyThenIndex)
	second := parsedTable(c, indexThenForeignKey)

	// The index is unnamed in both, which is what leaves the server free to
	// name it and makes the order load-bearing.
	c.Assert(indexNames(first), qt.DeepEquals, []string{""})
	c.Assert(indexNames(second), qt.DeepEquals, []string{""})
	c.Assert(first.Indexes[0].Columns, qt.DeepEquals, []string{"b"})
	c.Assert(second.Indexes[0].Columns, qt.DeepEquals, []string{"b"})

	c.Assert(constraintTypes(first), qt.DeepEquals,
		[]ast.ConstraintType{ast.ForeignKeyConstraint})
	c.Assert(constraintTypes(second), qt.DeepEquals,
		[]ast.ConstraintType{ast.ForeignKeyConstraint})
	c.Assert(first.Constraints[0].Name, qt.Equals, "b")
	c.Assert(second.Constraints[0].Name, qt.Equals, "b")
	c.Assert(first.Constraints[0].Columns, qt.DeepEquals, []string{"a"})
	c.Assert(second.Constraints[0].Columns, qt.DeepEquals, []string{"a"})

	c.Assert(tableElementPositions(first), qt.Not(qt.DeepEquals),
		tableElementPositions(second))
}
