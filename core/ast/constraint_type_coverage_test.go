package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

// TestConstraintType_EveryKindHasItsKeyword pins the enumeration against the
// method that is supposed to spell it.
//
// String is documented as "the SQL representation of the constraint type", and
// EXCLUDE answered "UNKNOWN" -- the kind was added to the enumeration and not to
// the switch. That is the same omission, in a second place, as the one that let
// a table-level CHECK be dropped by a `default` branch (stokaro/ptah#1215).
func TestConstraintType_EveryKindHasItsKeyword(t *testing.T) {
	tests := []struct {
		name string
		kind ast.ConstraintType
		want string
	}{
		{name: "primary key", kind: ast.PrimaryKeyConstraint, want: "PRIMARY KEY"},
		{name: "unique", kind: ast.UniqueConstraint, want: "UNIQUE"},
		{name: "foreign key", kind: ast.ForeignKeyConstraint, want: "FOREIGN KEY"},
		{name: "check", kind: ast.CheckConstraint, want: "CHECK"},
		{name: "exclude", kind: ast.ExcludeConstraint, want: "EXCLUDE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.kind.String(), qt.Equals, test.want)
		})
	}
}

// TestConstraintType_ASixthKindWouldBeUnspelled is what makes the table above a
// ratchet rather than a snapshot.
//
// The rows are hand-written, so a kind added to the enumeration and to String
// without a row here would go unmeasured. This asserts that the value one past
// the last kind is still unspelled: adding a sixth makes it spelled, and this
// goes red until the table and every switch over the enumeration are revisited.
func TestConstraintType_ASixthKindWouldBeUnspelled(t *testing.T) {
	c := qt.New(t)

	beyond := ast.ConstraintType(int(ast.ExcludeConstraint) + 1)

	c.Assert(beyond.String(), qt.Equals, "UNKNOWN",
		qt.Commentf("a constraint kind was added; give it a row above and check every switch "+
			"over ConstraintType, including toschema.ToConstraint"))
}
