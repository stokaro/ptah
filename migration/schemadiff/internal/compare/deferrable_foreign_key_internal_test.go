package compare

// White-box testing required: foreignKeyConstraintChanged is the one place that
// decides whether a declared foreign key differs from the one in the catalog,
// and it is unexported. Reaching it through the whole comparator would require
// a full schema pair on both sides to observe one predicate.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
)

// deferrableGenerated and deferrableDatabase are the two sides of one foreign
// key, identical but for the deferral each row sets.
func deferrableGenerated(deferrable bool, initially string) goschema.Constraint {
	return goschema.Constraint{
		Name:           "fk_child_parent",
		Type:           "FOREIGN KEY",
		Table:          "child",
		Columns:        []string{"parent_id"},
		ForeignTable:   "parent",
		ForeignColumn:  "id",
		ForeignColumns: []string{"id"},
		Deferrable:     deferrable,
		Initially:      initially,
	}
}

func deferrableDatabase(deferrable bool, initially string) types.DBConstraint {
	foreignTable := "parent"
	foreignColumn := "id"
	return types.DBConstraint{
		Name:           "fk_child_parent",
		Type:           "FOREIGN KEY",
		TableName:      "child",
		ColumnNames:    []string{"parent_id"},
		ForeignTable:   &foreignTable,
		ForeignColumn:  &foreignColumn,
		ForeignColumns: []string{"id"},
		Deferrable:     deferrable,
		Initially:      initially,
	}
}

// TestForeignKeyConstraintChanged_Deferral covers the half of
// stokaro/ptah#1624 without which the feature never arrives: if the comparator
// does not read the deferral, a schema declaring DEFERRABLE against a
// constraint created without it reports no difference, the plan is empty, and
// the property the author asked for silently never lands.
//
// The rows separate every operand:
//
//   - the two "differs" rows move exactly one axis, deferrability and timing;
//   - the identical row is the control that fails if the predicate started
//     reporting a difference for everything;
//   - the last row is the one a naive comparison gets wrong. A schema saying
//     `deferrable = true` with no timing and a catalog reporting condeferred
//     false are the SAME statement -- the engine's default for an unwritten
//     timing is IMMEDIATE -- so comparing the raw strings would plan a
//     drop-and-recreate on every run forever.
func TestForeignKeyConstraintChanged_Deferral(t *testing.T) {
	tests := []struct {
		name        string
		generated   goschema.Constraint
		database    types.DBConstraint
		wantChanged bool
	}{
		{
			name:        "declared deferrable against a catalog that is not",
			generated:   deferrableGenerated(true, "deferred"),
			database:    deferrableDatabase(false, ""),
			wantChanged: true,
		},
		{
			name:        "the same deferrability with a different timing",
			generated:   deferrableGenerated(true, "deferred"),
			database:    deferrableDatabase(true, "immediate"),
			wantChanged: true,
		},
		{
			name:      "identical deferral is no change",
			generated: deferrableGenerated(true, "deferred"),
			database:  deferrableDatabase(true, "deferred"),
		},
		{
			name:      "neither side deferrable is no change",
			generated: deferrableGenerated(false, ""),
			database:  deferrableDatabase(false, ""),
		},
		{
			// The row only the DEFERRABILITY check catches: both sides normalize
			// to an immediate timing, and they are still different constraints,
			// because one can be deferred inside a transaction and the other
			// cannot. Without it the bool comparison is redundant with the
			// timing comparison and a mutant removing it survives.
			name:        "deferrable with an immediate default against a catalog that is not deferrable",
			generated:   deferrableGenerated(true, "immediate"),
			database:    deferrableDatabase(false, ""),
			wantChanged: true,
		},
		{
			name:      "an unwritten timing equals the immediate the engine reports",
			generated: deferrableGenerated(true, ""),
			database:  deferrableDatabase(true, "immediate"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changed := foreignKeyConstraintChanged(test.generated, test.database, "postgres")

			c.Assert(changed, qt.Equals, test.wantChanged)
		})
	}
}
