package generator

// White-box testing required: what this pins is which materialized view the
// reversal resolves, and the reversal is unexported. Through the public API a
// rollback that recreates the wrong body and one that recreates the right one
// are both just SQL that applies.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestReverseSchemaDiff_AMaterializedViewOperandComesFromThePriorSchema pins
// the rollback of a modified materialized view to the body the database held.
//
// No engine has an in-place replacement that keeps a materialized view's rows,
// so a body change is a drop and a create. The create renders from the operand:
// carrying the declaration through would have the rollback rebuild the very
// definition it is undoing, and the plan would look exactly the same either way
// -- a DROP and a CREATE, in that order, naming the right view.
func TestReverseSchemaDiff_AMaterializedViewOperandComesFromThePriorSchema(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect(platform.Postgres)

	prior := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
		StructName: "Stats", Name: "user_stats",
		Body: "SELECT id, count(*) FROM users GROUP BY id",
	}}}

	reversed := reverseMaterializedViewDiffs([]difftypes.MaterializedViewDiff{{
		ViewName: "user_stats",
		Changes:  map[string]string{"body": "old -> new"},
		Desired: schemamodel.MaterializedView{
			StructName: "Stats", Name: "user_stats",
			Body: "SELECT id, count(*), max(created_at) FROM users GROUP BY id",
		},
	}}, prior, semantics)

	c.Assert(reversed, qt.HasLen, 1)
	c.Assert(reversed[0].Desired.Body, qt.Equals, "SELECT id, count(*) FROM users GROUP BY id",
		qt.Commentf("the rollback rebuilds the view the database held"))
}

// TestReverseSchemaDiff_AMaterializedViewOperandResolvesAcrossSchemaSpellings
// is the lookup half.
//
// The change spells a name the declaration produced and the schema it resolves
// against comes from a database read, so the two need not agree on whether the
// schema is written down. Resolving by string equality finds nothing, and a
// rollback with no operand drops the view and recreates nothing -- which reads,
// in the plan, as a view that needed only dropping.
func TestReverseSchemaDiff_AMaterializedViewOperandResolvesAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name      string
		diffName  string
		priorName string
	}{
		{
			name:      "the change qualifies public and the read does not",
			diffName:  "public.user_stats",
			priorName: "user_stats",
		},
		{
			name:      "the read qualifies public and the change does not",
			diffName:  "user_stats",
			priorName: "public.user_stats",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect(platform.Postgres)

			prior := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
				Name: test.priorName, Body: "SELECT 1",
			}}}

			reversed := reverseMaterializedViewDiffs([]difftypes.MaterializedViewDiff{{
				ViewName: test.diffName,
				Changes:  map[string]string{"body": "old -> new"},
			}}, prior, semantics)

			c.Assert(reversed, qt.HasLen, 1)
			c.Assert(reversed[0].Desired.Body, qt.Equals, "SELECT 1",
				qt.Commentf("the rollback found the view the database reported"))
		})
	}
}
