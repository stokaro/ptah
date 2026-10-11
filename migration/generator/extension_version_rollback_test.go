package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestPlanBidirectionalSchemaDiff_ExtensionVersionRollback pins the rollback
// of a raised extension version. PostgreSQL updates an extension in one
// direction only, so the rollback runs no statement for the version and says
// what it leaves; a move of the same extension still goes back. Before, the
// rollback asked for the downgrade the planner refuses, and the refusal
// failed the whole plan.
func TestPlanBidirectionalSchemaDiff_ExtensionVersionRollback(t *testing.T) {
	const limit = `-- Recovery limit: "PostgreSQL updates an extension in one direction only: after the rollback, pg_trgm stays at version 1.6 rather than 1.5."` + "\n"
	const rollback = `-- Rollback of "extension pg_trgm": "keeps the extension at the version the migration installed".` + "\n"
	tests := []struct {
		name     string
		declared schemamodel.Extension
		forward  string
		reverse  string
		moves    int
	}{
		{
			name:     "a raised version",
			declared: schemamodel.Extension{Name: "pg_trgm", Schema: "public", Version: "1.6"},
			forward:  "-- Update extension pg_trgm from 1.5 to 1.6\nALTER EXTENSION \"pg_trgm\" UPDATE TO '1.6';\n",
			reverse:  rollback + limit,
		},
		{
			name:     "a raised version and a move",
			declared: schemamodel.Extension{Name: "pg_trgm", Schema: "ext", Version: "1.6"},
			forward: "CREATE SCHEMA IF NOT EXISTS \"ext\";\n" +
				"-- Update extension pg_trgm from 1.5 to 1.6\nALTER EXTENSION \"pg_trgm\" UPDATE TO '1.6';\n" +
				"-- Move extension pg_trgm from schema public to ext\nALTER EXTENSION \"pg_trgm\" SET SCHEMA \"ext\";\n",
			reverse: rollback + limit + "-- Move extension pg_trgm from schema ext to public\nALTER EXTENSION \"pg_trgm\" SET SCHEMA \"public\";\n",
			moves:   1,
		},
		{
			name:     "control: a move alone",
			declared: schemamodel.Extension{Name: "pg_trgm", Schema: "ext"},
			forward:  "CREATE SCHEMA IF NOT EXISTS \"ext\";\n-- Move extension pg_trgm from schema public to ext\nALTER EXTENSION \"pg_trgm\" SET SCHEMA \"ext\";\n",
			reverse:  "-- Move extension pg_trgm from schema ext to public\nALTER EXTENSION \"pg_trgm\" SET SCHEMA \"public\";\n",
			moves:    1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{Extensions: []schemamodel.Extension{test.declared}}
			current := &catalog.Database{Extensions: []catalog.Extension{{Name: "pg_trgm", Schema: "public", Version: "1.5", Relocatable: true}}}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.Postgres, runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.Postgres})

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, plan.Forward.Nodes...)), qt.Equals, test.forward)
			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)), qt.Equals, test.reverse)
			c.Assert(plan.Reverse.Diff.ExtensionsModified, qt.HasLen, test.moves)
		})
	}
}
