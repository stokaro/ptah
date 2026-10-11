package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
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
	const keep = `-- Rollback of "PostgreSQL extension pg_trgm": "keep the version the migration installed".` + "\n"
	const moveAndKeep = `-- Rollback of "PostgreSQL extension pg_trgm": "move the extension back, keep the version the migration installed".` + "\n"
	const move = `-- Rollback of "PostgreSQL extension pg_trgm": "move the extension back".` + "\n"
	tests := []struct {
		name     string
		declared pgextension.DesiredExtension
		forward  string
		reverse  string
	}{
		{
			name:     "a raised version",
			declared: pgextension.DesiredExtension{Schema: "public", Version: "1.6"},
			forward:  "-- Update extension pg_trgm from 1.5 to 1.6\nALTER EXTENSION \"pg_trgm\" UPDATE TO '1.6';\n",
			reverse:  keep + limit,
		},
		{
			name:     "a raised version and a move",
			declared: pgextension.DesiredExtension{Schema: "ext", Version: "1.6"},
			forward: "CREATE SCHEMA IF NOT EXISTS \"ext\";\n" +
				"-- Update extension pg_trgm from 1.5 to 1.6\nALTER EXTENSION \"pg_trgm\" UPDATE TO '1.6';\n" +
				"-- Move extension pg_trgm from schema public to ext\nALTER EXTENSION \"pg_trgm\" SET SCHEMA \"ext\";\n",
			reverse: moveAndKeep + limit + "-- Move extension pg_trgm from schema ext to public\nALTER EXTENSION \"pg_trgm\" SET SCHEMA \"public\";\n",
		},
		{
			name:     "control: a move alone",
			declared: pgextension.DesiredExtension{Schema: "ext"},
			forward:  "CREATE SCHEMA IF NOT EXISTS \"ext\";\n-- Move extension pg_trgm from schema public to ext\nALTER EXTENSION \"pg_trgm\" SET SCHEMA \"ext\";\n",
			reverse:  move + "-- Move extension pg_trgm from schema ext to public\nALTER EXTENSION \"pg_trgm\" SET SCHEMA \"public\";\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := declaringExtension("pg_trgm", test.declared)
			current := &catalog.Database{
				FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.ObservedObject(pgextension.Ref("pg_trgm"),
					pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true})))),
				FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
			}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.Postgres, runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.Postgres})

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, plan.Forward.Nodes...)), qt.Equals, test.forward)
			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)), qt.Equals, test.reverse)
		})
	}
}
