package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestPlanBidirectionalSchemaDiff_DeclaredSchemaRollsBack pins both directions of
// a schema the comparison adds. The rollback gives back the database that had no
// such schema, after it drops what the forward plan put there. A table in a schema
// no declaration names keeps its old answer: the table goes and the schema stays,
// because nothing established that the schema was the plan's to remove.
func TestPlanBidirectionalSchemaDiff_DeclaredSchemaRollsBack(t *testing.T) {
	table := []schemamodel.Table{{StructName: "T", Name: "t", Schema: "app"}}
	fields := []schemamodel.Field{{StructName: "T", Name: "id", Type: "integer", Primary: true}}
	tests := []struct {
		name             string
		desired          *schemamodel.Database
		wantUp, wantDown []string
	}{
		{
			name:     "empty",
			desired:  &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}},
			wantUp:   []string{`CREATE SCHEMA IF NOT EXISTS "app"`},
			wantDown: []string{`DROP SCHEMA IF EXISTS "app"`},
		},
		{
			name:     "with a comment",
			desired:  &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app", Comment: "application"}}},
			wantUp:   []string{`CREATE SCHEMA IF NOT EXISTS "app"`, `COMMENT ON SCHEMA "app" IS 'application'`},
			wantDown: []string{`DROP SCHEMA IF EXISTS "app"`},
		},
		{
			name:    "with a table",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}, Tables: table, Fields: fields},
			wantUp: []string{
				`CREATE SCHEMA IF NOT EXISTS "app"`,
				"CREATE TABLE \"app\".\"t\" (\n  \"id\" integer PRIMARY KEY NOT NULL\n)",
			},
			wantDown: []string{"-- WARNING: This will delete all data!\n" + `DROP TABLE IF EXISTS "app"."t" CASCADE`, `DROP SCHEMA IF EXISTS "app"`},
		},
		{
			name:    "named by a table and not declared",
			desired: &schemamodel.Database{Tables: table, Fields: fields},
			wantUp: []string{
				`CREATE SCHEMA IF NOT EXISTS "app"`,
				"CREATE TABLE \"app\".\"t\" (\n  \"id\" integer PRIMARY KEY NOT NULL\n)",
			},
			wantDown: []string{"-- WARNING: This will delete all data!\n" + `DROP TABLE IF EXISTS "app"."t" CASCADE`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{Schemas: []catalog.Schema{{Name: "public"}}}
			runtime := must.Must(builtin.New())
			diff := must.Must(schemadiff.CompareWithDialect(c.Context(), test.desired, current, platform.Postgres, runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: test.desired, CurrentSchema: current, Dialect: platform.Postgres,
			})

			c.Assert(err, qt.IsNil)
			caps := capability.ForDialect(platform.Postgres)
			up := must.Must(builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Forward.Nodes...))
			down := must.Must(builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...))
			c.Assert(sqlutil.SplitSQLStatements(up), qt.DeepEquals, test.wantUp, qt.Commentf("%s", up))
			c.Assert(sqlutil.SplitSQLStatements(down), qt.DeepEquals, test.wantDown, qt.Commentf("%s", down))
		})
	}
}
