package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// planDeclaredSchemas plans diff on dialect in style and returns the statements
// joined, so a row can state their order.
func planDeclaredSchemas(c *qt.C, dialect string, style planext.StatementStyle, diff *difftypes.SchemaDiff) string {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: style})
	c.Assert(err, qt.IsNil)
	return strings.Join(statements, "; ")
}

// TestPlanner_CreatesADeclaredSchema pins the plan for a schema the comparison
// adds. It is created with what its declaration says, once however many objects
// name it, and not at all where the connection already resolved to it or where the
// target owns it.
func TestPlanner_CreatesADeclaredSchema(t *testing.T) {
	app := schemamodel.Schema{Name: "app"}
	tests := []struct {
		name    string
		dialect string
		diff    difftypes.SchemaDiff
		want    string
	}{
		{
			name: "alone", dialect: "postgres",
			diff: difftypes.SchemaDiff{SchemasAdded: []schemamodel.Schema{app}},
			want: `CREATE SCHEMA IF NOT EXISTS "app"`,
		},
		{
			name: "with its comment", dialect: "postgres",
			diff: difftypes.SchemaDiff{
				SchemasAdded:    []schemamodel.Schema{{Name: "app", Comment: "application"}},
				DeclaredSchemas: []schemamodel.Schema{{Name: "app", Comment: "application"}},
			},
			want: `CREATE SCHEMA IF NOT EXISTS "app"; COMMENT ON SCHEMA "app" IS 'application'`,
		},
		{
			name: "named by an added table as well", dialect: "postgres",
			diff: difftypes.SchemaDiff{
				SchemasAdded: []schemamodel.Schema{app},
				TablesAdded: difftypes.TableCreationsFor(&schemamodel.Database{
					Tables: []schemamodel.Table{{Name: "t", Schema: "app", StructName: "T"}},
					Fields: []schemamodel.Field{{Name: "c", StructName: "T", Type: "TEXT"}},
				}, identifier.ForDialect("postgres"), "app.t"),
			},
			want: `CREATE SCHEMA IF NOT EXISTS "app"; CREATE TABLE "app"."t"`,
		},
		{
			name: "the schema the connection resolved", dialect: "postgres",
			diff: difftypes.SchemaDiff{SchemasAdded: []schemamodel.Schema{app}, IdentifierSemantics: connectedTo("postgres", "app")},
			want: "",
		},
		{
			name: "a schema the target owns", dialect: "spanner",
			diff: difftypes.SchemaDiff{SchemasAdded: []schemamodel.Schema{{Name: "public"}}},
			want: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := planDeclaredSchemas(c, test.dialect, planext.StatementStyleExact, &test.diff)
			c.Assert(strings.HasPrefix(got, test.want), qt.IsTrue, qt.Commentf("%s", got))
			c.Assert(strings.Count(got, "CREATE SCHEMA"), qt.Equals, strings.Count(test.want, "CREATE SCHEMA"), qt.Commentf("%s", got))
		})
	}
}

// TestPlanner_DropsARemovedSchemaLast pins a rollback's drop. It comes after every
// other statement, because what the plan drops first may be in the schema, and it
// has no CASCADE in either style, because whatever else the schema holds by then
// the plan did not put there.
func TestPlanner_DropsARemovedSchemaLast(t *testing.T) {
	diff := func() *difftypes.SchemaDiff {
		return &difftypes.SchemaDiff{SchemasRemoved: []string{"app"}, TablesRemoved: difftypes.TableRemovals{{Name: "app.t"}}}
	}
	tests := []struct {
		name  string
		style planext.StatementStyle
		want  string
	}{
		{name: "the exact style", style: planext.StatementStyleExact, want: `DROP TABLE "app"."t"; DROP SCHEMA "app"`},
		{name: "the annotated style", style: planext.StatementStyleAnnotated, want: "-- WARNING: This will delete all data!\n" + `DROP TABLE IF EXISTS "app"."t" CASCADE; DROP SCHEMA IF EXISTS "app"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planDeclaredSchemas(c, "postgres", test.style, diff()), qt.Equals, test.want)
		})
	}
}

// TestPlanner_RefusesADatabaseChange pins what this family still has no statement
// for: a character set or a collation, which only a whole MySQL-family server
// reports.
func TestPlanner_RefusesADatabaseChange(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
		&difftypes.SchemaDiff{SchemasModified: []difftypes.SchemaChange{{Name: "app"}}}, "postgres",
		planext.Options{Capabilities: capability.ForDialect("postgres")})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*the diff changes a database, which only a MySQL or MariaDB plan of a whole server does; the postgres planner plans none`)
	c.Assert(statements, qt.IsNil)
}
