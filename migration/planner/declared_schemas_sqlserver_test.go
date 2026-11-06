package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// planSQLServerSchemas plans diff on SQL Server in style and returns the statements
// joined, so a row can state their order.
func planSQLServerSchemas(c *qt.C, style planext.StatementStyle, diff *difftypes.SchemaDiff) (string, error) {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, platform.SQLServer,
		planext.Options{Capabilities: capability.ForDialect(platform.SQLServer), Statements: style})
	return strings.Join(statements, "; "), err
}

// TestSQLServerPlansADeclaredSchema pins the statements for a schema the comparison
// adds and for one a rollback removes. The creation is guarded in both styles,
// because the server has no CREATE SCHEMA IF NOT EXISTS. The drop comes after
// everything else and has no CASCADE, which the server does not offer, so the
// server refuses a schema that still holds an object.
func TestSQLServerPlansADeclaredSchema(t *testing.T) {
	tests := []struct {
		name  string
		style planext.StatementStyle
		diff  difftypes.SchemaDiff
		want  string
	}{
		{
			name:  "a schema alone, created",
			style: planext.StatementStyleAnnotated,
			diff:  difftypes.SchemaDiff{SchemasAdded: []schemamodel.Schema{{Name: "app"}}},
			want:  "IF SCHEMA_ID('app') IS NULL\n    EXEC('CREATE SCHEMA [app]')",
		},
		{
			name:  "a schema the read found, in the exact style",
			style: planext.StatementStyleExact,
			diff:  difftypes.SchemaDiff{SchemasAdded: []schemamodel.Schema{{Name: "app"}}, HeldSchemas: []string{"app"}},
			want:  "",
		},
		{
			name:  "a comment with a creation, which holds if the schema is found",
			style: planext.StatementStyleExact,
			diff: difftypes.SchemaDiff{
				SchemasAdded:    []schemamodel.Schema{{Name: "app", Comment: "it's"}},
				DeclaredSchemas: []schemamodel.Schema{{Name: "app", Comment: "it's"}},
			},
			want: "IF SCHEMA_ID('app') IS NULL\n    EXEC('CREATE SCHEMA [app]'); " +
				"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 3 AND major_id = SCHEMA_ID('app') " +
				"AND minor_id = 0 AND name = N'MS_Description')\n" +
				"    EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'it''s', @level0type = N'SCHEMA', @level0name = N'app'",
		},
		{
			name:  "a comment on a schema that holds none",
			style: planext.StatementStyleExact,
			diff: difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedSchema, Name: "app", Desired: "new"},
			}},
			want: "EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'new', @level0type = N'SCHEMA', @level0name = N'app'",
		},
		{
			name:  "a comment on a schema that holds another",
			style: planext.StatementStyleExact,
			diff: difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedSchema, Name: "app", Current: "old", Desired: "new"},
			}},
			want: "EXEC sp_updateextendedproperty @name = N'MS_Description', @value = N'new', @level0type = N'SCHEMA', @level0name = N'app'",
		},
		{
			name:  "a comment removed, which a rollback plans",
			style: planext.StatementStyleExact,
			diff: difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedSchema, Name: "app", Current: "old"},
			}},
			want: "EXEC sp_dropextendedproperty @name = N'MS_Description', @level0type = N'SCHEMA', @level0name = N'app'",
		},
		{
			name:  "a schema alone, dropped, with a guard in the annotated style",
			style: planext.StatementStyleAnnotated,
			diff:  difftypes.SchemaDiff{SchemasRemoved: []string{"app"}},
			want:  "DROP SCHEMA IF EXISTS [app]",
		},
		{
			name:  "a schema alone, dropped, with no guard in the exact style",
			style: planext.StatementStyleExact,
			diff:  difftypes.SchemaDiff{SchemasRemoved: []string{"app"}},
			want:  "DROP SCHEMA [app]",
		},
		{
			name:  "dropped after the table in it",
			style: planext.StatementStyleExact,
			diff:  difftypes.SchemaDiff{SchemasRemoved: []string{"app"}, TablesRemoved: difftypes.TableRemovals{{Name: "app.t"}}},
			want:  "DROP TABLE [app].[t]; DROP SCHEMA [app]",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := test.diff

			got, err := planSQLServerSchemas(c, test.style, &diff)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestSQLServerRefusesADatabaseChange refuses what only a plan of a whole MySQL or
// MariaDB server states. Planning nothing would report the two sides equal.
func TestSQLServerRefusesADatabaseChange(t *testing.T) {
	c := qt.New(t)

	got, err := planSQLServerSchemas(c, planext.StatementStyleExact, &difftypes.SchemaDiff{
		SchemasModified: []difftypes.SchemaChange{{Name: "app", Collate: "Latin1_General_BIN2"}},
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(got, qt.Equals, "")
}
