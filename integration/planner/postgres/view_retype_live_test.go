//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/planext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestRetypeUnderAViewChainLive changes the type of a column two views read,
// one through the other, and runs the plan in both statement styles.
//
// PostgreSQL refuses ALTER COLUMN ... TYPE under a view that reads the column,
// so the plan drops both views before the change and creates them after it.
// The exact style drops them without CASCADE, which the server accepts only
// when each view goes after the view that reads it, and it creates them with a
// plain CREATE VIEW, which it accepts only because both are gone. The database
// is read back for the column's new length and for both views.
func TestRetypeUnderAViewChainLive(t *testing.T) {
	adminURL := livePostgresURLForRLSEnable(t)
	tests := []struct {
		name  string
		style planext.StatementStyle
	}{
		{name: "Ptah's own style", style: planext.StatementStyleAnnotated},
		{name: "the exact style", style: planext.StatementStyleExact},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL := createRLSEnableDatabase(c, adminURL)
			executeSQL(c, dbURL, []string{
				`CREATE TABLE items (id integer PRIMARY KEY, sku varchar)`,
				`CREATE VIEW item_skus AS SELECT id, sku FROM items`,
				`CREATE VIEW item_sku_count AS SELECT sku, count(*) AS n FROM item_skus GROUP BY sku`,
			})
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
				Fields: []schemamodel.Field{
					{StructName: "Item", Name: "id", Type: "INTEGER", Primary: true},
					{StructName: "Item", Name: "sku", Type: "VARCHAR(128)", Nullable: true},
				},
				Views: []schemamodel.View{
					{Name: "item_skus", Body: "SELECT id, sku FROM items"},
					{Name: "item_sku_count", Body: "SELECT sku, count(*) AS n FROM item_skus GROUP BY sku"},
				},
			}
			conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
			c.Assert(err, qt.IsNil)
			live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
			dbschema.CloseAndWarn(conn)
			c.Assert(err, qt.IsNil)
			diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, live, "postgres", must.Must(builtin.New())))

			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()), diff, "postgres",
				planext.Options{Statements: test.style},
			)
			c.Assert(err, qt.IsNil)
			c.Logf("plan:\n%s", strings.Join(statements, "\n"))
			executeSQL(c, dbURL, statements)

			c.Assert(queryStrings(c, dbURL, `SELECT character_maximum_length::text FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'items' AND column_name = 'sku'`), qt.DeepEquals, []string{"128"})
			c.Assert(queryStrings(c, dbURL, `SELECT table_name FROM information_schema.views
WHERE table_schema = 'public' ORDER BY table_name`), qt.DeepEquals, []string{"item_sku_count", "item_skus"})
		})
	}
}
