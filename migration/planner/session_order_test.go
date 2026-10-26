package planner_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// planInApp plans the change from current to desired in the exact style,
// under a connection whose search path is app alone. A read of the session's
// own schema reports its objects with no schema, so current names them bare.
func planInApp(c *qt.C, desired *schemamodel.Database, current *catalog.Database) []string {
	c.Helper()
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, must.Must(builtin.New())))
	diff.IdentifierSemantics = sessionSemantics("app")
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(context.Background(), must.Must(builtin.New()), diff,
		platform.Postgres, planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return statements
}

// statementOrder is the position of the first statement that begins with
// each prefix, -1 for one the plan does not hold.
func statementOrder(statements []string, prefixes ...string) []int {
	positions := make([]int, 0, len(prefixes))
	for _, prefix := range prefixes {
		positions = append(positions, slices.IndexFunc(statements, func(statement string) bool {
			return strings.HasPrefix(statement, prefix)
		}))
	}
	return positions
}

// ascending reports whether positions are all found and increase.
func ascending(positions []int) bool {
	return !slices.Contains(positions, -1) && slices.IsSorted(positions)
}

// TestGenerateSchemaDiffSQL_OrdersInTheSessionsSchema_HappyPath pins the
// order every step writes under `search_path=app`, where an object declared
// without a schema is in app and a body names it app.<name>. In each row an
// object reads one that comes later in the order it would otherwise take, so
// only an edge drawn in app puts the reader after it; ordered by the dialect's
// default schema, public, `app.<name>` reads nothing declared, and the server
// refuses the statement that comes too early.
func TestGenerateSchemaDiffSQL_OrdersInTheSessionsSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		current  *catalog.Database
		prefixes []string
	}{
		{
			name: "views the plan adds",
			desired: &schemamodel.Database{Views: []schemamodel.View{
				{Name: "asummary", Body: "SELECT id FROM app.zbase"},
				{Name: "zbase", Body: "SELECT 1 AS id"},
			}},
			current:  &catalog.Database{},
			prefixes: []string{`CREATE VIEW "zbase"`, `CREATE VIEW "asummary"`},
		},
		{
			// top reads both, so the rebuild finds it before mid.
			name: "readers a materialized view change rebuilds",
			desired: &schemamodel.Database{
				Views: []schemamodel.View{
					{Name: "top", Body: "SELECT m.total FROM app.zbase z, app.mid m"},
					{Name: "mid", Body: "SELECT total FROM app.zbase"},
				},
				MaterializedViews: []schemamodel.MaterializedView{{Name: "zbase", Body: "SELECT 1 AS total, 2 AS n"}},
			},
			current: &catalog.Database{
				Views: []catalog.View{
					{Name: "top", Body: "SELECT m.total FROM app.zbase z, app.mid m"},
					{Name: "mid", Body: "SELECT total FROM app.zbase"},
				},
				MatViews: []catalog.MaterializedView{{Name: "zbase", Body: "SELECT 1 AS total"}},
			},
			prefixes: []string{`DROP VIEW "top"`, `DROP VIEW "mid"`, `DROP MATERIALIZED VIEW "zbase"`, `CREATE VIEW "mid"`, `CREATE VIEW "top"`},
		},
		{
			// Swapping base's columns is a change CREATE OR REPLACE VIEW
			// cannot make, so base is created again and what reads it is
			// replaced after it.
			name: "readers a view change takes with it",
			desired: &schemamodel.Database{Views: []schemamodel.View{
				{Name: "top", Body: "SELECT m.a FROM app.base b, app.mid m"},
				{Name: "mid", Body: "SELECT a FROM app.base"},
				{Name: "base", Body: "SELECT 2 AS b, 1 AS a"},
			}},
			current: &catalog.Database{Views: []catalog.View{
				{Name: "top", Body: "SELECT m.a FROM app.base b, app.mid m"},
				{Name: "mid", Body: "SELECT a FROM app.base"},
				{Name: "base", Body: "SELECT 1 AS a, 2 AS b"},
			}},
			prefixes: []string{`CREATE OR REPLACE VIEW "mid"`, `CREATE OR REPLACE VIEW "top"`},
		},
		{
			name:    "views the plan removes",
			desired: &schemamodel.Database{},
			// Removed views arrive sorted by name, the read one first.
			current: &catalog.Database{Views: []catalog.View{
				{Name: "abase", Body: "SELECT 1 AS id"},
				{Name: "zreport", Body: "SELECT id FROM app.abase"},
			}},
			prefixes: []string{`DROP VIEW "zreport"`, `DROP VIEW "abase"`},
		},
		{
			name: "a routine whose result is a table the plan adds",
			desired: &schemamodel.Database{
				Functions: []schemamodel.Function{{
					Name: "recent", Language: "plpgsql", Returns: "SETOF app.orders", Body: "BEGIN RETURN; END",
				}},
				Tables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
				Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
			},
			current:  &catalog.Database{},
			prefixes: []string{`CREATE TABLE "orders"`, `CREATE FUNCTION "recent"`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements := planInApp(c, test.desired, test.current)

			c.Assert(ascending(statementOrder(statements, test.prefixes...)), qt.IsTrue,
				qt.Commentf("%v in %q", statementOrder(statements, test.prefixes...), statements))
		})
	}
}
