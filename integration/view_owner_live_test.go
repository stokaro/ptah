//go:build integration

package integration_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// heldViewOwners reads back each view and materialized view of public with
// the role that owns it.
func heldViewOwners(c *qt.C, db *sql.DB) []string {
	c.Helper()
	return queryStrings(c, db, `SELECT c.relname || '=' || pg_get_userbyid(c.relowner)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') ORDER BY 1`)
}

// connectedRole is the role db connects as.
func connectedRole(c *qt.C, db *sql.DB) string {
	c.Helper()
	var role string
	c.Assert(db.QueryRowContext(c.Context(), "SELECT current_user").Scan(&role), qt.IsNil)
	return role
}

// withOwners is text with {role} and {admin} replaced.
func withOwners(text, role, admin string) string {
	return strings.ReplaceAll(withRole(text, role), "{admin}", admin)
}

// viewOwnerFrontendCases declare the table, a view and a materialized view
// owned by {role} through each frontend.
var viewOwnerFrontendCases = []struct {
	name, file, document string
}{
	{
		name: "Go annotations",
		file: "schema.go",
		document: `package schema

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null
	ID int
	//ptah:schema:field name="amount" type="NUMERIC" not_null
	Amount float64
}

//ptah:schema:view name="report" body="SELECT id, amount FROM orders" owner="{role}"
type Report struct{}

//ptah:schema:matview name="totals" body="SELECT sum(amount) AS total FROM orders" owner="{role}"
type Totals struct{}
`,
	},
	{
		name: "YAML",
		file: "schema.yaml",
		document: `tables:
  orders:
    columns:
      id:
        type: INTEGER
        primary: true
        not_null: true
      amount:
        type: NUMERIC
        not_null: true
views:
  report:
    body: SELECT id, amount FROM orders
    owner: "{role}"
matviews:
  totals:
    body: SELECT sum(amount) AS total FROM orders
    owner: "{role}"
`,
	},
	{
		name: "SQL",
		file: "schema.sql",
		document: optionsSchema(optionsView, "", "") + "ALTER VIEW report OWNER TO {role};\n" +
			"ALTER MATERIALIZED VIEW totals OWNER TO {role};\n",
	},
}

// TestViewOwnerFromEveryFrontendLive applies each of
// [viewOwnerFrontendCases] to a PostgreSQL database that holds the table
// alone, and reads back the owners the server gave the views. A comparison
// afterwards finds nothing to do, and so does one against a database that
// SQL built with the same owners: every frontend declares the same owner.
func TestViewOwnerFromEveryFrontendLive(t *testing.T) {
	for _, test := range viewOwnerFrontendCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, db, role := viewRebuildTarget(c, optionsTable)
			desired := loadFrontendSchema(c, test.file, withRole(test.document, role))

			up, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
				compareDeclaredLive(c, url, desired), platform.Postgres,
				planext.Options{Capabilities: capability.ForDialect(platform.Postgres)})
			c.Assert(err, qt.IsNil)
			runRebuildStatements(c, db, up)

			c.Assert(heldViewOwners(c, db), qt.DeepEquals, []string{"report=" + role, "totals=" + role})
			c.Assert(compareDeclaredLive(c, url, desired).HasChanges(), qt.IsFalse)
			builtURL, _, _ := viewRebuildTarget(c, optionsSchema(optionsView, "", "")+
				"ALTER VIEW report OWNER TO "+role+";\nALTER MATERIALIZED VIEW totals OWNER TO "+role+";\n")
			c.Assert(compareDeclaredLive(c, builtURL, desired).HasChanges(), qt.IsFalse)
		})
	}
}

// viewOwnerBothWaysCase is one change of a view's owner, planned up and down
// the way `ptah migrations generate` plans it. {role} is the test's role and
// {admin} the role it connects as.
type viewOwnerBothWaysCase struct {
	name           string
	live, declared string
	// after is what [heldViewOwners] reads back after the up migration. The
	// rollback has to read back what the database held before it.
	after []string
}

var viewOwnerBothWaysCases = []viewOwnerBothWaysCase{
	{
		name:     "an owner given in place",
		live:     optionsSchema(optionsView, "", ""),
		declared: optionsSchema(optionsView, "", "") + "ALTER VIEW report OWNER TO {role};\nALTER MATERIALIZED VIEW totals OWNER TO {role};\n",
		after:    []string{"report={role}", "totals={role}"},
	},
	{
		// The wide view adds a column at the end, which CREATE OR REPLACE VIEW
		// does in place; the rollback drops it, which rebuilds the view.
		name:     "a replaced view gets the declared owner",
		live:     optionsSchema(optionsView, "", ""),
		declared: optionsSchema(optionsWide, "", "") + "ALTER VIEW report OWNER TO {role};\n",
		after:    []string{"report={role}", "totals={admin}"},
	},
	{
		// The narrow view drops a column, so the up migration rebuilds the
		// view, and the rollback replaces it in place, which keeps its owner.
		name:     "a rebuild keeps an undeclared owner",
		live:     optionsSchema(optionsWide, "", "") + "ALTER VIEW report OWNER TO {role};\n",
		declared: optionsSchema(optionsView, "", ""),
		after:    []string{"report={role}", "totals={admin}"},
	},
}

// TestViewOwnerBothWaysLive applies each of [viewOwnerBothWaysCases] on
// PostgreSQL in both statement styles: up, down and up again, each in one
// transaction. After each step the server holds the owners the step's schema
// declares, or keeps the one it found where the schema declares none, and a
// comparison finds nothing left to do.
func TestViewOwnerBothWaysLive(t *testing.T) {
	for _, test := range viewOwnerBothWaysCases {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db, role := viewRebuildTarget(c, test.live)
				admin := connectedRole(c, db)
				declared, live := withOwners(test.declared, role, admin), withOwners(test.live, role, admin)
				after := make([]string, len(test.after))
				for i, owner := range test.after {
					after[i] = withOwners(owner, role, admin)
				}
				before := heldViewOwners(c, db)

				up, down := planRebuildBothWays(c, url, declared, style.style)
				c.Logf("up:\n%s\ndown:\n%s", strings.Join(up, ";\n"), strings.Join(down, ";\n"))

				runRebuildStatements(c, db, up)
				c.Assert(heldViewOwners(c, db), qt.DeepEquals, after)
				c.Assert(rebuildPending(c, url, declared).HasChanges(), qt.IsFalse)
				runRebuildStatements(c, db, down)
				c.Assert(heldViewOwners(c, db), qt.DeepEquals, before)
				c.Assert(rebuildPending(c, url, live).HasChanges(), qt.IsFalse)
				runRebuildStatements(c, db, up)
				c.Assert(heldViewOwners(c, db), qt.DeepEquals, after)
			})
		}
	}
}
