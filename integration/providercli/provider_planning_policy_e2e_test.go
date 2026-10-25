//go:build integration

package providercli_test

import (
	"database/sql"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	// The test creates the fixture's table through a driver of its own, so the
	// starting state does not depend on the provider under test.
	_ "modernc.org/sqlite"

	"ptah.run/internal/testutils"
)

// rebuildFixture is a fixture database holding the empty table items, served
// by the operation-models program in the mode that honors the planning policy,
// and a desired schema that adds a NOT NULL column to the table. SQLite cannot
// add such a column in place, so the provider plans the change only as a
// rebuild of the table.
func rebuildFixture(c *qt.C) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "operation-models", testutils.FixtureRelease{})
	spec.Environment = []string{"FIXTURE_RENDER_MODE=policy"}
	f := newProviderCLI(c, spec)
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "note" TEXT)`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	schema := "tables:\n  items:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n      note: {type: TEXT}\n      flag: {type: INTEGER, not_null: true}\n"
	c.Assert(os.WriteFile(f.schemaPath, []byte(schema), 0o600), qt.IsNil)
	return f
}

// columns reads the columns of items back through the test's own driver.
func (f providerCLI) columns(c *qt.C) []string {
	c.Helper()
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT name FROM pragma_table_info('items') ORDER BY cid`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestProviderPlanningPolicy_HappyPath plans a change only a rebuild can make
// when the operator asks with --allow-table-rebuild: the provider receives the
// request in its planning policy, the plan applies, and the next plan finds
// nothing to do.
func TestProviderPlanningPolicy_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := rebuildFixture(c)

	planned := f.success(c, "plan", "--schema-file", f.schemaPath, "--allow-table-rebuild", "--output", f.planPath)
	c.Assert(planned, qt.Contains, "Native operation: example.org/fixture/table-operation\n  items: Table rebuild (warning)\n")
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.columns(c), qt.DeepEquals, []string{"id", "note", "flag"})
	c.Assert(f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json"), qt.Contains, `"outcome": "no-changes"`)
}

// TestProviderPlanningPolicy_FailurePath refuses the change when the operator
// did not ask, in the words the provider received for how to ask, and writes
// nothing.
func TestProviderPlanningPolicy_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := rebuildFixture(c)

	stdout, stderr, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath)

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `adding NOT NULL column "flag" to table "items" needs a rebuild of the table; ask for one with --allow-table-rebuild`)
	c.Assert(f.columns(c), qt.DeepEquals, []string{"id", "note"})
}
