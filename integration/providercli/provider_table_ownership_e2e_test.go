//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// ownedTables declares the tables named, each with one integer key.
func ownedTables(names ...string) string {
	schema := "tables:\n"
	for _, name := range names {
		schema += "  " + name + ":\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n"
	}
	return schema
}

// A planning service owns the fixture's tables with no table setting
// (planning.table-ownership): the host hands it each table the declaration
// adds and each one it removes, and the service creates and drops the table in
// place of the host's step, in its own statement text. The database holds the
// tables the declaration names, and a second plan changes nothing.
func TestProviderTableOwnership_HappyPath(t *testing.T) {
	c := qt.New(t)
	spec := fixtureReleaseSpec(c, "constraints", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_CONSTRAINT_MODE=text", "FIXTURE_TABLE_OWNERSHIP=service")
	f := newProviderCLI(c, spec)

	f.declare(c, ownedTables("items", "notes"))
	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.tableNames(c), qt.DeepEquals, []string{"items", "notes"})
	c.Assert(f.storedTable(c, "items"), qt.Equals, `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY)`)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)

	f.declare(c, ownedTables("notes"))
	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.tableNames(c), qt.DeepEquals, []string{"notes"})
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}
