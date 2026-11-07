//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// heldPairs is a table with a UNIQUE over two columns, which SQLite enforces
// through an index of its own that no read lists.
const heldPairs = `CREATE TABLE "pairs" ("id" INTEGER NOT NULL PRIMARY KEY, "a" INTEGER, "b" INTEGER, CONSTRAINT "pairs_a_b_key" UNIQUE ("a", "b"))`

// declaredPairs declares pairs, with constraints as the YAML of its
// constraints.
func declaredPairs(constraints string) string {
	return "tables:\n  pairs:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n" +
		"      a: {type: INTEGER}\n      b: {type: INTEGER}\n" + constraints
}

// pairsKey is the YAML of the UNIQUE heldPairs holds.
const pairsKey = "constraints:\n  pairs_a_b_key:\n    table: pairs\n    type: UNIQUE\n    columns: [a, b]\n"

// A UNIQUE over two columns that the read reports is the declared one, and
// the plan leaves it. Declared away, the provider rebuilds the table without
// it and the database takes two equal pairs; declared again, the database
// refuses them. Each plan after an apply leaves the table.
func TestProviderUniqueConstraintChanges_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "", heldPairs)
	f.declare(c, declaredPairs(pairsKey))

	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
	read := f.success(c, "inspect", "--format", "json")
	c.Assert(read, qt.Contains, `"pairs_a_b_key"`)

	f.declare(c, declaredPairs(""))
	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")
	c.Assert(f.storedTable(c, "pairs"), qt.Equals, `CREATE TABLE "pairs" ("id" INTEGER NOT NULL PRIMARY KEY, "a" INTEGER, "b" INTEGER)`)
	c.Assert(f.insert(c, `INSERT INTO "pairs" ("id", "a", "b") VALUES (1, 1, 1), (2, 1, 1)`), qt.IsNil)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)

	c.Assert(f.insert(c, `DELETE FROM "pairs" WHERE "id" = 2`), qt.IsNil)
	f.declare(c, declaredPairs(pairsKey))
	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")
	c.Assert(f.storedTable(c, "pairs"), qt.Equals, heldPairs)
	c.Assert(f.insert(c, `INSERT INTO "pairs" ("id", "a", "b") VALUES (3, 1, 1)`), qt.ErrorMatches, `.*UNIQUE constraint failed.*`)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// A provider that reports UNIQUE constraints and does not plan constraint
// changes has the change refused by constraint and table, and the database
// keeps the constraint.
func TestProviderUniqueConstraintChanges_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "unplanned", heldPairs)
	f.declare(c, declaredPairs(""))

	_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Matches, `(?s).*changing constraint pairs_a_b_key of table (main\.)?pairs on the fixture engine, whose provider does not plan it.*`)
	c.Assert(f.storedTable(c, "pairs"), qt.Equals, heldPairs)
}
