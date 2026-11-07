//go:build integration

package providercli_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// enumFixture is the CLI over an empty fixture database, served by the
// provider that creates the tables it owns (the constraints fixture in its
// text mode) with the enum form named, and the declaration of a table with an
// enum column.
func enumFixture(c *qt.C, form string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "constraints", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_CONSTRAINT_MODE=text", "FIXTURE_ENUM_COLUMNS="+form)
	f := newProviderCLI(c, spec)
	f.declare(c, "tables:\n  items:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n"+
		"      kind: {type: ENUM, enum: [a, b, c], not_null: true}\n")
	return f
}

// tableNames are the tables the fixture database holds.
func (f providerCLI) tableNames(c *qt.C) []string {
	c.Helper()
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var result []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		result = append(result, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return result
}

// An engine that holds an enum's values under a CHECK of a TEXT column
// receives that column, as SQLite's descriptor builds it for the built-in
// engine: the database refuses a value outside the enum, and the read reports
// the column and the CHECK, so a second plan leaves the table. Without the
// form the host plans an enum type, which the provider path refuses.
func TestProviderEnumColumns_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := enumFixture(c, "check")

	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.storedTable(c, "items"), qt.Equals,
		`CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "kind" TEXT NOT NULL, CONSTRAINT "items_kind_check" CHECK (kind IN ('a', 'b', 'c')))`)
	c.Assert(f.insert(c, `INSERT INTO "items" ("id", "kind") VALUES (1, 'd')`), qt.ErrorMatches, `.*CHECK constraint failed.*`)
	c.Assert(f.insert(c, `INSERT INTO "items" ("id", "kind") VALUES (2, 'b')`), qt.IsNil)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// `schema render` builds the enum column the plan builds, offline, so the
// script it writes creates the column a read of the applied plan reports.
func TestProviderEnumColumns_Render(t *testing.T) {
	c := qt.New(t)
	f := enumFixture(c, "check")

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"schema", "render", "--provider-config", f.manifestPath, "--dialect", "fixture",
		"--schema-file", f.schemaPath})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains,
		`CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "kind" TEXT NOT NULL, CONSTRAINT "items_kind_check" CHECK (kind IN ('a', 'b', 'c')));`)
}

// An engine whose column type lists the values receives the column typed
// enum('a','b','c'), which the up file of a generated migration writes.
// SQLite cannot build that type, so the migration is written and not run.
func TestProviderEnumColumns_ValuesType(t *testing.T) {
	c := qt.New(t)
	f := enumFixture(c, "values")

	_, diagnostics, code, dir := f.generateMigration(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(upMigration(c, dir), qt.Contains, `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "kind" enum('a','b','c') NOT NULL);`)
}

// An engine that builds no enum has the column refused by its table and
// column before anything is planned, and an engine that states no form has
// the enum type refused by the provider path; either way the database holds
// no table.
func TestProviderEnumColumns_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		form string
		want string
	}{
		{name: "an engine without enums", form: "refused",
			want: `(?s).*column "kind" of table "items" is an enum column with the values \("a", "b", "c"\), and the fixture engine builds no enum.*`},
		{name: "an engine that states no form", form: "", want: `(?s).*does not support SchemaDiff\.EnumsAdded.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := enumFixture(c, test.form)

			_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

			c.Assert(code, qt.Equals, 2)
			c.Assert(diagnostics, qt.Matches, test.want)
			c.Assert(f.tableNames(c), qt.IsNil)
		})
	}
}
