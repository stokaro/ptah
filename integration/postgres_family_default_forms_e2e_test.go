//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// CockroachDB stores some defaults in a form of its own and spells no declared
// default: DEFAULT -1 as (-1), CURRENT_DATE as current_date(), and an array
// literal such as '{a,b}' as ARRAY['a', 'b']. Each test here applies a table
// and plans it again, so the stored form meets the declaration it came from.

// defaultFormServers are the engines each test runs on. PostgreSQL keeps the
// declared form and is the control.
var defaultFormServers = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "postgres", engine: dbtarget.PostgreSQL},
	{name: "cockroachdb", engine: dbtarget.CockroachDB},
}

// defaultForms are column declarations whose default a server may store in
// another form.
var defaultForms = []struct {
	name   string
	column string
}{
	{name: "a negative integer", column: `bigint DEFAULT -1`},
	{name: "a negative decimal", column: `numeric DEFAULT -2.5`},
	{name: "a negative float", column: `double precision DEFAULT -2.5`},
	{name: "CURRENT_DATE", column: `date DEFAULT CURRENT_DATE`},
	{name: "an empty array literal", column: `text[] DEFAULT '{}'`},
	{name: "an array literal of strings", column: `text[] DEFAULT '{a,b}'`},
	{name: "an array constructor of strings", column: `text[] DEFAULT ARRAY['a','b']`},
	{name: "an array literal of numbers", column: `bigint[] DEFAULT '{1,2}'`},
}

// TestDefaultInTheServersFormConvergesE2E applies each declaration and plans
// it again: the second run plans nothing.
func TestDefaultInTheServersFormConvergesE2E(t *testing.T) {
	for _, form := range defaultForms {
		for _, server := range defaultFormServers {
			t.Run(form.name+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				database := newPolicyClauseDatabase(c, server.engine)
				schema := writeDeclaredSchemaFile(c, "schema.sql", defaultCastTable(form.column))

				applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
				replanned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				c.Assert(replanned.ExitCode, qt.Equals, 0, historyComment(replanned))
				c.Assert(replanned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}

// changedDefaultForms are a declaration in one of the forms above and the same
// column with another value.
var changedDefaultForms = []struct {
	name    string
	column  string
	changed string
	planned string
}{
	{
		name: "another negative integer", column: `bigint DEFAULT -1`, changed: `bigint DEFAULT -2`,
		planned: `ALTER COLUMN "n" SET DEFAULT -2`,
	},
	{
		name: "a date literal for CURRENT_DATE", column: `date DEFAULT CURRENT_DATE`, changed: `date DEFAULT '2020-01-01'`,
		planned: `ALTER COLUMN "n" SET DEFAULT '2020-01-01'`,
	},
	{
		name: "an element in an empty array", column: `text[] DEFAULT '{}'`, changed: `text[] DEFAULT '{a}'`,
		planned: `ALTER COLUMN "n" SET DEFAULT '{a}'`,
	},
	{
		name: "another element", column: `text[] DEFAULT ARRAY['a','b']`, changed: `text[] DEFAULT ARRAY['a','c']`,
		planned: `ALTER COLUMN "n" SET DEFAULT ARRAY['a','c']`,
	},
}

// TestChangedDefaultInTheServersFormPlansE2E applies a declaration, then
// plans the column with another value: the plan sets the default, and once it
// is applied the next run plans nothing.
func TestChangedDefaultInTheServersFormPlansE2E(t *testing.T) {
	for _, form := range changedDefaultForms {
		for _, server := range defaultFormServers {
			t.Run(form.name+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				database := newPolicyClauseDatabase(c, server.engine)
				before := writeDeclaredSchemaFile(c, "before.sql", defaultCastTable(form.column))
				after := writeDeclaredSchemaFile(c, "after.sql", defaultCastTable(form.changed))

				created := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", before, "--auto-approve")
				planned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", after, "--dry-run")
				applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", after, "--auto-approve")
				replanned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", after, "--dry-run")

				c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Contains, form.planned)
				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				c.Assert(replanned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}

// TestGeneratedColumnOnAServerWithoutTemporaryTablesWarnsAboutNothingE2E
// applies a stored generated column on CockroachDB and plans it again. The
// server offers no temporary table, so the probe that spells the expression
// cannot run there, and the run says nothing about it: that answer is about
// the server, not about the column, and it holds on every run.
func TestGeneratedColumnOnAServerWithoutTemporaryTablesWarnsAboutNothingE2E(t *testing.T) {
	c := qt.New(t)
	database := newPolicyClauseDatabase(c, dbtarget.CockroachDB)
	schema := writeDeclaredSchemaFile(c, "schema.sql",
		`CREATE TABLE "public"."totals" ("id" bigint PRIMARY KEY, "a" bigint, "b" bigint GENERATED ALWAYS AS (a + 1) STORED);`+"\n")

	applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
	replanned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(replanned.ExitCode, qt.Equals, 0, historyComment(replanned))
	c.Assert(replanned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(replanned.Stderr, qt.Not(qt.Contains), "compared as written")
}
