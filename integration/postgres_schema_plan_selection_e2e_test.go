//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/clirun"
)

// `schema plan --schemas` reads the schemas it names and no other, as `schema
// apply --schemas` does, and the plan file records them. The database holds
// public.users and the two tables of crm, its URL pins no search_path, and the
// desired schema declares public.users and a new public.accounts. A plan at
// the selection creates accounts and leaves crm alone; a plan without one
// reads every schema, drops both crm tables and warns about crm.

// schemaPlanSelectionDeclared is the desired schema every test plans for.
const schemaPlanSelectionDeclared = "CREATE TABLE public.users (id integer PRIMARY KEY);\n" +
	"CREATE TABLE public.accounts (id integer PRIMARY KEY);\n"

// schemaPlanSelectionCreate is the statement each plan carries for the one
// table the desired schema adds.
const schemaPlanSelectionCreate = `CREATE TABLE "public"."accounts"`

// TestSchemaPlanAtASelectionPlansNothingOutsideItE2E plans at --schemas public.
// The plan creates accounts, drops nothing in crm, warns about nothing, and the
// file records the selection.
func TestSchemaPlanAtASelectionPlansNothingOutsideItE2E(t *testing.T) {
	c := qt.New(t)
	database := newUndescribedSchemaDatabase(c)

	run, path := runSchemaPlanSelection(c, database, "--schemas", "public")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, schemaPlanSelectionCreate)
	c.Assert(run.Stdout, qt.Not(qt.Contains), `"crm"`)
	c.Assert(run.Stderr, qt.Not(qt.Contains), undescribedAnyWarning)
	plan, err := atlasschema.ReadPlanFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Schemas, qt.DeepEquals, []string{"public"})
	c.Assert(plan.SchemasBeyondURL, qt.IsNil)
	c.Assert(strings.Join(plan.StatementSQL(), "\n"), qt.Contains, schemaPlanSelectionCreate)
	c.Assert(strings.Join(plan.StatementSQL(), "\n"), qt.Not(qt.Contains), `"crm"`)
}

// TestSchemaPlanWithoutASelectionWarnsAboutTheDropsE2E is the same plan with no
// selection. The URL pins no search_path, so the read covers crm, the plan
// drops both of its tables, and the warning `schema apply` prints says so once.
func TestSchemaPlanWithoutASelectionWarnsAboutTheDropsE2E(t *testing.T) {
	c := qt.New(t)
	database := newUndescribedSchemaDatabase(c)

	run, path := runSchemaPlanSelection(c, database)

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, schemaPlanSelectionCreate)
	c.Assert(run.Stdout, qt.Contains, undescribedDropFirst)
	c.Assert(run.Stdout, qt.Contains, undescribedDropSecond)
	c.Assert(run.Stderr, qt.Contains, undescribedWarning)
	c.Assert(strings.Count(run.Stderr, undescribedAnyWarning), qt.Equals, 1)
	plan, err := atlasschema.ReadPlanFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Schemas, qt.IsNil)
	// A saved statement carries no terminator.
	c.Assert(strings.Join(plan.StatementSQL(), "\n"), qt.Contains, strings.TrimSuffix(undescribedDropFirst, ";"))
	c.Assert(strings.Join(plan.StatementSQL(), "\n"), qt.Contains, strings.TrimSuffix(undescribedDropSecond, ";"))
}

// TestSchemaApplyRunsASelectionPlanAtItsScopeE2E applies the plan saved at
// --schemas public. A table created in crm after planning is outside the
// selection, so the plan is not stale, and the apply creates accounts and
// leaves every crm table in place.
func TestSchemaApplyRunsASelectionPlanAtItsScopeE2E(t *testing.T) {
	c := qt.New(t)
	database := newUndescribedSchemaDatabase(c)
	planned, path := runSchemaPlanSelection(c, database, "--schemas", "public")
	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE crm.invoices (id integer PRIMARY KEY)")

	applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--plan", path, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(schemaPlanSelectionTables(c, database), qt.DeepEquals, []string{
		"crm.customers", "crm.invoices", "crm.orders", "public.accounts", "public.users",
	})
}

// TestSchemaApplyRefusesASelectionPlanStaleInsideItE2E is the control for the
// test above: a change inside the selection after planning still makes the
// plan stale, so the verification read covers the selected schema rather than
// nothing.
func TestSchemaApplyRefusesASelectionPlanStaleInsideItE2E(t *testing.T) {
	c := qt.New(t)
	database := newUndescribedSchemaDatabase(c)
	planned, path := runSchemaPlanSelection(c, database, "--schemas", "public")
	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE public.invoices (id integer PRIMARY KEY)")

	applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--plan", path, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "pre-planned migration is stale")
	c.Assert(schemaPlanSelectionTables(c, database), qt.DeepEquals, []string{
		"crm.customers", "crm.orders", "public.invoices", "public.users",
	})
}

// runSchemaPlanSelection saves the plan for the desired schema against
// database with the extra arguments, and returns the run and the plan file's
// path.
func runSchemaPlanSelection(c *qt.C, database string, extra ...string) (run clirun.Result, path string) {
	c.Helper()
	dir := c.TempDir()
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(schemaPlanSelectionDeclared), 0o600), qt.IsNil)
	path = filepath.Join(dir, "selection.plan.json")
	args := append([]string{"schema", "plan", "--db-url", database, "--schema-file", schema, "--output", path}, extra...)
	return runPtahIn(c, args...), path
}

// schemaPlanSelectionTables lists the tables of public and crm as
// schema.table, in name order.
func schemaPlanSelectionTables(c *qt.C, database string) []string {
	c.Helper()
	rows, err := openReadScopeDatabase(c, database).QueryContext(c.Context(), `
		SELECT table_schema || '.' || table_name
		FROM information_schema.tables
		WHERE table_schema IN ('public', 'crm') AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		c.Assert(rows.Scan(&table), qt.IsNil)
		tables = append(tables, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}
