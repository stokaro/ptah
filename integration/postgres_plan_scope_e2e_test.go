//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A saved plan records what its URL covered. Applied through a URL that covers
// something else, the source fingerprint differs because the two reads do, and
// the refusal names both scopes rather than reporting a changed database. The
// database holds public.users and crm.customers; the desired schema adds
// public.orders.

// planScopeDesired is the desired schema both tests plan.
const planScopeDesired = "CREATE TABLE public.users (id integer PRIMARY KEY);\n" +
	"CREATE TABLE crm.customers (id integer PRIMARY KEY);\n" +
	"CREATE TABLE public.orders (id integer PRIMARY KEY);\n"

// newPlanScopeDatabase is the database both tests plan against, and the
// desired schema file.
func newPlanScopeDatabase(c *qt.C) (database, schema string) {
	c.Helper()
	database = newEmptyReadScopeDatabase(c, "plan_scope")
	for _, statement := range []string{
		"CREATE SCHEMA crm",
		"CREATE TABLE public.users (id integer PRIMARY KEY)",
		"CREATE TABLE crm.customers (id integer PRIMARY KEY)",
	} {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(planScopeDesired), 0o600), qt.IsNil)
	return database, schema
}

// TestSchemaApplyPlanNamesTheScopeItWasComputedForE2E plans through a URL
// that pins no search_path, which covers both schemas, and applies the plan
// through one pinning public. Nothing in the database changed; the refusal
// says the plan was computed for every schema, and nothing is applied.
func TestSchemaApplyPlanNamesTheScopeItWasComputedForE2E(t *testing.T) {
	c := qt.New(t)
	database, schema := newPlanScopeDatabase(c)
	planFile := filepath.Join(c.TempDir(), "realm.plan.json")

	planned := runPtahIn(c, "schema", "plan", "--db-url", database, "--schema-file", schema, "--output", planFile)
	applied := runPtahIn(c, "schema", "apply", "--db-url", planE2ESearchPath(c, database, "public"), "--plan", planFile, "--auto-approve")

	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "error: pre-planned migration is stale: the plan was computed for every schema "+
		`of the database, and the target URL reaches schema "public" (plan sha256:`)
	c.Assert(readScopeTables(c, database), qt.Equals, "public.users(id)")
}

// TestSchemaApplyPlanReportsAChangedDatabaseUnderItsOwnScopeE2E is the
// control: the plan is applied through the URL it was computed through, after
// a table was created in public, and the refusal reports the change.
func TestSchemaApplyPlanReportsAChangedDatabaseUnderItsOwnScopeE2E(t *testing.T) {
	c := qt.New(t)
	database, schema := newPlanScopeDatabase(c)
	pinned := planE2ESearchPath(c, database, "public")
	planFile := filepath.Join(c.TempDir(), "public.plan.json")

	planned := runPtahIn(c, "schema", "plan", "--db-url", pinned, "--schema-file", schema, "--output", planFile)
	execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE public.extra (id integer PRIMARY KEY)")
	applied := runPtahIn(c, "schema", "apply", "--db-url", pinned, "--plan", planFile, "--auto-approve")

	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "error: pre-planned migration is stale: the target database schema does not match "+
		"the plan's source fingerprint")
	c.Assert(readScopeTables(c, database), qt.Equals, "public.extra(id), public.users(id)")
}
