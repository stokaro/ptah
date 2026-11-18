//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// scopedPlanFixture is a PostgreSQL database whose schema app holds a table,
// and whose public schema holds pgcrypto, an extension of the whole database.
const scopedPlanFixture = `CREATE EXTENSION pgcrypto WITH SCHEMA public;
CREATE SCHEMA app;
CREATE TABLE app.users (id integer PRIMARY KEY, name text NOT NULL);`

// inspectScopedPlan writes scopedPlanFixture and runs the `sql` helper of
// schema inspect with binary, with extra arguments after the URL.
func inspectScopedPlan(c *qt.C, binary string, extra ...string) clirun.Result {
	c.Helper()
	target := postgresScratchDevURL(c, "ptah_scoped_plan")
	execPostgresFamilySQL(c, c.Context(), target, scopedPlanFixture)
	args := append([]string{"schema", "inspect", "--format", "{{ sql . }}", "--url", target}, extra...)
	return runFormatTemplateVerb(c, binary, c.TempDir(), nil, args...)
}

// scopedPlanNote is the note ptah-compat writes for the extension the plan of
// a read of schema app leaves out.
const scopedPlanNote = "note: the read covers some schemas of the database, so the plan creates what they hold and leaves out " +
	`what belongs to the database: extension "pgcrypto" in schema "public"; inspect the whole database to plan them` + "\n"

// TestCompatScopedInspectPlanLeavesOutTheDatabaseObjectsE2E is the shipped
// compat binary planning a read of schema app: the schema and its table, and
// not the extension installed in public, which the note names.
func TestCompatScopedInspectPlanLeavesOutTheDatabaseObjectsE2E(t *testing.T) {
	c := qt.New(t)

	got := inspectScopedPlan(c, clirun.Build(c, clirun.Compat), "--schema", "app")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "-- Add new schema named \"app\"\n"+
		"CREATE SCHEMA IF NOT EXISTS \"app\";\n"+
		"-- Create \"users\" table\n"+
		"CREATE TABLE \"app\".\"users\" (\"id\" integer NOT NULL, \"name\" text NOT NULL, PRIMARY KEY (\"id\"));\n")
	c.Assert(got.Stderr, qt.Contains, scopedPlanNote)
}

// TestCompatWholeInspectPlanCreatesTheDatabaseObjectsE2E is the control: a
// read of the whole database plans the extension, and writes no note.
func TestCompatWholeInspectPlanCreatesTheDatabaseObjectsE2E(t *testing.T) {
	c := qt.New(t)

	got := inspectScopedPlan(c, clirun.Build(c, clirun.Compat))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Matches, `(?s).*-- Create "pgcrypto" extension\n`+
		`CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "public" VERSION '[0-9.]+';\n.*`)
	c.Assert(got.Stderr, qt.Not(qt.Contains), "leaves out what belongs to the database")
}

// TestReferenceScopedInspectPlanE2E takes the measurement against the pinned
// community binary, which plans no extension on any read and says nothing:
// it writes CREATE SCHEMA without IF NOT EXISTS, and no note.
func TestReferenceScopedInspectPlanE2E(t *testing.T) {
	c := qt.New(t)

	got := inspectScopedPlan(c, requireEngineReference(t), "--schema", "app")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "-- Add new schema named \"app\"\n"+
		"CREATE SCHEMA \"app\";\n"+
		"-- Create \"users\" table\n"+
		"CREATE TABLE \"app\".\"users\" (\"id\" integer NOT NULL, \"name\" text NOT NULL, PRIMARY KEY (\"id\"));\n")
	c.Assert(got.Stderr, qt.Equals, "")
}

// TestCompatScopedInspectPlanNotesOnceE2E is a template that calls the helper
// twice: it writes the plan twice and the note once.
func TestCompatScopedInspectPlanNotesOnceE2E(t *testing.T) {
	c := qt.New(t)
	target := postgresScratchDevURL(c, "ptah_scoped_plan")
	execPostgresFamilySQL(c, c.Context(), target, scopedPlanFixture)

	got := runFormatTemplateVerb(c, clirun.Build(c, clirun.Compat), c.TempDir(), nil,
		"schema", "inspect", "--format", "{{ sql . }}{{ sql . }}", "--url", target, "--schema", "app")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(strings.Count(got.Stdout, "-- Create \"users\" table\n"), qt.Equals, 2)
	c.Assert(strings.Count(got.Stderr, scopedPlanNote), qt.Equals, 1)
}

// TestCompatIncludeInspectPlanKeepsTheExtensionE2E pins what --include does
// to an extension: the selection keeps schema public, so pgcrypto, installed
// in it, is planned, and nothing is noted.
func TestCompatIncludeInspectPlanKeepsTheExtensionE2E(t *testing.T) {
	c := qt.New(t)

	got := inspectScopedPlan(c, clirun.Build(c, clirun.Compat), "--include", "app.*")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Matches, `(?s).*CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "public" VERSION '[0-9.]+';\n.*`)
	c.Assert(got.Stderr, qt.Not(qt.Contains), "leaves out what belongs to the database")
}

// TestCompatExcludeInspectPlanDropsTheExtensionE2E pins the other selector:
// --exclude of schema public removes pgcrypto with it, as a selector removes
// what it names, and nothing is noted.
func TestCompatExcludeInspectPlanDropsTheExtensionE2E(t *testing.T) {
	c := qt.New(t)

	got := inspectScopedPlan(c, clirun.Build(c, clirun.Compat), "--exclude", "public")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Not(qt.Contains), "pgcrypto")
	c.Assert(got.Stderr, qt.Not(qt.Contains), "leaves out what belongs to the database")
}
