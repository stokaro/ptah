//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// inspectFixture is a PostgreSQL database of users, an index on it and posts
// with a foreign key to users.
const inspectFixture = `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL);
CREATE INDEX users_name ON users (name);
CREATE TABLE posts (id integer PRIMARY KEY, user_id integer NOT NULL REFERENCES users (id));`

// pinnedCreationPlan is what both binaries write for inspectFixture read with
// a URL pinned to public: a change comment over each statement, one statement
// a line, and the table a foreign key references before the table that holds
// it.
const pinnedCreationPlan = "-- Create \"users\" table\n" +
	"CREATE TABLE \"users\" (\"id\" integer NOT NULL, \"name\" text NOT NULL, PRIMARY KEY (\"id\"));\n" +
	"-- Create index \"users_name\" to table: \"users\"\n" +
	"CREATE INDEX \"users_name\" ON \"users\" (\"name\");\n" +
	"-- Create \"posts\" table\n" +
	"CREATE TABLE \"posts\" (\"id\" integer NOT NULL, \"user_id\" integer NOT NULL, PRIMARY KEY (\"id\"), " +
	"CONSTRAINT \"posts_user_id_fkey\" FOREIGN KEY (\"user_id\") REFERENCES \"users\" (\"id\") " +
	"ON UPDATE NO ACTION ON DELETE NO ACTION);\n"

// inspectPinnedSQL writes inspectFixture and runs the `sql` helper of schema
// inspect with binary on a URL pinned to public.
func inspectPinnedSQL(c *qt.C, binary string) clirun.Result {
	c.Helper()
	target := postgresScratchDevURL(c, "ptah_output_bytes")
	execPostgresFamilySQL(c, c.Context(), target, inspectFixture)
	return runFormatTemplateVerb(c, binary, c.TempDir(), nil,
		"schema", "inspect", "--format", "{{ sql . }}", "--url", pinnedDevURL(c, target, "public"))
}

// TestCompatSchemaInspectSQLWritesTheCreationPlanE2E is the shipped compat
// binary writing the creation plan.
func TestCompatSchemaInspectSQLWritesTheCreationPlanE2E(t *testing.T) {
	c := qt.New(t)

	got := inspectPinnedSQL(c, clirun.Build(c, clirun.Compat))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, pinnedCreationPlan)
}

// TestReferenceSchemaInspectSQLWritesTheCreationPlanE2E takes the same
// measurement against the pinned community binary.
func TestReferenceSchemaInspectSQLWritesTheCreationPlanE2E(t *testing.T) {
	c := qt.New(t)

	got := inspectPinnedSQL(c, requireEngineReference(t))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, pinnedCreationPlan)
}

// applyHeaderSchema declares the public schema a new database holds and one
// table in it.
const applyHeaderSchema = `schema "public" {
  comment = "standard public schema"
}
table "t" {
  schema = schema.public
  column "id" {
    type = integer
  }
}
`

// dryRunApplyPlan runs a dry run of schema apply with binary that creates
// table t on a new database read whole.
func dryRunApplyPlan(c *qt.C, binary string) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.hcl"), []byte(applyHeaderSchema), 0o600), qt.IsNil)
	return runFormatTemplateVerb(c, binary, dir, nil,
		"schema", "apply", "--to", "file://schema.hcl", "--dry-run", "--url", postgresScratchDevURL(c, "ptah_output_bytes"))
}

// dryRunApplyHeader is the line both binaries head a dry-run plan with.
const dryRunApplyHeader = "-- Planned Changes:\n"

// TestCompatSchemaApplyDryRunHeaderE2E is the shipped compat binary heading
// its plan as the pinned community binary does.
func TestCompatSchemaApplyDryRunHeaderE2E(t *testing.T) {
	c := qt.New(t)

	got := dryRunApplyPlan(c, clirun.Build(c, clirun.Compat))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Matches, `(?s)`+dryRunApplyHeader+`-- Create "t" table\n.*`)
}

// TestReferenceSchemaApplyDryRunHeaderE2E takes the same measurement against
// the pinned community binary.
func TestReferenceSchemaApplyDryRunHeaderE2E(t *testing.T) {
	c := qt.New(t)

	got := dryRunApplyPlan(c, requireEngineReference(t))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Matches, `(?s)`+dryRunApplyHeader+`-- Create "t" table\n.*`)
}

// wholeDatabaseCreationPlan is the plan both binaries write for the first
// inspectFixture read whole: every object names its schema.
const wholeDatabaseCreationPlan = "-- Create \"users\" table\n" +
	"CREATE TABLE \"public\".\"users\" (\"id\" integer NOT NULL, \"name\" text NOT NULL, PRIMARY KEY (\"id\"));\n" +
	"-- Create index \"users_name\" to table: \"users\"\n" +
	"CREATE INDEX \"users_name\" ON \"public\".\"users\" (\"name\");\n" +
	"-- Create \"posts\" table\n" +
	"CREATE TABLE \"public\".\"posts\" (\"id\" integer NOT NULL, \"user_id\" integer NOT NULL, PRIMARY KEY (\"id\"), " +
	"CONSTRAINT \"posts_user_id_fkey\" FOREIGN KEY (\"user_id\") REFERENCES \"public\".\"users\" (\"id\") " +
	"ON UPDATE NO ACTION ON DELETE NO ACTION);\n"

// inspectWholeDatabaseSQL writes inspectFixture and runs the `sql`
// helper of schema inspect with binary on a URL that pins no schema.
func inspectWholeDatabaseSQL(c *qt.C, binary string) clirun.Result {
	c.Helper()
	target := postgresScratchDevURL(c, "ptah_output_bytes")
	execPostgresFamilySQL(c, c.Context(), target, inspectFixture)
	return runFormatTemplateVerb(c, binary, c.TempDir(), nil,
		"schema", "inspect", "--format", "{{ sql . }}", "--url", target)
}

// TestCompatSchemaInspectSQLOfAWholeDatabaseE2E writes the plan that creates a
// database read whole on a new database of the same server, which holds the
// public schema and its comment. So it creates neither.
func TestCompatSchemaInspectSQLOfAWholeDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	compat := clirun.Build(c, clirun.Compat)

	got := inspectWholeDatabaseSQL(c, compat)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, wholeDatabaseCreationPlan)
}

// TestReferenceSchemaInspectSQLOfAWholeDatabaseE2E is the measurement: the
// pinned community binary writes the same plan under two statements for the
// public schema a new database already holds.
func TestReferenceSchemaInspectSQLOfAWholeDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	reference := requireEngineReference(t)

	got := inspectWholeDatabaseSQL(c, reference)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "-- Add new schema named \"public\"\n"+
		"CREATE SCHEMA IF NOT EXISTS \"public\";\n"+
		"-- Set comment to schema: \"public\"\n"+
		"COMMENT ON SCHEMA \"public\" IS 'standard public schema';\n"+
		wholeDatabaseCreationPlan)
}
