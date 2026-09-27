package schematests_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// The schema verbs take a snapshot of the dev database before they use it,
// and the pinned community binary v1.3.0 refuses one that holds a table, at
// exit 1, touching nothing. Each expected sentence below is that binary's,
// measured on 2026-09-26 against a SQLite dev database holding `keep_me`: the
// snapshot prefix for SQL the dev database runs, none for an HCL document.
// Each test reads keep_me back afterwards (stokaro/ptah#3797).

const (
	devSnapshotRefusal = `sql/migrate: taking database snapshot: sql/migrate: connected database is not clean: found table "keep_me"`
	devHCLRefusal      = `sql/migrate: connected database is not clean: found table "keep_me"`
)

// devCleanFixture is a dev database, a target and the sources the verbs read,
// in one directory.
type devCleanFixture struct {
	dir string
}

// newDevCleanFixture writes the sources and a dev database that has run devDDL.
func newDevCleanFixture(c *qt.C, devDDL string) devCleanFixture {
	c.Helper()
	dir := c.TempDir()
	write := func(name, body string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	write("s.sql", "CREATE TABLE t2 (id int NOT NULL, PRIMARY KEY (id));\n")
	write("s2.sql", "CREATE TABLE t2 (id int NOT NULL, PRIMARY KEY (id));\nCREATE TABLE t3 (id int NOT NULL, PRIMARY KEY (id));\n")
	write("s.hcl", `schema "main" {}
table "t2" {
  schema = schema.main
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`)
	c.Assert(os.MkdirAll(filepath.Join(dir, "mig"), 0o755), qt.IsNil)
	write(filepath.Join("mig", "20260101000000_a.sql"), "CREATE TABLE t2 (id int NOT NULL, PRIMARY KEY (id));\n")
	_, err := migratesum.WriteWithFormat(filepath.Join(dir, "mig"), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	atlastest.SqliteExec(c, filepath.Join(dir, "dev.db"), devDDL)
	atlastest.SqliteExec(c, filepath.Join(dir, "target.db"), "SELECT 1")
	return devCleanFixture{dir: dir}
}

// args spells a row's arguments with the fixture's paths.
func (f devCleanFixture) args(template []string) []string {
	replacer := strings.NewReplacer(
		"{dev}", "sqlite://"+filepath.Join(f.dir, "dev.db"),
		"{target}", "sqlite://"+filepath.Join(f.dir, "target.db"),
		"{dir}", f.dir,
	)
	args := make([]string, len(template))
	for i, arg := range template {
		args[i] = replacer.Replace(arg)
	}
	return args
}

// keptRows counts keep_me's rows.
func (f devCleanFixture) keptRows(c *qt.C) int {
	c.Helper()
	return atlastest.SqliteQueryInt(c, filepath.Join(f.dir, "dev.db"), "SELECT count(*) FROM keep_me")
}

// schemaDevVerbs are the schema verbs with a source that uses the dev
// database, and the sentence each prints for a dev database holding keep_me.
var schemaDevVerbs = []struct {
	name    string
	args    []string
	wantErr string
}{
	{
		name:    "schema inspect of a SQL file",
		args:    []string{"schema", "inspect", "-u", "file://{dir}/s.sql", "--dev-url", "{dev}"},
		wantErr: devSnapshotRefusal,
	},
	{
		name:    "schema inspect of an HCL file",
		args:    []string{"schema", "inspect", "-u", "file://{dir}/s.hcl", "--dev-url", "{dev}"},
		wantErr: devHCLRefusal,
	},
	{
		name:    "schema inspect of a migration directory",
		args:    []string{"schema", "inspect", "-u", "file://{dir}/mig", "--dev-url", "{dev}"},
		wantErr: devSnapshotRefusal,
	},
	{
		// Two files are compared as read, without a reset of the dev
		// database; the binary still takes its snapshot.
		name:    "schema diff of two SQL files",
		args:    []string{"schema", "diff", "--from", "file://{dir}/s.sql", "--to", "file://{dir}/s2.sql", "--dev-url", "{dev}"},
		wantErr: devSnapshotRefusal,
	},
	{
		name:    "schema diff of two HCL files",
		args:    []string{"schema", "diff", "--from", "file://{dir}/s.hcl", "--to", "file://{dir}/s.hcl", "--dev-url", "{dev}"},
		wantErr: devHCLRefusal,
	},
	{
		name:    "schema diff of a database with an HCL file",
		args:    []string{"schema", "diff", "--from", "{target}", "--to", "file://{dir}/s.hcl", "--dev-url", "{dev}"},
		wantErr: devHCLRefusal,
	},
	{
		name:    "schema apply of a SQL file",
		args:    []string{"schema", "apply", "-u", "{target}", "--to", "file://{dir}/s.sql", "--dev-url", "{dev}", "--auto-approve"},
		wantErr: devSnapshotRefusal,
	},
	{
		name:    "schema apply of an HCL file",
		args:    []string{"schema", "apply", "-u", "{target}", "--to", "file://{dir}/s.hcl", "--dev-url", "{dev}", "--auto-approve"},
		wantErr: devHCLRefusal,
	},
}

// TestCompatSchemaVerbsRefuseADevDatabaseThatHoldsATable runs each verb with
// the dev database holding keep_me.
func TestCompatSchemaVerbsRefuseADevDatabaseThatHoldsATable(t *testing.T) {
	for _, test := range schemaDevVerbs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newDevCleanFixture(c, "CREATE TABLE keep_me (id INTEGER PRIMARY KEY); INSERT INTO keep_me VALUES (1);")

			out, err := atlastest.RunCompatOutput(fixture.args(test.args)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("%s", out))
			c.Assert(fixture.keptRows(c), qt.Equals, 1)
		})
	}
}

// TestCompatSchemaVerbsUseADevDatabaseThatHoldsNoTable is the control: the
// same runs with the dev database holding only a view, which the binary
// counts as clean.
func TestCompatSchemaVerbsUseADevDatabaseThatHoldsNoTable(t *testing.T) {
	for _, test := range schemaDevVerbs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newDevCleanFixture(c, "CREATE VIEW keep_v AS SELECT 1 AS id;")

			out, err := atlastest.RunCompatOutput(fixture.args(test.args)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		})
	}
}

// TestCompatSchemaDiffOfTwoDatabasesLeavesTheDevDatabaseAlone is the other
// control: two database sides do not use the dev database, and the binary
// diffs them at exit 0 with keep_me in it.
func TestCompatSchemaDiffOfTwoDatabasesLeavesTheDevDatabaseAlone(t *testing.T) {
	c := qt.New(t)
	fixture := newDevCleanFixture(c, "CREATE TABLE keep_me (id INTEGER PRIMARY KEY); INSERT INTO keep_me VALUES (1);")

	out, err := atlastest.RunCompatOutput(fixture.args([]string{
		"schema", "diff", "--from", "{target}", "--to", "{target}", "--dev-url", "{dev}",
	})...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(fixture.keptRows(c), qt.Equals, 1)
}
