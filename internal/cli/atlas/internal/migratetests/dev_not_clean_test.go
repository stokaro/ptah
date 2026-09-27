package migratetests_test

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

// The migrate verbs take a snapshot of the dev database before they use it,
// and the pinned community binary v1.3.0 refuses one that holds a table, at
// exit 1, touching nothing. Each expected sentence below is that binary's,
// measured on 2026-09-26 against a SQLite dev database holding `keep_me`; the
// verb decides what goes in front of it. Each test reads keep_me back
// afterwards (stokaro/ptah#3797).

// devRefusalReason is the part every verb prints the same way.
const devRefusalReason = `sql/migrate: connected database is not clean: found table "keep_me"`

// migrateDevFixture is a dev database and the directories the verbs read, in
// one directory.
type migrateDevFixture struct {
	dir string
}

// newMigrateDevFixture writes a hashed migration directory, an empty one, a
// schema file and a dev database that has run devDDL.
func newMigrateDevFixture(c *qt.C, devDDL string) migrateDevFixture {
	c.Helper()
	dir := c.TempDir()
	for _, sub := range []string{"mig", "empty", "out", "flyway", "flyway-empty"} {
		c.Assert(os.MkdirAll(filepath.Join(dir, sub), 0o755), qt.IsNil)
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "mig", "20260101000000_a.sql"),
		[]byte("CREATE TABLE t2 (id int NOT NULL, PRIMARY KEY (id));\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(filepath.Join(dir, "mig"), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "flyway", "V1__a.sql"),
		[]byte("CREATE TABLE t2 (id int NOT NULL, PRIMARY KEY (id));\n"), 0o600), qt.IsNil)
	out, err := atlastest.RunCompatOutput("migrate", "hash", "--dir", "file://"+filepath.Join(dir, "flyway"), "--dir-format", "flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(os.WriteFile(filepath.Join(dir, "s.sql"),
		[]byte("CREATE TABLE t2 (id int NOT NULL, PRIMARY KEY (id));\n"), 0o600), qt.IsNil)
	atlastest.SqliteExec(c, filepath.Join(dir, "dev.db"), devDDL)
	return migrateDevFixture{dir: dir}
}

// args spells a row's arguments with the fixture's paths.
func (f migrateDevFixture) args(template []string) []string {
	replacer := strings.NewReplacer("{dev}", "sqlite://"+filepath.Join(f.dir, "dev.db"), "{dir}", f.dir)
	args := make([]string, len(template))
	for i, arg := range template {
		args[i] = replacer.Replace(arg)
	}
	return args
}

// migrateDevVerbs are the migrate verbs that use the dev database, and the
// sentence each prints for one holding keep_me.
var migrateDevVerbs = []struct {
	name    string
	args    []string
	wantErr string
}{
	{
		name:    "migrate diff",
		args:    []string{"migrate", "diff", "x", "--dir", "file://{dir}/out", "--to", "file://{dir}/s.sql", "--dev-url", "{dev}"},
		wantErr: `sql/migrate: taking database snapshot: ` + devRefusalReason,
	},
	{
		name:    "migrate lint",
		args:    []string{"migrate", "lint", "--dir", "file://{dir}/mig", "--dev-url", "{dev}", "--latest", "1"},
		wantErr: `taking database snapshot: ` + devRefusalReason,
	},
	{
		name:    "migrate validate",
		args:    []string{"migrate", "validate", "--dir", "file://{dir}/mig", "--dev-url", "{dev}"},
		wantErr: `replaying the migration directory: sql/migrate: taking database snapshot: ` + devRefusalReason,
	},
	{
		// An empty directory replays nothing; the binary still takes the
		// snapshot.
		name:    "migrate validate of an empty directory",
		args:    []string{"migrate", "validate", "--dir", "file://{dir}/empty", "--dev-url", "{dev}"},
		wantErr: `replaying the migration directory: sql/migrate: taking database snapshot: ` + devRefusalReason,
	},
	{
		// A foreign layout is verified and replayed by this surface rather
		// than by the native command it forwards an Atlas directory to.
		name:    "migrate validate of a Flyway directory",
		args:    []string{"migrate", "validate", "--dir", "file://{dir}/flyway", "--dir-format", "flyway", "--dev-url", "{dev}"},
		wantErr: `replaying the migration directory: sql/migrate: taking database snapshot: ` + devRefusalReason,
	},
	{
		name:    "migrate validate of an empty Flyway directory",
		args:    []string{"migrate", "validate", "--dir", "file://{dir}/flyway-empty", "--dir-format", "flyway", "--dev-url", "{dev}"},
		wantErr: `replaying the migration directory: sql/migrate: taking database snapshot: ` + devRefusalReason,
	},
}

// TestCompatMigrateVerbsRefuseADevDatabaseThatHoldsATable runs each verb with
// the dev database holding keep_me.
func TestCompatMigrateVerbsRefuseADevDatabaseThatHoldsATable(t *testing.T) {
	for _, test := range migrateDevVerbs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newMigrateDevFixture(c, "CREATE TABLE keep_me (id INTEGER PRIMARY KEY); INSERT INTO keep_me VALUES (1);")

			out, err := atlastest.RunCompatOutput(fixture.args(test.args)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("%s", out))
			c.Assert(atlastest.SqliteQueryInt(c, filepath.Join(fixture.dir, "dev.db"), "SELECT count(*) FROM keep_me"), qt.Equals, 1)
		})
	}
}

// TestCompatMigrateVerbsUseADevDatabaseThatHoldsNoTable is the control: the
// same runs with the dev database holding only a view, which the binary
// counts as clean.
func TestCompatMigrateVerbsUseADevDatabaseThatHoldsNoTable(t *testing.T) {
	for _, test := range migrateDevVerbs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newMigrateDevFixture(c, "CREATE VIEW keep_v AS SELECT 1 AS id;")

			out, err := atlastest.RunCompatOutput(fixture.args(test.args)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		})
	}
}
