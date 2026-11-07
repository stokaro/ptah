package migratedown_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/migratedown"
	"ptah.run/internal/cli/migrateup"
	"ptah.run/migration/migrator"
)

// documentRollback is a directory of two migrations applied to a fresh SQLite
// database, which a test rolls back.
type documentRollback struct {
	dir   string
	dbURL string
}

func newDocumentRollback(c *qt.C) documentRollback {
	c.Helper()
	dir := c.TempDir()
	for name, content := range map[string]string{
		"0000000001_users.up.sql":    "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql":  "DROP TABLE users;\n",
		"0000000002_orders.up.sql":   "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"0000000002_orders.down.sql": "DROP TABLE orders;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	f := documentRollback{dir: dir, dbURL: atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "document.db"))}
	up := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	up.SetOut(&out)
	up.SetErr(&out)
	up.SetArgs([]string{"--db-url", f.dbURL, "--migrations-dir", dir})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return f
}

// down runs `migrations down` against the fixture with args after the
// connection flags, keeping the two streams apart.
func (f documentRollback) down(args ...string) (stdout, stderr string, err error) {
	cmd := migratedown.NewMigrateDownCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(append([]string{"--db-url", f.dbURL, "--migrations-dir", f.dir}, args...))
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// decodeRunDocument reads stdout as exactly one run document, refusing a field
// the document type does not declare and anything after it.
func decodeRunDocument(c *qt.C, stdout string) migrator.RunResult {
	c.Helper()
	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.DisallowUnknownFields()
	var document migrator.RunResult
	c.Assert(decoder.Decode(&document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	var trailing json.RawMessage
	c.Assert(decoder.Decode(&trailing), qt.Equals, io.EOF, qt.Commentf("stdout:\n%s", stdout))
	return document
}

// The document of a rollback names what the run selected under its lock and
// what it took away, with the history it left. Everything written for a person
// goes to standard error.
func TestMigrateDownJSON_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newDocumentRollback(c)

	stdout, stderr, err := f.down("--target", "0", "--confirm", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	document := decodeRunDocument(c, stdout)
	c.Assert(document.Command, qt.Equals, "migrations down")
	c.Assert(document.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(document.Planned, qt.DeepEquals, []int64{2, 1})
	c.Assert(document.RolledBack, qt.DeepEquals, []int64{2, 1})
	c.Assert(document.Applied, qt.IsNil)
	c.Assert(document.Error, qt.IsNil)
	c.Assert(document.Status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(document.Status.AppliedMigrations, qt.HasLen, 0)
	c.Assert(stderr, qt.Contains, "Database is now at version: 0")
}

// A dry run selects the same migrations and takes nothing away.
func TestMigrateDownJSONDryRun_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newDocumentRollback(c)

	stdout, stderr, err := f.down("--target", "0", "--dry-run", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	document := decodeRunDocument(c, stdout)
	c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeDryRun)
	c.Assert(document.Planned, qt.DeepEquals, []int64{2, 1})
	c.Assert(document.RolledBack, qt.IsNil)
	c.Assert(document.Status.CurrentVersion, qt.Equals, int64(2))
}

// A target the database is already at selects nothing.
func TestMigrateDownJSONAtTheTarget_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newDocumentRollback(c)

	stdout, stderr, err := f.down("--target", "2", "--confirm", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	document := decodeRunDocument(c, stdout)
	c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeUpToDate)
	c.Assert(document.Planned, qt.IsNil)
	c.Assert(document.Status.CurrentVersion, qt.Equals, int64(2))
}

// A pre-flight hook's output is written for a person, so it goes to standard
// error and the document stays the only thing on standard output.
func TestMigrateDownJSONKeepsHookOutputOffTheDocument_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newDocumentRollback(c)

	stdout, stderr, err := f.down("--target", "1", "--confirm", "--json", "--pre-down-hook", "echo hook-said-this")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(decodeRunDocument(c, stdout).RolledBack, qt.DeepEquals, []int64{2})
	c.Assert(stderr, qt.Contains, "hook-said-this")
}

// A rollback whose second down body fails reports the first as taken away, the
// statement the database refused, and the dirty row it stopped at.
func TestMigrateDownJSON_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newDocumentRollback(c)
	c.Assert(os.WriteFile(filepath.Join(f.dir, "0000000001_users.down.sql"), []byte("DROP TABLE no_such_table;\n"), 0o600), qt.IsNil)

	stdout, stderr, err := f.down("--target", "0", "--confirm", "--json")

	c.Assert(err, qt.ErrorMatches, `(?s)error running down migrations: .*no such table: no_such_table.*`)
	document := decodeRunDocument(c, stdout)
	c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(document.Planned, qt.DeepEquals, []int64{2, 1})
	c.Assert(document.RolledBack, qt.DeepEquals, []int64{2})
	c.Assert(document.Error.Message, qt.Equals, err.Error())
	c.Assert(document.Failure.Version, qt.Equals, int64(1))
	c.Assert(document.Failure.Statement, qt.Equals, 1)
	c.Assert(document.Status.DirtyRevision.Version, qt.Equals, int64(1))
	c.Assert(document.Status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(stderr, qt.Contains, "error: "+err.Error())
}

// A document run that would stop at the confirmation prompt is refused before
// it connects, and the document says so; the database keeps both migrations.
func TestMigrateDownJSONWithoutConfirmation_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newDocumentRollback(c)

	stdout, _, err := f.down("--target", "0", "--json")

	c.Assert(err, qt.ErrorMatches, `--json needs --confirm or --dry-run: a run that prints a document for a program cannot stop to ask for confirmation`)
	document := decodeRunDocument(c, stdout)
	c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(document.Planned, qt.IsNil)
	c.Assert(document.Status, qt.IsNil)
	c.Assert(document.Error.Message, qt.Equals, err.Error())
	statusOut, _, statusErr := f.down("--target", "2", "--confirm", "--json")
	c.Assert(statusErr, qt.IsNil)
	c.Assert(decodeRunDocument(c, statusOut).Status.CurrentVersion, qt.Equals, int64(2))
}
