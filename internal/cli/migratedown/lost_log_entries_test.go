package migratedown_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migratedown"
	"ptah.run/internal/cli/migrateup"
)

// A SQLite trigger stands in for whatever a server refuses a log row for, and the
// first file plants it, so the `up` that seeds the test is logged in full.
const refuseDownLogRows = `CREATE TABLE users (id INTEGER PRIMARY KEY);
CREATE TRIGGER refuse_down BEFORE INSERT ON schema_migrations_log
WHEN NEW.operation = 'down'
BEGIN
  SELECT RAISE(ABORT, 'this log row is refused');
END;
`

// seedForLostRollback applies a directory and returns its path and the database.
func seedForLostRollback(c *qt.C, up string) (dir, dbPath string) {
	c.Helper()
	dir = c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_users.up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_users.down.sql"), []byte("DROP TABLE users;\n"), 0o600), qt.IsNil)
	dbPath = filepath.Join(c.TempDir(), "lost.db")
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return dir, dbPath
}

// Without --json the notice is what a caller gets. The rollback ran and the
// exit stays clean: the loss is about the record of it.
func TestMigrateDownTellsAPersonWhatTheLogLost(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := seedForLostRollback(c, refuseDownLogRows)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Matches,
		`(?s).*Migration rollback completed successfully.*The migration log lost 2 of 2 entries: version 1 \(started, rolled_back\)\..*`)
	c.Assert(out, qt.Contains, "Could not append to the migration log")
}

// The notice follows a failed rollback too, ahead of the error the command
// returns, because the last thing it says about the log is what a person reads.
func TestMigrateDownTellsAPersonWhatAFailedRollbackLost(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := seedForLostRollback(c, refuseDownLogRows)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_users.down.sql"), []byte("NOT SQL;\n"), 0o600), qt.IsNil)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration log lost 2 of 2 entries: version 1 (started, failed).")
}

// The control: a rollback the log got in full says nothing about it.
func TestMigrateDownSaysNothingOfALogThatLostNothing(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := seedForLostRollback(c, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	cmd := migratedown.NewMigrateDownCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir, "--target", "0", "--confirm"})

	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Migration rollback completed successfully")
	c.Assert(out.String(), qt.Not(qt.Contains), "migration log")
}
