package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationfailure"
	"ptah.run/migration/migrator"
)

// serverDetailsDocument is the run document of a run asked for
// --server-details.
type serverDetailsDocument struct {
	migrator.RunResult
	ServerDetails *migrationfailure.Details `json:"server_details"`
}

// writeFailingUpMigrations writes a directory whose second migration inserts
// into a table no migration creates. SQLite refuses it, and its driver reports
// no server details, so what the details carry is the file alone.
func writeFailingUpMigrations(t *testing.T) string {
	c := qt.New(t)
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":    "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql":  "DROP TABLE users;\n",
		"0000000002_orders.up.sql":   "INSERT INTO missing (id) VALUES (1);\n",
		"0000000002_orders.down.sql": "DELETE FROM missing;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// The details follow the error line, and the document carries them beside
// the failure record, which they leave as it is.
func TestMigrateUpServerDetails_HappyPath(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(t.TempDir(), "app.db"),
		"--migrations-dir", writeFailingUpMigrations(t), "--json", "--server-details")

	c.Assert(err, qt.IsNotNil)
	document := decodeOnly[serverDetailsDocument](c, stdout)
	c.Assert(document.Failure, qt.DeepEquals, &migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1})
	c.Assert(document.ServerDetails, qt.DeepEquals, &migrationfailure.Details{File: "0000000002_orders.up.sql"})
	c.Assert(stderr, qt.Contains, "\nserver details (may contain application data):\n"+
		"  failed at: statement 1 of 1 of migration 2 (0000000002_orders.up.sql)\n"+
		"  server: no details reported\n")
}

// A run that applied everything has nothing to explain, with or without the
// flag.
func TestMigrateUpServerDetailsOfASuccessfulRun(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(t.TempDir(), "app.db"),
		"--migrations-dir", writeUpMigrations(t), "--json", "--server-details")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(decodeOnly[migrator.RunResult](c, stdout).Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(stderr, qt.Not(qt.Contains), "server details")
}

// Without the flag the document is the one its consumers decode, with no
// field they do not declare, and the error line stands alone.
func TestMigrateUpWithoutServerDetails(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(t.TempDir(), "app.db"),
		"--migrations-dir", writeFailingUpMigrations(t), "--json")

	c.Assert(err, qt.IsNotNil)
	document := decodeOnly[migrator.RunResult](c, stdout)
	c.Assert(document.Failure, qt.DeepEquals, &migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1})
	c.Assert(stderr, qt.Not(qt.Contains), "server details")
}
