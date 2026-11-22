package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// The drift check replays the applied history in the run's --tx-mode. SQLite
// refuses VACUUM inside a transaction, so a migration that holds it beside
// another statement applies under none, or under its own no_transaction
// directive, and its replay fails in a transaction.

// vacuumHistory is a SQLite target whose migration 1, a table and a VACUUM
// with header in front of them, was applied with applyArgs, and whose
// migration 2 is pending. It returns the directory, the target's path and an
// empty shadow database's URL.
func vacuumHistory(c *qt.C, header string, applyArgs ...string) (migrationsDir, dbPath, shadowURL string) {
	c.Helper()
	migrationsDir = c.TempDir()
	write := func(name, content string) {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(content), 0o600), qt.IsNil)
	}
	write("0000000001_items.up.sql", header+"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\nVACUUM;\n")
	write("0000000001_items.down.sql", "DROP TABLE items;\n")
	dbPath = filepath.Join(c.TempDir(), "target.db")
	out, code := runUpExit(c, append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir}, applyArgs...)...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
	write("0000000002_note.up.sql", "ALTER TABLE items ADD COLUMN note TEXT;\n")
	write("0000000002_note.down.sql", "ALTER TABLE items DROP COLUMN note;\n")
	return migrationsDir, dbPath, "sqlite://" + filepath.Join(c.TempDir(), "shadow.db")
}

func TestMigrateUpCheckDriftHappyPathReplaysInTheRunsTxMode(t *testing.T) {
	for _, test := range []struct {
		name string
		// header leads migration 1, which applyArgs applied.
		header    string
		applyArgs []string
		checkArgs []string
	}{
		{
			name:      "a history applied under none, checked under none",
			applyArgs: []string{"--tx-mode", "none"},
			checkArgs: []string{"--tx-mode", "none"},
		},
		{
			// A run under all cannot apply migration 1, so a run in another
			// mode applied it, and its replay honors its directive.
			name:      "a no_transaction file, checked under all",
			header:    "-- +ptah no_transaction\n",
			checkArgs: []string{"--tx-mode", "all"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath, shadowURL := vacuumHistory(c, test.header, test.applyArgs...)

			out, code := runUpExit(c, append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir,
				"--check-drift", "--shadow-db", shadowURL}, test.checkArgs...)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Drift check: the database matches the schema its 1 applied migration produces")
			c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
			c.Assert(tableCount(c, shadowURL), qt.Equals, 0)
		})
	}
}

// The mode is the run's, not one the replay picks for itself: a history
// applied under none, checked under the default file, replays in a
// transaction and fails, and nothing pending runs.
func TestMigrateUpCheckDriftFailurePathReplaysInTheRunsTxMode(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath, shadowURL := vacuumHistory(c, "", "--tx-mode", "none")

	out, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--check-drift", "--shadow-db", shadowURL)

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "replay the applied migrations: 0000000001_items.up.sql cannot run inside a transaction: "+
		"line 2 VACUUM is refused inside a transaction block")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(1))
	c.Assert(tableCount(c, shadowURL), qt.Equals, 0)
}
