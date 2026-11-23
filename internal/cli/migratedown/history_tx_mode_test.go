package migratedown_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/migratedown"
	"ptah.run/migration/migrator"
)

// --shadow-db rehearses a rollback on the history the target applied, and
// replays each applied file as it ran, with no mode stated. SQLite refuses
// VACUUM inside a transaction, so migration 1 applies under none, and its
// replay runs without a transaction.

// vacuumRollbackTarget applies a history whose migration 1 holds a VACUUM to a
// new SQLite target under none, and returns the directory, the target's URL
// and the migrator that reads its status.
func vacuumRollbackTarget(c *qt.C) (dir, dbURL string, mig *migrator.Migrator) {
	c.Helper()
	dir = c.TempDir()
	for name, body := range map[string]string{
		"000001_items.up.sql":   "CREATE TABLE items (id INTEGER PRIMARY KEY);\nVACUUM;\n",
		"000001_items.down.sql": "DROP TABLE items;\n",
		"000002_note.up.sql":    "ALTER TABLE items ADD COLUMN note TEXT;\n",
		"000002_note.down.sql":  "ALTER TABLE items DROP COLUMN note;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	dbURL = atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "ptah.db"))
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err = migrator.NewFSMigrator(conn, os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.WithTransactionMode(migrator.MigrationTxModeNone).MigrateUp(c.Context()), qt.IsNil)
	return dir, dbURL, mig
}

// rollBackVacuumTarget rolls the target back to version 1 with a rehearsal on
// a new shadow database, args after the rest.
func rollBackVacuumTarget(c *qt.C, dir, dbURL string, args ...string) (string, error) {
	c.Helper()
	cmd := migratedown.NewMigrateDownCommand()
	resetMigrateDownCommandForTest(c, cmd)
	cmd.SetArgs(append([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--target", "1",
		"--shadow-db", atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "shadow.db")),
		"--confirm",
	}, args...))
	return captureStdIO(c, "", cmd.Execute)
}

// A project's migration.tx_mode, which `migrations up` reads, changes
// nothing, and neither does PTAH_TX_MODE.
func TestMigrateDownShadowDB_HappyPathReplaysEachFileAsItRan(t *testing.T) {
	for _, test := range []struct {
		name    string
		project string
		env     string
	}{
		{name: "no mode", project: "migration: {}\n"},
		{name: "the project's migration.tx_mode", project: "migration:\n  tx_mode: file\n"},
		{name: "PTAH_TX_MODE", project: "migration: {}\n", env: "all"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_TX_MODE", test.env)
			dir, dbURL, mig := vacuumRollbackTarget(c)
			config := filepath.Join(c.TempDir(), "ptah.yaml")
			c.Assert(os.WriteFile(config, []byte(test.project), 0o600), qt.IsNil)

			out, err := rollBackVacuumTarget(c, dir, dbURL, "--config", config)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Rollback plan verified on shadow database")
			status, err := mig.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(status.CurrentVersion, qt.Equals, int64(1))
		})
	}
}

// The rehearsal takes no transaction mode, so --tx-mode is refused before
// the rollback starts.
func TestMigrateDownShadowDB_FailurePathTakesNoTxMode(t *testing.T) {
	c := qt.New(t)
	dir, dbURL, mig := vacuumRollbackTarget(c)

	_, err := rollBackVacuumTarget(c, dir, dbURL, "--tx-mode", "none")

	c.Assert(err, qt.ErrorMatches, `unknown flag: --tx-mode`)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
}
