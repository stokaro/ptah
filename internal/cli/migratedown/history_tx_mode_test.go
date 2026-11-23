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
// replays that history in --tx-mode, the mode `migrations up --tx-mode`
// applied it in. SQLite refuses VACUUM inside a transaction, so migration 1
// applies under none and its replay fails in a transaction.

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

// The flag wins over the project's migration.tx_mode, and the project's value
// is the default without it.
func TestMigrateDownShadowDB_HappyPathReplaysInTheTxMode(t *testing.T) {
	for _, test := range []struct {
		name    string
		project string
		args    []string
	}{
		{name: "the flag", project: "migration:\n  tx_mode: file\n", args: []string{"--tx-mode", "none"}},
		{name: "the project's migration.tx_mode", project: "migration:\n  tx_mode: none\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbURL, mig := vacuumRollbackTarget(c)
			config := filepath.Join(c.TempDir(), "ptah.yaml")
			c.Assert(os.WriteFile(config, []byte(test.project), 0o600), qt.IsNil)

			out, err := rollBackVacuumTarget(c, dir, dbURL, append([]string{"--config", config}, test.args...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Rollback plan verified on shadow database")
			status, err := mig.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(status.CurrentVersion, qt.Equals, int64(1))
		})
	}
}

// Without --tx-mode the rehearsal replays the history as `migrations up`
// applies by default, a file to a transaction, and the VACUUM is refused
// before the target is touched. A value that names no mode is refused before
// the rollback starts.
func TestMigrateDownShadowDB_FailurePathReplaysInTheTxMode(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "no mode",
			wantErr: `(?s)rollback verification failed: replay migrations: 000001_items\.up\.sql cannot run inside a transaction: line 2 VACUUM is refused inside a transaction block.*`,
		},
		{
			name:    "a mode that does not exist",
			args:    []string{"--tx-mode", "each"},
			wantErr: `invalid tx-mode "each": expected file, all, or none`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbURL, mig := vacuumRollbackTarget(c)

			_, err := rollBackVacuumTarget(c, dir, dbURL, test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			status, err := mig.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(status.CurrentVersion, qt.Equals, int64(2))
		})
	}
}
