package migratedown_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/migratedown"
	"ptah.run/internal/cli/migrateup"
)

// A runner Ptah does not model is refused before the database is opened,
// from the flag or from ptah.yaml, as `migrations up` refuses it: rolling a
// directory back as a tool runs it that Ptah cannot run is not something to
// fall back from.
func TestMigrateDownCommand_RunnerRefusesAnUnknownTool(t *testing.T) {
	tests := []struct {
		name   string
		config string
		flags  []string
	}{
		{name: "the flag", flags: []string{"--runner", "goose"}},
		{name: "ptah.yaml", config: "migration:\n  runner: goose\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte("CREATE TABLE t (id int);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE t;\n"), 0o600), qt.IsNil)
			config := filepath.Join(c.TempDir(), "ptah.yaml")
			c.Assert(os.WriteFile(config, []byte(test.config), 0o600), qt.IsNil)
			database := filepath.Join(c.TempDir(), "ptah.db")
			cmd := migratedown.NewMigrateDownCommand()
			cmd.SetArgs(append([]string{"--config", config, "--migrations-dir", dir,
				"--db-url", atlasurl.SQLiteURLFromPath(database), "--confirm"}, test.flags...))

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `invalid runner "goose": expected ptah or golang-migrate`)
			_, statErr := os.Stat(database)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// SQLite's migrator reads no file's own transactions, so under the
// golang-migrate runner a down file keeps the transaction around it, and one
// that controls transaction state is refused before it runs, as an up file
// is. The table the file drops is still there, and the revision is still
// applied.
func TestMigrateDownCommand_GolangMigrateRunnerOnSQLiteKeepsTheTransactionAroundTheFile(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte("CREATE TABLE t (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("BEGIN;\nDROP TABLE t;\nCOMMIT;\n"), 0o600),
		qt.IsNil)
	dbPath := filepath.Join(root, "app.db")
	up := migrateup.NewMigrateUpCommand()
	var upOut bytes.Buffer
	up.SetOut(&upOut)
	up.SetErr(&upOut)
	up.SetArgs([]string{"--db-url", atlasurl.SQLiteURLFromPath(dbPath), "--migrations-dir", dir, "--runner", "golang-migrate"})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", upOut.String()))

	_, err := runDown("--db-url", atlasurl.SQLiteURLFromPath(dbPath), "--migrations-dir", dir, "--runner", "golang-migrate",
		"--confirm")

	c.Assert(err, qt.ErrorMatches, `.*migration 1 cannot run tx-mode file down statement 1 because it controls transaction state.*`)
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, []string{"schema_migrations", "schema_migrations_log", "t"})
	c.Assert(downCurrentVersion(c, dbPath), qt.Equals, int64(1))
}
