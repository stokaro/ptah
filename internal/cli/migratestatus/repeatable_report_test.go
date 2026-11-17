package migratestatus_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
)

// writeRepeatableStatusDirectory writes one versioned migration and one
// repeatable migration whose body is body.
func writeRepeatableStatusDirectory(c *qt.C, body string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "R__widget_ids.sql"), []byte(body), 0o600), qt.IsNil)
	return dir
}

const statusRepeatableView = "DROP VIEW IF EXISTS widget_ids;\nCREATE VIEW widget_ids AS SELECT id FROM widgets;\n"

func TestMigrateStatusRepeatables_HappyPath(t *testing.T) {
	t.Run("an applied repeatable migration is up to date", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableStatusDirectory(c, statusRepeatableView)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		applyStatusMigrations(c, dir, dbPath)

		out, err := runStatus(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--exit-code")
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Repeatable Migrations: 1\n  widget_ids applied h1:")
		c.Assert(out, qt.Contains, "Status: ✅ Database is up to date")
	})

	t.Run("the record of a deleted file is listed as orphaned and is not a pending change", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableStatusDirectory(c, statusRepeatableView)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		applyStatusMigrations(c, dir, dbPath)
		c.Assert(os.Remove(filepath.Join(dir, "R__widget_ids.sql")), qt.IsNil)

		out, err := runStatus(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--exit-code")
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Repeatable Migrations: 1\n  widget_ids orphaned h1:")
		c.Assert(out, qt.Contains, " (no file in the directory)\n")
		c.Assert(out, qt.Contains, "Status: ✅ Database is up to date")
	})
}

func TestMigrateStatusRepeatables_FailurePath(t *testing.T) {
	t.Run("a changed repeatable migration is a pending change", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableStatusDirectory(c, statusRepeatableView)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		applyStatusMigrations(c, dir, dbPath)
		c.Assert(os.WriteFile(filepath.Join(dir, "R__widget_ids.sql"), []byte(statusRepeatableView+"-- changed\n"), 0o600), qt.IsNil)

		out, err := runStatus(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--exit-code")
		c.Assert(err, qt.ErrorMatches, "pending migrations available")
		c.Assert(out, qt.Contains, "  widget_ids changed h1:")
	})

	t.Run("a failed repeatable migration is reported with its error", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableStatusDirectory(c, "THIS IS NOT SQL;\n")
		dbPath := filepath.Join(c.TempDir(), "app.db")
		cmd := migrateup.NewMigrateUpCommand()
		var applyOut bytes.Buffer
		cmd.SetOut(&applyOut)
		cmd.SetErr(&applyOut)
		cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
		c.Assert(cmd.Execute(), qt.ErrorMatches, `(?s)error running migrations: repeatable migration widget_ids failed and took no effect: .*`)

		out, err := runStatus(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--exit-code")
		c.Assert(err, qt.ErrorMatches, "pending migrations available")
		c.Assert(out, qt.Contains, "Status: ❌ Repeatable migration did not finish")
		c.Assert(out, qt.Contains, "Dirty Repeatable: identity=widget_ids state=failed attempted=h1:")
		c.Assert(out, qt.Contains, "ptah migrations up --allow-dirty")
	})
}
