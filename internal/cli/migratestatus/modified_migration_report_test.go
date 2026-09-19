package migratestatus_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migratestatus"
	"ptah.run/internal/cli/migrateup"
)

// A migration the database applied and the file no longer accounts for is the
// one state the human report had no line for: it is not pending and not dirty,
// so the report reached its "up to date" branch over an edited history
// (stokaro/ptah#3438).

func TestMigrateStatusReport_FailurePath(t *testing.T) {
	t.Run("an applied migration was edited", func(t *testing.T) {
		c := qt.New(t)
		dir := writeStatusMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "edited.db")
		applyStatusMigrations(c, dir, dbPath)
		editAppliedUpFile(c, dir)

		out, err := runStatus(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--exit-code")

		c.Assert(err, qt.ErrorMatches, "modified migrations detected")
		c.Assert(out, qt.Contains, "Status: ❌ Modified migration detected")
		c.Assert(out, qt.Contains, "Modified Migration: version=1 description=Init")
		c.Assert(out, qt.Not(qt.Contains), "Database is up to date")
	})
}

func TestMigrateStatusReport_HappyPath(t *testing.T) {
	t.Run("the directory is untouched", func(t *testing.T) {
		c := qt.New(t)
		dir := writeStatusMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "untouched.db")
		applyStatusMigrations(c, dir, dbPath)

		out, err := runStatus(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--exit-code")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Status: ✅ Database is up to date")
	})
}

func writeStatusMigrations(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"0000000001_init.down.sql": "DROP TABLE widgets;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

func applyStatusMigrations(c *qt.C, dir, dbPath string) {
	c.Helper()
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
}

// editAppliedUpFile rewrites the up body the database already applied. The
// directory carries no ptah.sum, so the recorded checksum is the only thing
// left disagreeing.
func editAppliedUpFile(c *qt.C, dir string) {
	c.Helper()
	path := filepath.Join(dir, "0000000001_init.up.sql")
	edited := "CREATE TABLE widgets (id INTEGER PRIMARY KEY, note TEXT);\n"
	c.Assert(os.WriteFile(path, []byte(edited), 0o600), qt.IsNil)
}

func runStatus(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := migratestatus.NewMigrateStatusCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}
