package migratestatus_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
)

// Applying an Atlas-format history that is missing a file proceeds, because the
// Atlas community binary proceeds and refusing would stop a pipeline it runs.
// The fact still has to be askable, and this verb is where it is asked
// (stokaro/ptah#3442).

func TestMigrateStatusMissingOnAnAtlasHistory_FailurePath(t *testing.T) {
	t.Run("the applied migration was deleted", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := appliedAtlasHistory(c)
		c.Assert(os.Remove(filepath.Join(dir, "20240102000000_orders.sql")), qt.IsNil)

		out, err := runStatus(c,
			"--db-url", "sqlite://"+filepath.ToSlash(dbPath), "--migrations-dir", dir,
			"--dir-format", "atlas", "--revision-format", "atlas", "--exit-code")

		c.Assert(err, qt.ErrorMatches, "applied migrations with no file detected")
		c.Assert(out, qt.Contains, "Status: ❌ Applied migration with no file")
		c.Assert(out, qt.Contains, "Missing Migration: version=20240102000000 description=orders")
	})
}

func TestMigrateStatusMissingOnAnAtlasHistory_HappyPath(t *testing.T) {
	t.Run("the directory is untouched", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := appliedAtlasHistory(c)

		out, err := runStatus(c,
			"--db-url", "sqlite://"+filepath.ToSlash(dbPath), "--migrations-dir", dir,
			"--dir-format", "atlas", "--revision-format", "atlas", "--exit-code")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Status: ✅ Database is up to date")
	})
}

// appliedAtlasHistory writes the single-file Atlas layout, applies it under the
// Atlas revision format, and returns the directory and the database.
func appliedAtlasHistory(c *qt.C) (dir, dbPath string) {
	c.Helper()
	dir = c.TempDir()
	files := map[string]string{
		"20240101000000_users.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"20240102000000_orders.sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(c.TempDir(), "atlas.db")
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--db-url", "sqlite://" + filepath.ToSlash(dbPath), "--migrations-dir", dir,
		"--dir-format", "atlas", "--revision-format", "atlas",
	})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return dir, dbPath
}
