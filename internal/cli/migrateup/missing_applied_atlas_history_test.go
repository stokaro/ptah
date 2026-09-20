package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// An Atlas-format history is one the Atlas community binary also writes, and
// measured against the pinned v1.3.0 a directory missing an applied file
// applies at exit 0 with "No migration files to execute". Refusing here would
// stop a pipeline that binary runs, so the fact is reported by
// `migrations status` and this path proceeds (stokaro/ptah#3442).

func TestMigrateUpWithAMissingAppliedFile_AtlasHistoryProceeds(t *testing.T) {
	t.Run("the applied migration was deleted", func(t *testing.T) {
		c := qt.New(t)
		dir := writeAtlasFormatMigrations(c)
		dbPath := filepath.Join(t.TempDir(), "atlas.db")
		args := []string{
			"--db-url", "sqlite://" + filepath.ToSlash(dbPath), "--migrations-dir", dir,
			"--dir-format", "atlas", "--revision-format", "atlas",
		}
		out, err := runUp(args...)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(os.Remove(filepath.Join(dir, "20240102000000_orders.sql")), qt.IsNil)

		out, err = runUp(args...)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "already up to date")
	})
}

// writeAtlasFormatMigrations writes the single-file layout an Atlas directory
// uses, which is what selects the Atlas revision identity the test is about.
func writeAtlasFormatMigrations(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"20240101000000_users.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"20240102000000_orders.sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}
