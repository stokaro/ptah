package migratedown_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
)

// An Atlas history answers a deleted applied file differently from a native
// one. Atlas records a revision by its own identity rather than by the file
// that produced it, so a rollback that finds the database already at or below
// its target says so and stops, where the native directory refuses because
// there is no down body it could have run.
//
// The tolerance is one clause on the down path, and the up path's twin is
// TestMigrateUpWithAMissingAppliedFile_AtlasHistoryProceeds. Without this test
// the whole offline contour passes with the clause deleted: `go test ./...`
// exits 0 while the rollback refuses an Atlas history it is meant to accept.

func TestMigrateDownWithAMissingAppliedFile_AtlasHistoryProceeds(t *testing.T) {
	t.Run("the applied migration was deleted", func(t *testing.T) {
		c := qt.New(t)
		dir := writeAtlasDownMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "atlas-down.db")
		applyAtlasDownMigrations(c, dir, dbPath)
		c.Assert(os.Remove(filepath.Join(dir, "20240102000000_orders.sql")), qt.IsNil)

		out, err := runDown(
			"--db-url", "sqlite://"+filepath.ToSlash(dbPath), "--migrations-dir", dir,
			"--dir-format", "atlas", "--revision-format", "atlas",
			"--target", "20240102000000", "--confirm",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "already at or below target")
	})
}

// writeAtlasDownMigrations writes the single-file layout an Atlas directory
// uses, which is what selects the Atlas revision identity this test is about.
func writeAtlasDownMigrations(c *qt.C) string {
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

// applyAtlasDownMigrations runs the migrations through the up command, so the
// recorded history this test rolls back is one the product wrote rather than a
// fixture's idea of one.
func applyAtlasDownMigrations(c *qt.C, dir, dbPath string) {
	c.Helper()
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--db-url", "sqlite://" + filepath.ToSlash(dbPath), "--migrations-dir", dir,
		"--dir-format", "atlas", "--revision-format", "atlas",
	})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
}
