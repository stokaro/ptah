package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A revision the database recorded and the directory holds no file for is a
// third way the two disagree, beside a pending migration and an edited one, and
// the one nothing reported: the checksum rule is a loop over the files, so a row
// without one is never visited (stokaro/ptah#3441).

func TestMigrateUpWithAMissingAppliedFile_FailurePath(t *testing.T) {
	t.Run("an applied migration was deleted", func(t *testing.T) {
		c := qt.New(t)
		dir := writeUpMigrations(t)
		dbPath := filepath.Join(t.TempDir(), "deleted.db")
		args := []string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir}
		out, err := runUp(args...)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		deleteAppliedMigration(c, dir)

		out, err = runUp(args...)

		c.Assert(err, qt.ErrorMatches,
			`migration 2 is recorded as applied and this directory has no file for it: "Orders".*`)
		c.Assert(out, qt.Not(qt.Contains), "already up to date")
	})
}

func TestMigrateUpWithAMissingAppliedFile_HappyPath(t *testing.T) {
	// The control the refusal needs, and the one that keeps it from becoming a
	// blanket refusal: a checkpoint covers the migrations below it, so a
	// database that bootstrapped from one holds no row for them and the
	// directory is free of their files by design.
	t.Run("a database bootstrapped from a checkpoint", func(t *testing.T) {
		c := qt.New(t)
		dir := writeCheckpointOnlyMigrations(c)
		dbPath := filepath.Join(t.TempDir(), "bootstrapped.db")
		args := []string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir}
		out, err := runUp(args...)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

		out, err = runUp(args...)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "already up to date")
	})
}

// deleteAppliedMigration removes both files of the second migration, which the
// database has already applied.
func deleteAppliedMigration(c *qt.C, dir string) {
	c.Helper()
	for _, name := range []string{"0000000002_orders.up.sql", "0000000002_orders.down.sql"} {
		c.Assert(os.Remove(filepath.Join(dir, name)), qt.IsNil)
	}
}

// writeCheckpointOnlyMigrations writes a directory whose whole history is one
// checkpoint at version 3, which is what a squash leaves behind: the covered
// versions have no files, and a fresh database records the checkpoint alone.
func writeCheckpointOnlyMigrations(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000003_squash.checkpoint.up.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
			"CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"0000000003_squash.checkpoint.down.sql": "DROP TABLE orders;\nDROP TABLE users;\n",
		"0000000004_payments.up.sql":            "CREATE TABLE payments (id INTEGER PRIMARY KEY);\n",
		"0000000004_payments.down.sql":          "DROP TABLE payments;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}
