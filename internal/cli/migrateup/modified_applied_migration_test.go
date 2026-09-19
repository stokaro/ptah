package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// An edited applied migration is neither a pending change nor a dirty
// revision, so the "nothing to do" shortcut in front of MigrateUp is the one
// path where nothing asks whether the recorded checksums still hold
// (stokaro/ptah#3438). These two tests are a pair: the refusal proves the
// question is asked, and the control proves the shortcut still answers when
// there is nothing to refuse.

func TestMigrateUpWithNothingPending_FailurePath(t *testing.T) {
	t.Run("an applied migration was edited", func(t *testing.T) {
		c := qt.New(t)
		dir := writeUpMigrations(t)
		dbPath := filepath.Join(t.TempDir(), "edited.db")
		args := []string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir}
		out, err := runUp(args...)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		editAppliedUpFile(c, dir)

		out, err = runUp(args...)

		c.Assert(err, qt.ErrorMatches, `migration 2 checksum mismatch: stored .*, current .*`)
		c.Assert(out, qt.Not(qt.Contains), "already up to date")
	})
}

func TestMigrateUpWithNothingPending_HappyPath(t *testing.T) {
	t.Run("the directory is untouched", func(t *testing.T) {
		c := qt.New(t)
		dir := writeUpMigrations(t)
		dbPath := filepath.Join(t.TempDir(), "untouched.db")
		args := []string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir}
		out, err := runUp(args...)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

		out, err = runUp(args...)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "already up to date")
	})
}

// editAppliedUpFile rewrites the up body of the second migration, which the
// database has already applied. The directory carries no ptah.sum, so this is
// the recorded-checksum question on its own rather than the sealed-directory
// one.
func editAppliedUpFile(c *qt.C, dir string) {
	c.Helper()
	path := filepath.Join(dir, "0000000002_orders.up.sql")
	edited := "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER);\n"
	c.Assert(os.WriteFile(path, []byte(edited), 0o600), qt.IsNil)
}
