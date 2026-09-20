package migratedown_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// The rollback path verifies applied checksums before it rolls anything back.
// The branch that returns because the database is already at or below the
// target never reaches that verification, so it is checked on its own
// (stokaro/ptah#3438).

func TestMigrateDownAtOrBelowTarget_FailurePath(t *testing.T) {
	t.Run("an applied migration was edited and re-hashed", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedWidgetsDir(c)
		dbPath := filepath.Join(c.TempDir(), "edited.db")
		applyWidgets(c, dir, dbPath)
		editAppliedUpFileAndReseal(c, dir)

		out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "1", "--confirm")

		c.Assert(err, qt.ErrorMatches, `migration 1 checksum mismatch: stored .*, current .*`)
		c.Assert(out, qt.Not(qt.Contains), "already at or below target")
	})
}

func TestMigrateDownAtOrBelowTarget_HappyPath(t *testing.T) {
	t.Run("the directory is untouched", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedWidgetsDir(c)
		dbPath := filepath.Join(c.TempDir(), "untouched.db")
		applyWidgets(c, dir, dbPath)

		out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "1", "--confirm")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "already at or below target")
	})
}

// editAppliedUpFileAndReseal rewrites the applied up body and re-hashes, so the
// sealed-directory gate passes and the recorded checksum is the only thing left
// disagreeing. Without the reseal this would measure the integrity gate instead.
func editAppliedUpFileAndReseal(c *qt.C, dir string) {
	c.Helper()
	path := filepath.Join(dir, "0000000001_init.up.sql")
	edited := "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL, note TEXT);\n"
	c.Assert(os.WriteFile(path, []byte(edited), 0o600), qt.IsNil)
	resealWidgets(c, dir)
}

// resealWidgets rewrites ptah.sum over whatever the directory holds now, so a
// test that changed it measures the recorded-revision rule rather than the
// sealed-directory gate in front of it.
func resealWidgets(c *qt.C, dir string) {
	c.Helper()
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatPtah)
	c.Assert(err, qt.IsNil)
}
