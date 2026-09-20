package migratedown_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// Rolling back a revision whose file is gone is the one thing a rollback
// cannot do: there is no down body to run. The branch that returns because the
// database is already at or below the target refuses for the same reason as
// the branch that would roll back (stokaro/ptah#3441).

func TestMigrateDownWithAMissingAppliedFile_FailurePath(t *testing.T) {
	t.Run("an applied migration was deleted", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedWidgetsDir(c)
		dbPath := filepath.Join(c.TempDir(), "deleted.db")
		applyWidgets(c, dir, dbPath)
		deleteAppliedWidget(c, dir)

		out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "1", "--confirm")

		c.Assert(err, qt.ErrorMatches,
			`migration 1 is recorded as applied and this directory has no file for it: "Init".*`)
		c.Assert(out, qt.Not(qt.Contains), "already at or below target")
	})
}

// deleteAppliedWidget removes the applied migration and re-seals the directory,
// so the sealed-directory gate passes and the recorded revision is the only
// thing left with nothing to point at.
func deleteAppliedWidget(c *qt.C, dir string) {
	c.Helper()
	for _, name := range []string{"0000000001_init.up.sql", "0000000001_init.down.sql"} {
		c.Assert(os.Remove(filepath.Join(dir, name)), qt.IsNil)
	}
	resealWidgets(c, dir)
}
