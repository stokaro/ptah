//go:build !windows

package atlasmigrate

// White-box testing required: recovery of an interrupted publication is reached
// through no exported callback, so the interrupted state has to be built with
// the internal staging and journal primitives and the bound writer handed to
// recovery directly.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// Recovery is the half of stokaro/ptah#895 that no callback can reach: it runs
// before the first callback of a diff run, and on the native generate surface
// it runs as the whole operation. The tests around it therefore build the
// interrupted state, replace the directory underneath the bound writer, and
// then call recovery directly.
//
// Both rows assert the same two things, because either one alone can be
// satisfied by the wrong code: recovery repaired the directory it opened, AND
// it left the replacement completely untouched. A recovery that follows the
// pathname satisfies neither -- it withdraws the decoy's copy and leaves the
// retained directory holding the file the interrupted batch published.
//
// Unix only, for the same reason as
// TestGenerateDiff_RenamedDirectoryCannotRedirectPublication: the fixture
// renames a directory the writer holds open, which Win32 refuses outright.

// entryExists reports whether path names an entry at all. It does not follow a
// final symlink, so an assertion about a name inside the retained directory
// answers for that directory rather than for whatever a link would reach.
func entryExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// replaceBoundDirectory moves dir aside to retained and leaves a symlink to
// decoy in its place, the way a hostile writer would between the moment the
// interrupted run was cut short and the moment the next lock holder recovers.
func replaceBoundDirectory(c *qt.C, dir, retained, decoy string) {
	c.Helper()
	// The decoy is a live copy, staged temporary files included, so a recovery
	// that follows the pathname finds a complete interrupted batch there and
	// withdraws that one instead of erroring out on a missing file.
	c.Assert(os.CopyFS(decoy, os.DirFS(dir)), qt.IsNil)
	c.Assert(os.Rename(dir, retained), qt.IsNil)
	c.Assert(os.Symlink(decoy, dir), qt.IsNil)
}

func TestRecoverPendingPublication_RollsBackInTheRetainedDirectory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	retained := filepath.Join(root, "migrations.moved")
	decoy := filepath.Join(root, "decoy")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"), []byte("SELECT 1;"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	writer := openTestWriter(c, dir)
	batch, err := stageMigrationBatchAt(
		writer,
		"interrupted",
		2,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(c, writer, batch, []MigrationFileContent{{SQL: "SELECT 2;"}})
	published, err := publishMigrationBatch(writer, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	publishedName := filepath.Base(batch.paths[0])
	stagedName := batch.stagedNames[0]
	replaceBoundDirectory(c, dir, retained, decoy)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)

	// The retained directory is the one that was rolled back.
	c.Assert(entryExists(filepath.Join(retained, publishedName)), qt.IsFalse)
	c.Assert(entryExists(filepath.Join(retained, stagedName)), qt.IsFalse)
	c.Assert(entryExists(testJournalPath(writer)), qt.IsFalse)
	// The replacement kept every byte it was given.
	c.Assert(entryExists(filepath.Join(decoy, publishedName)), qt.IsTrue)
	c.Assert(entryExists(filepath.Join(decoy, stagedName)), qt.IsTrue)
}

func TestRecoverPendingPublication_CleansUpInTheRetainedDirectory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	retained := filepath.Join(root, "migrations.moved")
	decoy := filepath.Join(root, "decoy")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	writer := openTestWriter(c, dir)
	// An orphan staging file with no journal beside it: the shape an aborted run
	// leaves behind, which recovery removes as cleanup rather than as rollback.
	stagedName, err := stageRootedFile(
		writer.dir,
		stagedMigrationPattern,
		[]byte("SELECT 1;"),
		publishedFileMode,
	)
	c.Assert(err, qt.IsNil)
	replaceBoundDirectory(c, dir, retained, decoy)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)

	c.Assert(entryExists(filepath.Join(retained, stagedName)), qt.IsFalse)
	c.Assert(entryExists(filepath.Join(decoy, stagedName)), qt.IsTrue)
}
