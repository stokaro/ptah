//go:build !windows

package atlasmigrate

// White-box testing required: replacing a staged file during the internal
// preparation callback cannot be exercised through the exported API.

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
)

func TestGenerateDiff_PreparePublicationRejectsSymlinkReplacement(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c.TB)
	previousSum, err := os.ReadFile(filepath.Join(opts.Dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	targetPath := filepath.Join(c.TempDir(), "replacement.sql")
	c.Assert(os.WriteFile(targetPath, []byte("SELECT 99;\n"), 0o600), qt.IsNil)
	opts.PreparePublication = func(stagedPaths []string) error {
		return errors.Join(
			os.Remove(stagedPaths[0]),
			os.Symlink(targetPath, stagedPaths[0]),
		)
	}

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorMatches, `staged migration file is not a regular file: .*`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	restoredSum, err := os.ReadFile(filepath.Join(opts.Dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	c.Assert(restoredSum, qt.DeepEquals, previousSum)
	sqlFiles, err := filepath.Glob(filepath.Join(opts.Dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	assertSQLiteCleanupObjectCount(c.TB, conn, 0)
	assertDiffDirectoryLockReleased(c.TB, opts.Dir)
}
