//go:build !windows

package atlasmigrate

// White-box testing required: the window this exercises opens after
// GenerateDiff has already locked, recovered and snapshotted the migration
// directory, so it cannot be reached from the exported API. PreparePublication
// is the same internal seam diff_prepare_nonwindows_internal_test.go uses, and
// recoverPendingPublication is only reachable from inside the package.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/migration/migrator"
)

// swappedDirectories names the two directories a swap leaves behind: the
// object that was validated, which every write must still reach, and the
// pathname that now resolves to the replacement, which must receive nothing.
type swappedDirectories struct {
	retained    string
	replacement string
}

// replaceMigrationDirectory moves dir aside, copies its contents into a
// replacement, and installs the replacement on dir's pathname through install.
// Callers pass os.Symlink or a rename to choose which shape of replacement the
// pathname resolves to afterwards; either way the pathname is what reads of
// swappedDirectories.replacement go through.
func replaceMigrationDirectory(
	c *qt.C,
	dir string,
	install func(replacement, pathname string) error,
) swappedDirectories {
	c.Helper()
	swap := swappedDirectories{retained: dir + ".retained", replacement: dir}
	staging := filepath.Join(filepath.Dir(dir), "replacement")
	c.Assert(os.CopyFS(staging, os.DirFS(dir)), qt.IsNil)
	c.Assert(os.Rename(dir, swap.retained), qt.IsNil)
	c.Assert(install(staging, dir), qt.IsNil)
	return swap
}

func migrationFileNames(c *qt.C, dir string) []string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		names = append(names, filepath.Base(match))
	}
	return names
}

func readSumFile(c *qt.C, dir string) string {
	c.Helper()
	contents, err := os.ReadFile(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	return string(contents)
}

// TestGenerateDiff_PublishesIntoTheValidatedDirectory pins that the migration
// file and atlas.sum land in the directory this run validated even after the
// directory's pathname is made to resolve somewhere else, and that the
// replacement receives nothing (stokaro/ptah#895).
func TestGenerateDiff_PublishesIntoTheValidatedDirectory(t *testing.T) {
	tests := []struct {
		name    string
		install func(replacement, pathname string) error
	}{
		{
			name:    "pathname becomes a symlink to the replacement",
			install: os.Symlink,
		},
		{
			name: "pathname becomes the replacement directory",
			install: func(replacement, pathname string) error {
				return os.Rename(replacement, pathname)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, opts := prepareGenerateDiffFaultTest(c)
			previousSum := readSumFile(c, opts.Dir)
			var swap swappedDirectories
			opts.PreparePublication = func([]string) error {
				swap = replaceMigrationDirectory(c, opts.Dir, test.install)
				return nil
			}

			result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
				readDevSchema:        readScopedDevSchema,
				withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(result.MigrationPaths, qt.HasLen, 1)
			// The validated directory received the migration and the refreshed
			// checksum, and still verifies against them.
			c.Assert(migrationFileNames(c, swap.retained), qt.HasLen, 2)
			c.Assert(readSumFile(c, swap.retained), qt.Not(qt.Equals), previousSum)
			verification, verifyErr := migratesum.VerifyWithFormat(
				os.DirFS(swap.retained),
				migrator.MigrationDirFormatAtlas,
			)
			c.Assert(verifyErr, qt.IsNil)
			c.Assert(verification.OK(), qt.IsTrue)
			// The directory that took over the pathname received nothing.
			c.Assert(migrationFileNames(c, swap.replacement), qt.DeepEquals, []string{"1_init.sql"})
			c.Assert(readSumFile(c, swap.replacement), qt.Equals, previousSum)
			assertSQLiteCleanupObjectCount(c, conn, 0)
		})
	}
}

// TestGenerateDiff_PublishesIntoUnswappedDirectory is the control for the swap
// fixture above: with no swap the same reads observe the migration and the
// refreshed checksum in the same directory, which is what makes "the
// replacement stayed empty" evidence rather than an artifact of looking in the
// wrong place.
func TestGenerateDiff_PublishesIntoUnswappedDirectory(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	previousSum := readSumFile(c, opts.Dir)

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.MigrationPaths, qt.HasLen, 1)
	c.Assert(migrationFileNames(c, opts.Dir), qt.HasLen, 2)
	c.Assert(readSumFile(c, opts.Dir), qt.Not(qt.Equals), previousSum)
}

// TestRecoverPendingPublication_RollsBackInTheRetainedDirectory pins that
// recovery undoes the interrupted batch in the directory the handle was opened
// on, and leaves an identically named file in the replacement untouched.
func TestRecoverPendingPublication_RollsBackInTheRetainedDirectory(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	pub := openTestPublicationDir(c, dir)
	batch, err := stageMigrationBatchAt(
		pub,
		"interrupted",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(c, pub, batch, []MigrationFileContent{{SQL: "SELECT 1;"}})
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	migrationName := filepath.Base(batch.paths(pub)[0])
	swap := replaceMigrationDirectory(c, dir, os.Symlink)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)

	_, err = os.Stat(filepath.Join(swap.retained, migrationName))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	replaced, err := os.ReadFile(filepath.Join(swap.replacement, migrationName))
	c.Assert(err, qt.IsNil)
	c.Assert(string(replaced), qt.Equals, "SELECT 1;")
}

// TestRecoverPendingPublication_CleansUpInTheRetainedDirectory pins that orphan
// staging cleanup removes the temporary file inside the directory the handle
// was opened on and cannot reach an identically named file in a directory that
// took over the pathname.
func TestRecoverPendingPublication_CleansUpInTheRetainedDirectory(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	pub := openTestPublicationDir(c, dir)
	stagedName, err := stageMigrationFile(pub, "SELECT 1;")
	c.Assert(err, qt.IsNil)
	swap := replaceMigrationDirectory(c, dir, os.Symlink)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)

	_, err = os.Stat(filepath.Join(swap.retained, stagedName))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	orphan, err := os.ReadFile(filepath.Join(swap.replacement, stagedName))
	c.Assert(err, qt.IsNil)
	c.Assert(string(orphan), qt.Equals, "SELECT 1;")
}
