package atlasmigrate

// White-box testing required: writeMigrationFiles' version-collision retry
// and its all-or-nothing rollback react to filesystem races that cannot be
// staged deterministically through GenerateDiff, which allocates versions
// under the directory lock immediately before writing.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestWriteMigrationFilesAt_CollisionRollsBackAttemptAndReportsVersion(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	// A concurrent writer claimed version 2 after this run allocated 1..2:
	// the second batch file collides, the attempt rolls back, and the caller
	// retries the whole batch above the colliding version.
	c.Assert(os.WriteFile(filepath.Join(dir, "2_add_email_concurrent_indexes.sql"), []byte("taken"), 0o600), qt.IsNil)
	contents := []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "SELECT 1;"},
		{NameSuffix: "_concurrent_indexes", SQL: "SELECT 2;", NoTransaction: true},
	}

	paths, collidedVersion, err := writeMigrationFilesAt(dir, "add_email", 1, contents)

	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.HasLen, 0)
	c.Assert(collidedVersion, qt.Equals, int64(2))
	// The colliding file kept its content and no version-1 leftover remains
	// from the aborted attempt.
	taken, readErr := os.ReadFile(filepath.Join(dir, "2_add_email_concurrent_indexes.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(taken), qt.Equals, "taken")
	_, leftoverErr := os.Stat(filepath.Join(dir, "1_add_email_transactional.sql"))
	c.Assert(leftoverErr, qt.ErrorIs, os.ErrNotExist)

	retryPaths, retryCollided, retryErr := writeMigrationFilesAt(dir, "add_email", collidedVersion+1, contents)

	c.Assert(retryErr, qt.IsNil)
	c.Assert(retryCollided, qt.Equals, int64(0))
	c.Assert(retryPaths, qt.DeepEquals, []string{
		filepath.Join(dir, "3_add_email_transactional.sql"),
		filepath.Join(dir, "4_add_email_concurrent_indexes.sql"),
	})
}

func TestWriteMigrationFiles_FailedWriteRollsBackEarlierFiles(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	// The second file's name exceeds the filesystem's name limit, so its
	// create fails with a non-collision error after the first file was
	// already written; the batch rolls back completely.
	oversizedSuffix := "_" + strings.Repeat("x", 512)

	paths, err := writeMigrationFiles(dir, "add_email", []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "SELECT 1;"},
		{NameSuffix: oversizedSuffix, SQL: "SELECT 2;", NoTransaction: true},
	})

	c.Assert(err, qt.ErrorMatches, `write migration file: .*`)
	c.Assert(paths, qt.HasLen, 0)
	_, leftoverErr := os.Stat(filepath.Join(dir, "1_add_email_transactional.sql"))
	c.Assert(leftoverErr, qt.ErrorIs, os.ErrNotExist)
}

func TestWriteMigrationFiles_EmptySQLIsRejected(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()

	paths, err := writeMigrationFiles(dir, "noop", []MigrationFileContent{{SQL: "   \n"}})

	c.Assert(err, qt.ErrorMatches, `migration SQL is empty`)
	c.Assert(paths, qt.HasLen, 0)
}
