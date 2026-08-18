package migrationartifact_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/content/memory"

	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestPushDirectoryTo_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	err := os.WriteFile(
		filepath.Join(dir, "0000000001_create_users.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER);\n"),
		0o600,
	)
	c.Assert(err, qt.IsNil)
	store := memory.New()

	result, err := migrationartifact.PushDirectoryTo(
		context.Background(),
		store,
		migrationartifact.DirectoryPushOptions{
			Reference:        "oci://example.invalid/acme/migrations",
			Directory:        dir,
			Tags:             []string{"stable"},
			DirFormat:        migrator.MigrationDirFormatPtah,
			Latest:           true,
			GeneratedVersion: true,
			Now: func() time.Time {
				return time.Date(2026, time.July, 27, 12, 35, 19, 0, time.UTC)
			},
		},
	)
	c.Assert(err, qt.IsNil)

	c.Assert(result.Version, qt.Matches, `v20260727123519-[A-Z2-7]+`)
	c.Assert(result.Tags, qt.DeepEquals, []string{result.Version, "stable", "latest"})
	pulled, err := migrationartifact.PullFrom(context.Background(), store, "latest")
	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(pulled.FileSystem, "0000000001_create_users.up.sql"), qt.IsNil)
}

// TestPushDirectoryTo_WritesOnlyTheTagItWasGiven pins the default. A publish
// and an alias move are two operations, and a publish that also moved latest
// was promoting whatever had just been built without being asked. The version
// tag is opt-in for the same reason: one that exists only because a default
// created it is a tag nothing refers to.
func TestPushDirectoryTo_WritesOnlyTheTagItWasGiven(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_create_users.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER);\n"),
		0o600,
	), qt.IsNil)
	store := memory.New()

	result, err := migrationartifact.PushDirectoryTo(
		context.Background(),
		store,
		migrationartifact.DirectoryPushOptions{
			Reference: "oci://example.invalid/acme/migrations:release",
			Directory: dir,
			DirFormat: migrator.MigrationDirFormatPtah,
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Version, qt.Equals, "",
		qt.Commentf("no version was asked for, so none was invented"))
	c.Assert(result.Tags, qt.DeepEquals, []string{"release"})
	_, err = migrationartifact.PullFrom(context.Background(), store, "latest")
	c.Assert(err, qt.IsNotNil, qt.Commentf("latest must not have been moved onto this push"))
}

func TestPushDirectoryTo_FailurePath(t *testing.T) {
	t.Run("missing reference", func(t *testing.T) {
		c := qt.New(t)
		_, err := migrationartifact.PushDirectoryTo(
			context.Background(),
			memory.New(),
			migrationartifact.DirectoryPushOptions{Directory: t.TempDir()},
		)
		c.Assert(err, qt.ErrorMatches, "OCI reference is required")
	})

	t.Run("missing directory", func(t *testing.T) {
		c := qt.New(t)
		_, err := migrationartifact.PushDirectoryTo(
			context.Background(),
			memory.New(),
			migrationartifact.DirectoryPushOptions{Reference: "oci://example.invalid/acme/migrations"},
		)
		c.Assert(err, qt.ErrorMatches, "migrations directory is required")
	})

	t.Run("missing sum when verification is requested", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		err := os.WriteFile(
			filepath.Join(dir, "0000000001_create_users.up.sql"),
			[]byte("CREATE TABLE users (id INTEGER);\n"),
			0o600,
		)
		c.Assert(err, qt.IsNil)
		_, err = migrationartifact.PushDirectoryTo(
			context.Background(),
			memory.New(),
			migrationartifact.DirectoryPushOptions{
				Reference: "oci://example.invalid/acme/migrations",
				Directory: dir,
				DirFormat: migrator.MigrationDirFormatPtah,
				VerifySum: true,
			},
		)
		c.Assert(err, qt.ErrorMatches, "verify migrations directory: .*ptah.sum.*")
	})
}

func TestPullDirectory_RejectsExistingOutputBeforeRegistryAccess(t *testing.T) {
	c := qt.New(t)
	output := t.TempDir()

	_, err := migrationartifact.PullDirectory(
		context.Background(),
		migrationartifact.DirectoryPullOptions{
			Reference: "oci://unreachable.invalid/acme/migrations:latest",
			Output:    output,
		},
	)

	c.Assert(err, qt.ErrorMatches, "migration artifact output path already exists: .*")
}
