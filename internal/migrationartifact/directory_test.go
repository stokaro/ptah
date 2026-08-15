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
			Reference: "oci://example.invalid/acme/migrations",
			Directory: dir,
			Tags:      []string{"stable"},
			DirFormat: migrator.MigrationDirFormatPtah,
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

func TestPushDirectoryTo_FailurePath(t *testing.T) {

	t.Run("missing reference", func(t2 *testing.T) {
		c := qt.New(t2)
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

	t.Run("missing sum when verification is requested", func(t2 *testing.T) {
		c := qt.New(t2)
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
