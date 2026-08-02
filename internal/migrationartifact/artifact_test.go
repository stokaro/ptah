package migrationartifact_test

import (
	"context"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content/memory"

	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestVersionTag(t *testing.T) {
	c := qt.New(t)
	timestamp := time.Date(2026, time.July, 27, 14, 35, 19, 999, time.FixedZone("test", 2*60*60))

	got := migrationartifact.VersionTag(timestamp)
	second := migrationartifact.VersionTag(timestamp)

	c.Assert(got, qt.Matches, `v20260727123519-[A-Z2-7]+`)
	c.Assert(second, qt.Matches, `v20260727123519-[A-Z2-7]+`)
	c.Assert(second, qt.Not(qt.Equals), got)
}

func TestCapture_IncludesOnlyMigrationInputs(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"0000000001_create_users.up.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"README.md":                      {Data: []byte("not part of the artifact\n")},
		"ptah.sum":                       {Data: []byte("h1:example\n")},
	}

	snapshot, err := migrationartifact.Capture(source, migrator.MigrationDirFormatPtah)

	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(snapshot, "0000000001_create_users.up.sql", "ptah.sum"), qt.IsNil)
	_, err = fs.Stat(snapshot, "README.md")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

func TestPushToAndPullFrom_RoundTrip(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	source := fstest.MapFS{
		"0000000001_create_users.up.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"ptah.sum":                       {Data: []byte("h1:example\n")},
	}

	pushed, err := migrationartifact.PushTo(ctx, store, source, migrationartifact.PushOptions{
		Tags:      []string{"v1"},
		DirFormat: migrator.MigrationDirFormatPtah,
		Annotations: map[string]string{
			ocispec.AnnotationCreated: "2026-07-27T12:00:00Z",
		},
	})
	c.Assert(err, qt.IsNil)
	pulled, err := migrationartifact.PullFrom(ctx, store, "v1")
	c.Assert(err, qt.IsNil)

	c.Assert(pulled.Descriptor.Digest, qt.Equals, pushed.Descriptor.Digest)
	c.Assert(pulled.DirFormat, qt.Equals, migrator.MigrationDirFormatPtah)
	c.Assert(fstest.TestFS(pulled.FileSystem, "0000000001_create_users.up.sql", "ptah.sum"), qt.IsNil)
}

func TestCapture_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("no migrations", func(c *qt.C) {
		_, err := migrationartifact.Capture(
			fstest.MapFS{"ptah.sum": {Data: []byte("h1:example\n")}},
			migrator.MigrationDirFormatPtah,
		)
		c.Assert(err, qt.ErrorMatches, "migration artifact contains no migration files")
	})

	c.Run("invalid format", func(c *qt.C) {
		_, err := migrationartifact.Capture(
			fstest.MapFS{"001.sql": {Data: []byte("SELECT 1;\n")}},
			migrator.MigrationDirFormat("invalid"),
		)
		c.Assert(err, qt.ErrorMatches, `unknown migration directory format "invalid".*`)
	})

	c.Run("symbolic link", func(c *qt.C) {
		_, err := migrationartifact.Capture(
			fstest.MapFS{
				"0000000001_create_users.up.sql": {
					Data: []byte("../outside.sql"),
					Mode: fs.ModeSymlink,
				},
			},
			migrator.MigrationDirFormatPtah,
		)
		c.Assert(err, qt.ErrorIs, ociartifact.ErrUnsafeArtifactPath)
	})
}

func TestPullFrom_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	_, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"001.sql": {Data: []byte("SELECT 1;\n")},
	}, ociartifact.PushOptions{
		ArtifactType:   ociartifact.MigrationArtifactType,
		LayerMediaType: migrationartifact.LayerMediaType,
		Tags:           []string{"invalid-format"},
		Annotations: map[string]string{
			ocispec.AnnotationCreated:          "2026-07-27T12:00:00Z",
			"io.stokaro.ptah.migration-format": "invalid",
		},
	})
	c.Assert(err, qt.IsNil)

	_, err = migrationartifact.PullFrom(ctx, store, "invalid-format")

	c.Assert(err, qt.ErrorMatches, `invalid migration artifact format: unknown migration directory format "invalid".*`)
}
