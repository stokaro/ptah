package ociartifact_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/memory"

	"go.5x5.cz/ptah/internal/ociartifact"
)

const fixedCreatedAt = "2026-07-27T12:00:00Z"

func TestPushToAndPullFrom_RoundTrip(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	source := fstest.MapFS{
		"001_create_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
		"001_create_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
		"metadata/ptah.sum":         {Data: []byte("h1:example\n")},
	}

	first, err := ociartifact.PushTo(ctx, store, source, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{"latest", "v1", "latest"},
		Annotations:  map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(first.Tags, qt.DeepEquals, []string{"v1", "latest"})

	second, err := ociartifact.PushTo(ctx, store, source, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{"v2"},
		Annotations:  map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(second.Descriptor.Digest, qt.Equals, first.Descriptor.Digest)

	pulled, err := ociartifact.PullFrom(ctx, store, "v1", ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(pulled.Descriptor.Digest, qt.Equals, first.Descriptor.Digest)
	c.Assert(pulled.ArtifactType, qt.Equals, ociartifact.MigrationArtifactType)
	c.Assert(fstest.TestFS(
		pulled.FileSystem,
		"001_create_users.up.sql",
		"001_create_users.down.sql",
		"metadata/ptah.sum",
	), qt.IsNil)

	contents, err := fs.ReadFile(pulled.FileSystem, "001_create_users.up.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	contents[0] = 'X'
	fresh, err := fs.ReadFile(pulled.FileSystem, "001_create_users.up.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(fresh), qt.Equals, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
}

func TestPushTo_WriteOnceTagConflictLeavesLatestUnchanged(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()

	first, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"migration.sql": {Data: []byte("SELECT 1;\n")},
	}, ociartifact.PushOptions{
		ArtifactType:  ociartifact.MigrationArtifactType,
		Tags:          []string{ociartifact.DefaultTag},
		WriteOnceTags: []string{"v20260727120000"},
	})
	c.Assert(err, qt.IsNil)

	_, err = ociartifact.PushTo(ctx, store, fstest.MapFS{
		"migration.sql": {Data: []byte("SELECT 2;\n")},
	}, ociartifact.PushOptions{
		ArtifactType:  ociartifact.MigrationArtifactType,
		Tags:          []string{ociartifact.DefaultTag},
		WriteOnceTags: []string{"v20260727120000"},
	})
	c.Assert(err, qt.ErrorIs, ociartifact.ErrTagConflict)

	version, err := store.Resolve(ctx, "v20260727120000")
	c.Assert(err, qt.IsNil)
	c.Assert(version.Digest, qt.Equals, first.Descriptor.Digest)
	latest, err := store.Resolve(ctx, ociartifact.DefaultTag)
	c.Assert(err, qt.IsNil)
	c.Assert(latest.Digest, qt.Equals, first.Descriptor.Digest)
}

func TestArtifactWriteToDir_HappyPath(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	_, err := ociartifact.PushTo(context.Background(), store, fstest.MapFS{
		"nested/migration.sql": {Data: []byte("SELECT 1;\n")},
		"ptah.sum":             {Data: []byte("h1:example\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{"latest"},
		Annotations:  map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)
	artifact, err := ociartifact.PullFrom(context.Background(), store, "latest", ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
	})
	c.Assert(err, qt.IsNil)
	output := filepath.Join(t.TempDir(), "pulled")

	err = artifact.WriteToDir(output)
	c.Assert(err, qt.IsNil)
	migration, err := os.ReadFile(filepath.Join(output, "nested", "migration.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(migration), qt.Equals, "SELECT 1;\n")
	sum, err := os.ReadFile(filepath.Join(output, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Equals, "h1:example\n")
	info, err := os.Stat(filepath.Join(output, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, fs.FileMode(0o600))
}

func TestArtifactWriteToDir_FailurePath(t *testing.T) {
	t.Run("missing filesystem", func(t *testing.T) {
		c := qt.New(t)
		err := (ociartifact.Artifact{}).WriteToDir(filepath.Join(t.TempDir(), "output"))
		c.Assert(err, qt.ErrorMatches, "artifact filesystem is required")
	})

	t.Run("empty directory destination", func(t *testing.T) {
		c := qt.New(t)
		output := t.TempDir()
		artifact := ociartifact.Artifact{FileSystem: fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;")},
		}}

		err := artifact.WriteToDir(output)
		c.Assert(err, qt.ErrorMatches, "artifact output path already exists: .*")
	})

	t.Run("nonempty destination", func(t *testing.T) {
		c := qt.New(t)
		output := t.TempDir()
		err := os.WriteFile(filepath.Join(output, "keep.txt"), []byte("keep"), 0o600)
		c.Assert(err, qt.IsNil)
		artifact := ociartifact.Artifact{FileSystem: fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;")},
		}}

		err = artifact.WriteToDir(output)
		c.Assert(err, qt.ErrorMatches, "artifact output path already exists: .*")
		contents, err := os.ReadFile(filepath.Join(output, "keep.txt"))
		c.Assert(err, qt.IsNil)
		c.Assert(string(contents), qt.Equals, "keep")
	})

	t.Run("file destination", func(t *testing.T) {
		c := qt.New(t)
		output := filepath.Join(t.TempDir(), "output")
		err := os.WriteFile(output, []byte("keep"), 0o600)
		c.Assert(err, qt.IsNil)
		artifact := ociartifact.Artifact{FileSystem: fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;")},
		}}

		err = artifact.WriteToDir(output)
		c.Assert(err, qt.ErrorMatches, "artifact output path already exists: .*")
	})
}

func TestArtifactWriteToDir_PathSwapDoesNotDeleteReplacement(t *testing.T) {
	c := qt.New(t)
	output := filepath.Join(t.TempDir(), "output")
	opened := make(chan struct{})
	release := make(chan struct{})
	artifact := ociartifact.Artifact{FileSystem: &blockingFS{
		MapFS: fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;\n")},
		},
		opened:  opened,
		release: release,
	}}
	result := make(chan error, 1)

	go func() {
		result <- artifact.WriteToDir(output)
	}()
	<-opened
	c.Assert(os.WriteFile(output, []byte("keep"), 0o600), qt.IsNil)
	close(release)

	err := <-result
	c.Assert(err, qt.ErrorMatches, "install artifact output directory: .*")
	contents, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "keep")
}

func TestPushTo_PartialTagFailureReturnsPublishedState(t *testing.T) {
	tests := []struct {
		name        string
		failAt      int
		wantApplied []string
		wantFailed  string
	}{
		{
			name:        "second tag",
			failAt:      2,
			wantApplied: []string{"v1"},
			wantFailed:  "stable",
		},
		{
			name:        "final latest tag",
			failAt:      3,
			wantApplied: []string{"v1", "stable"},
			wantFailed:  ociartifact.DefaultTag,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			target := &failingTagTarget{
				Store:  memory.New(),
				failAt: tt.failAt,
				err:    errors.New("injected tag failure"),
			}

			result, err := ociartifact.PushTo(context.Background(), target, fstest.MapFS{
				"migration.sql": {Data: []byte("SELECT 1;\n")},
			}, ociartifact.PushOptions{
				ArtifactType: ociartifact.MigrationArtifactType,
				Tags:         []string{"v1", "stable", ociartifact.DefaultTag},
			})

			var partial *ociartifact.PartialPushError
			c.Assert(err, qt.ErrorAs, &partial)
			c.Assert(result.Descriptor.Digest, qt.Equals, partial.Descriptor.Digest)
			c.Assert(result.Tags, qt.DeepEquals, tt.wantApplied)
			c.Assert(partial.AppliedTags, qt.DeepEquals, tt.wantApplied)
			c.Assert(partial.FailedTag, qt.Equals, tt.wantFailed)
			c.Assert(partial.Err, qt.ErrorMatches, "injected tag failure")
		})
	}
}

func TestPushTo_FailurePath(t *testing.T) {
	ctx := context.Background()

	t.Run("nil filesystem", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), nil, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
		})
		c.Assert(err, qt.ErrorMatches, "artifact filesystem is required")
	})

	t.Run("missing artifact type", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;")},
		}, ociartifact.PushOptions{})
		c.Assert(err, qt.ErrorMatches, "artifact type is required")
	})

	t.Run("empty artifact", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
		})
		c.Assert(err, qt.ErrorMatches, "artifact must contain at least one file")
	})

	t.Run("symbolic link", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{
			"migration.sql": {Data: []byte("../outside.sql"), Mode: fs.ModeSymlink},
		}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrUnsafeArtifactPath)
	})

	t.Run("file count limit", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{
			"one.sql": {Data: []byte("SELECT 1;")},
			"two.sql": {Data: []byte("SELECT 2;")},
		}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
			Limits:       ociartifact.Limits{Files: 1},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})

	t.Run("single file limit", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;")},
		}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
			Limits:       ociartifact.Limits{FileBytes: 4},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})

	t.Run("file growth after stat remains bounded", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), misreportedSizeFS{
			data:         "12345",
			reportedSize: 1,
		}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
			Limits:       ociartifact.Limits{FileBytes: 4},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})

	t.Run("total size limit", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{
			"one.sql": {Data: []byte("1234")},
			"two.sql": {Data: []byte("5678")},
		}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
			Limits:       ociartifact.Limits{TotalBytes: 7},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})

	t.Run("path length limit", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{
			strings.Repeat("a", 9) + ".sql": {Data: []byte("SELECT 1;")},
		}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
			Limits:       ociartifact.Limits{PathBytes: 8},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})

	t.Run("path depth limit", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{
			"one/two/migration.sql": {Data: []byte("SELECT 1;")},
		}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
			Limits:       ociartifact.Limits{PathDepth: 2},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})

	t.Run("invalid tag before upload", func(t *testing.T) {
		c := qt.New(t)
		store := memory.New()
		_, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;")},
		}, ociartifact.PushOptions{
			ArtifactType: ociartifact.MigrationArtifactType,
			Tags:         []string{"not a tag"},
		})
		c.Assert(err, qt.ErrorMatches, `invalid OCI tag "not a tag": .*`)
		_, err = store.Resolve(ctx, "latest")
		c.Assert(err, qt.IsNotNil)
	})

	t.Run("latest cannot be write once", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.PushTo(ctx, memory.New(), fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;")},
		}, ociartifact.PushOptions{
			ArtifactType:  ociartifact.MigrationArtifactType,
			WriteOnceTags: []string{ociartifact.DefaultTag},
		})
		c.Assert(err, qt.ErrorMatches, `write-once OCI tag "latest" is reserved for the movable latest pointer`)
	})
}

type blockingFS struct {
	fstest.MapFS
	opened  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingFS) Open(name string) (fs.File, error) {
	if name == "migration.sql" {
		f.block()
	}
	return f.MapFS.Open(name)
}

func (f *blockingFS) ReadFile(name string) ([]byte, error) {
	if name == "migration.sql" {
		f.block()
	}
	return f.MapFS.ReadFile(name)
}

func (f *blockingFS) block() {
	f.once.Do(func() {
		close(f.opened)
	})
	<-f.release
}

type failingTagTarget struct {
	*memory.Store
	failAt int
	calls  int
	err    error
}

func (t *failingTagTarget) Tag(
	ctx context.Context,
	descriptor ocispec.Descriptor,
	tag string,
) error {
	t.calls++
	if t.calls == t.failAt {
		return t.err
	}
	return t.Store.Tag(ctx, descriptor, tag)
}

type misreportedSizeFS struct {
	data         string
	reportedSize int64
}

func (f misreportedSizeFS) Open(name string) (fs.File, error) {
	switch name {
	case ".":
		return &misreportedFile{
			Reader: strings.NewReader(""),
			info:   misreportedInfo{name: ".", directory: true},
		}, nil
	case "migration.sql":
		return &misreportedFile{
			Reader: strings.NewReader(f.data),
			info:   misreportedInfo{name: name, size: f.reportedSize},
		}, nil
	default:
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
}

func (f misreportedSizeFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "." {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	return []fs.DirEntry{misreportedInfo{name: "migration.sql", size: f.reportedSize}}, nil
}

type misreportedFile struct {
	*strings.Reader
	info misreportedInfo
}

func (f *misreportedFile) Stat() (fs.FileInfo, error) {
	return f.info, nil
}

func (f *misreportedFile) Close() error {
	return nil
}

type misreportedInfo struct {
	name      string
	size      int64
	directory bool
}

func (i misreportedInfo) Name() string {
	return i.name
}

func (i misreportedInfo) Size() int64 {
	return i.size
}

func (i misreportedInfo) Mode() fs.FileMode {
	if i.directory {
		return fs.ModeDir | 0o555
	}
	return 0o444
}

func (misreportedInfo) ModTime() time.Time {
	return time.Time{}
}

func (i misreportedInfo) IsDir() bool {
	return i.directory
}

func (misreportedInfo) Sys() any {
	return nil
}

func (i misreportedInfo) Type() fs.FileMode {
	return i.Mode().Type()
}

func (i misreportedInfo) Info() (fs.FileInfo, error) {
	return i, nil
}

func TestPullFrom_RejectsUntrustedManifest(t *testing.T) {
	ctx := context.Background()

	t.Run("path traversal", func(t *testing.T) {
		c := qt.New(t)
		store := memory.New()
		pushManifest(c.TB, ctx, store, "../escape.sql", ociartifact.FileMediaType, ociartifact.MigrationArtifactType, "unsafe")

		_, err := ociartifact.PullFrom(ctx, store, "unsafe", ociartifact.PullOptions{
			ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrUnsafeArtifactPath)
	})

	t.Run("backslash path", func(t *testing.T) {
		c := qt.New(t)
		store := memory.New()
		pushManifest(c.TB, ctx, store, `dir\escape.sql`, ociartifact.FileMediaType, ociartifact.MigrationArtifactType, "unsafe")

		_, err := ociartifact.PullFrom(ctx, store, "unsafe", ociartifact.PullOptions{
			ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrUnsafeArtifactPath)
	})

	t.Run("unexpected artifact type", func(t *testing.T) {
		c := qt.New(t)
		store := memory.New()
		pushManifest(c.TB, ctx, store, "schema.sql", ociartifact.FileMediaType, ociartifact.SchemaArtifactType, "schema")

		_, err := ociartifact.PullFrom(ctx, store, "schema", ociartifact.PullOptions{
			ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrUnexpectedArtifactType)
	})

	t.Run("unexpected layer type", func(t *testing.T) {
		c := qt.New(t)
		store := memory.New()
		pushManifest(c.TB, ctx, store, "migration.sql", "application/octet-stream", ociartifact.MigrationArtifactType, "wrong-layer")

		_, err := ociartifact.PullFrom(ctx, store, "wrong-layer", ociartifact.PullOptions{
			ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrUnexpectedArtifactType)
	})

	t.Run("file size limit", func(t *testing.T) {
		c := qt.New(t)
		store := memory.New()
		pushManifest(c.TB, ctx, store, "migration.sql", ociartifact.FileMediaType, ociartifact.MigrationArtifactType, "large")

		_, err := ociartifact.PullFrom(ctx, store, "large", ociartifact.PullOptions{
			ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
			Limits:                ociartifact.Limits{FileBytes: 4},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})

	t.Run("path length limit", func(t *testing.T) {
		c := qt.New(t)
		store := memory.New()
		pushManifest(c.TB, ctx, store, "migration.sql", ociartifact.FileMediaType, ociartifact.MigrationArtifactType, "long-path")

		_, err := ociartifact.PullFrom(ctx, store, "long-path", ociartifact.PullOptions{
			ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
			Limits:                ociartifact.Limits{PathBytes: 8},
		})
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})
}

func TestNewRepository_UsesDockerConfigurationAndExplicitPlainHTTP(t *testing.T) {
	c := qt.New(t)
	configDir := t.TempDir()
	err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte("{}\n"), 0o600)
	c.Assert(err, qt.IsNil)
	t.Setenv("DOCKER_CONFIG", configDir)
	ref, err := ociartifact.ParseRef("oci://localhost:5000/acme/migrations")
	c.Assert(err, qt.IsNil)

	repository, err := ociartifact.NewRepository(ref, ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	c.Assert(repository.PlainHTTP, qt.IsTrue)
	c.Assert(repository.Reference.Registry, qt.Equals, "localhost:5000")
	c.Assert(repository.Reference.Repository, qt.Equals, "acme/migrations")
	c.Assert(repository.Client, qt.IsNotNil)
}

func pushManifest(
	tb testing.TB,
	ctx context.Context,
	store *memory.Store,
	title string,
	layerMediaType string,
	artifactType string,
	tag string,
) {
	c := qt.New(tb)
	layer, err := oras.PushBytes(ctx, store, layerMediaType, []byte("SELECT 1;"))
	c.Assert(err, qt.IsNil)
	layer.Annotations = map[string]string{ocispec.AnnotationTitle: title}
	manifest, err := oras.PackManifest(ctx, store, oras.PackManifestVersion1_1, artifactType, oras.PackManifestOptions{
		Layers:              []ocispec.Descriptor{layer},
		ManifestAnnotations: map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)
	err = store.Tag(ctx, manifest, tag)
	c.Assert(err, qt.IsNil)
}

// TestPush_DigestReferenceFailsBeforeNetworkAccess pins that every reference
// carrying a digest is refused by the push gate, including the tag@digest form
// ParseRef now accepts (stokaro/ptah#944). Accepting that shape must not let a
// push through: the tag beside the digest is a label, not a push target.
func TestPush_DigestReferenceFailsBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	configDir := t.TempDir()
	err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte("{}\n"), 0o600)
	c.Assert(err, qt.IsNil)
	t.Setenv("DOCKER_CONFIG", configDir)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name string
		ref  string
	}{
		{name: "digest only", ref: "oci://registry.invalid/acme/migrations@" + digest},
		{name: "tag and digest", ref: "oci://registry.invalid/acme/migrations:stable@" + digest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := client.Push(context.Background(), tt.ref, fstest.MapFS{
				"migration.sql": {Data: []byte("SELECT 1;")},
			}, ociartifact.PushOptions{ArtifactType: ociartifact.MigrationArtifactType})
			c.Assert(err, qt.ErrorIs, ociartifact.ErrDigestPush)
			c.Assert(err, qt.Not(qt.ErrorIs), ociartifact.ErrInvalidReference)
		})
	}
}
