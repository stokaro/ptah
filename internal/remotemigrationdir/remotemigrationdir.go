// Package remotemigrationdir opens a migration directory that lives in an OCI
// registry, named by the vendor `atlas://` spelling.
//
// It exists so that the two places which accept such a reference -- an
// `atlas.hcl` migration.dir and the `--dir` flag -- share one pull path. Two
// copies would be two answers to what a pulled directory is, what transport it
// uses, and when the fetch happens (stokaro/ptah#1210).
package remotemigrationdir

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"sync"

	"ptah.run/internal/atlasregistry"
	"ptah.run/internal/migrationartifact"
	"ptah.run/internal/ociartifact"
)

// Names reports whether a raw migration-directory value is a registry
// reference rather than a path.
func Names(raw string) bool {
	return strings.HasPrefix(strings.TrimSpace(raw), atlasregistry.Scheme)
}

// Open returns a filesystem over the migration directory the reference names.
//
// Nothing happens until the filesystem is READ. Resolution and fetch are both
// deferred, because both callers reach this before knowing whether the command
// will open the directory at all: a project file is read by every verb, and a
// flag is parsed before the verb decides what it needs. An eager pull would
// make `schema inspect --env prod` fail over a directory it never opens, and
// eager resolution would fail the same command whenever PTAH_ATLAS_REGISTRY is
// unset.
//
// The outcome is kept, error included: a verb that reads the directory twice
// must not fetch twice, and must not succeed the second time after failing the
// first.
func Open(ctx context.Context, reference string) fs.FS {
	return &lazyFS{
		fetch: func() (fs.FS, error) { return pull(ctx, strings.TrimSpace(reference)) },
	}
}

// pull resolves the reference and fetches the artifact it names.
//
// An `oci://` reference is already a location: it carries registry, repository
// and tag, so it skips [atlasregistry.Resolve], which exists to compose those
// from the configured namespace for a bare `atlas://` repository. Sending one
// through anyway would either refuse it -- Resolve accepts only the vendor
// scheme -- or, if it did not, prefix a namespace onto a registry the author
// already named (stokaro/ptah#1215).
func pull(ctx context.Context, reference string) (fs.FS, error) {
	location := strings.TrimSpace(reference)
	if !atlasregistry.IsOCIReference(location) {
		resolved, err := atlasregistry.Resolve(reference)
		if err != nil {
			return nil, err
		}
		location = resolved.OCI
	}
	plainHTTP, err := atlasregistry.PlainHTTP.Resolve()
	if err != nil {
		return nil, fmt.Errorf("reading the registry transport setting: %w", err)
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: plainHTTP})
	if err != nil {
		return nil, fmt.Errorf("opening the registry client: %w", err)
	}
	artifact, err := migrationartifact.Pull(ctx, client, location)
	if err != nil {
		return nil, err
	}
	return artifact.FileSystem, nil
}

// lazyFS resolves and pulls on first read.
type lazyFS struct {
	once  sync.Once
	fetch func() (fs.FS, error)
	fsys  fs.FS
	err   error
}

func (l *lazyFS) resolve() (fs.FS, error) {
	l.once.Do(func() { l.fsys, l.err = l.fetch() })
	return l.fsys, l.err
}

func (l *lazyFS) Open(name string) (fs.File, error) {
	fsys, err := l.resolve()
	if err != nil {
		return nil, err
	}
	return fsys.Open(name)
}

// ReadDir keeps the fast path a directory read would otherwise lose: without
// it, fs.ReadDir falls back to Open and the pulled artifact's own ReadDirFS
// implementation goes unused.
func (l *lazyFS) ReadDir(name string) ([]fs.DirEntry, error) {
	fsys, err := l.resolve()
	if err != nil {
		return nil, err
	}
	return fs.ReadDir(fsys, name)
}
