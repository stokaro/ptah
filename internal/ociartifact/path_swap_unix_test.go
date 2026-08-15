//go:build unix

package ociartifact_test

// The guarantee this file pins is Unix-only today, and that is a gap rather
// than a platform difference.
//
// WriteToDir installs its staging directory with os.Rename. Renaming a
// directory onto an existing file fails on Unix, which is exactly what makes a
// path swapped in mid-write survive. On Windows the same call did not fail, so
// nothing here establishes that the replacement is preserved -- see
// stokaro/ptah#1547. fsdurable already carries the primitive that would
// settle it, rootRenameNoReplace, but only for names inside an os.Root.
//
// Build-tagged rather than deleted or weakened: the property holds where it is
// asserted, and the platform where it does not is recorded rather than
// quietly blessed.

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociartifact"
)

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
