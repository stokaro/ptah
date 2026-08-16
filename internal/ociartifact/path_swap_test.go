package ociartifact_test

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

// WriteToDir checks that its destination is absent before it starts staging,
// but a look is not a guarantee: what makes the check binding is that the
// install itself refuses a destination that appeared in between. These pin
// that refusal for both kinds of entry that can appear there.
//
// The file row used to be Unix-only (stokaro/ptah#1547), because the install
// was os.Rename, which refuses an existing destination on Unix and asks for
// replacement on Windows. Both rows run everywhere now that the install is a
// conditional move.
//
// The directory row is here because a destination that appears mid-write can
// be either kind, and the two are refused by different means: nothing in the
// file row would notice a move that happily replaced an empty directory.

func TestArtifactWriteToDir_PathSwapDoesNotDeleteReplacementFile(t *testing.T) {
	c := qt.New(t)
	install := startBlockedInstall(c)

	c.Assert(os.WriteFile(install.output, []byte("keep"), 0o600), qt.IsNil)
	err := install.finish()

	assertRefusedInstall(c, err)
	contents, readErr := os.ReadFile(install.output)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "keep")
}

func TestArtifactWriteToDir_PathSwapDoesNotReplaceDirectory(t *testing.T) {
	c := qt.New(t)
	install := startBlockedInstall(c)

	c.Assert(os.Mkdir(install.output, 0o755), qt.IsNil)
	err := install.finish()

	assertRefusedInstall(c, err)
	entries, readErr := os.ReadDir(install.output)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

// assertRefusedInstall states the refusal without restating the sentence the
// platform wrote. Windows spells this one across two lines -- "file already
// exists\nCannot create a file when that file already exists." -- and a `.*`
// pattern silently does not match a newline, so a regexp over the whole
// message is an assertion about the operating system's phrasing rather than
// about the refusal. What the caller can rely on is the sentinel and the
// operation that failed.
func assertRefusedInstall(c *qt.C, err error) {
	c.Helper()
	c.Assert(err, qt.ErrorIs, fs.ErrExist)
	c.Assert(err, qt.ErrorMatches, `(?s)install artifact output directory: .*`)
}

// blockedInstall is a WriteToDir suspended after it has checked its
// destination and while it is still staging, which is the only window in which
// something else can claim the output path.
type blockedInstall struct {
	output  string
	release chan struct{}
	result  chan error
}

// finish lets the suspended install proceed and reports what it returned.
func (i *blockedInstall) finish() error {
	close(i.release)
	return <-i.result
}

func startBlockedInstall(c *qt.C) *blockedInstall {
	c.Helper()
	opened := make(chan struct{})
	install := &blockedInstall{
		output:  filepath.Join(c.TempDir(), "output"),
		release: make(chan struct{}),
		result:  make(chan error, 1),
	}
	artifact := ociartifact.Artifact{FileSystem: &blockingFS{
		MapFS: fstest.MapFS{
			"migration.sql": {Data: []byte("SELECT 1;\n")},
		},
		opened:  opened,
		release: install.release,
	}}
	go func() {
		install.result <- artifact.WriteToDir(install.output)
	}()
	<-opened
	return install
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
