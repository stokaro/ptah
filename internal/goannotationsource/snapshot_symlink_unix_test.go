//go:build unix

package goannotationsource_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"golang.org/x/sys/unix"

	"go.5x5.cz/ptah/internal/goannotationsource"
)

func TestCapture_FailurePath_RejectsSelectedSourceSymlink(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	target := writeSource(c.TB, t.TempDir(), "outside.go", "package outside\n")
	link := filepath.Join(root, "model.go")
	c.Assert(os.Symlink(target, link), qt.IsNil)

	snapshot, err := goannotationsource.Capture(root)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "refuse to capture symlinked Go source")
	c.Assert(snapshot, qt.IsNil)
}

func TestCapture_FailurePath_RejectsSelectedHiddenFilenameSymlink(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	target := writeSource(c.TB, t.TempDir(), "outside.go", "package outside\n")
	link := filepath.Join(root, ".model.go")
	c.Assert(os.Symlink(target, link), qt.IsNil)

	snapshot, err := goannotationsource.Capture(root)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "refuse to capture symlinked Go source")
	c.Assert(snapshot, qt.IsNil)
}

func TestCapture_FailurePath_RejectsSelectedNamedPipe(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := filepath.Join(root, "events.go")
	c.Assert(unix.Mkfifo(path, 0o600), qt.IsNil)

	snapshot, err := goannotationsource.Capture(root)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "refuse to capture non-regular Go source")
	c.Assert(snapshot, qt.IsNil)
}

func TestCapture_HappyPath_AnchorsSymlinkedRoot(t *testing.T) {
	c := qt.New(t)
	target := t.TempDir()
	writeSource(c.TB, target, "models.go", "package models\n")
	root := filepath.Join(t.TempDir(), "models")
	c.Assert(os.Symlink(target, root), qt.IsNil)

	snapshot, err := goannotationsource.Capture(root)

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Root(), qt.Equals, root)
	c.Assert(sourcePaths(snapshot.Files()), qt.DeepEquals, []string{"models.go"})
	c.Assert(os.Remove(root), qt.IsNil)
	c.Assert(os.Symlink(t.TempDir(), root), qt.IsNil)
	c.Assert(snapshot.Revalidate(), qt.ErrorIs, goannotationsource.ErrChanged)
}

func TestSnapshotSourceAlias_HappyPath_ReportsSymlinkAndHardLink(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeSource(c.TB, root, "model.go", "package models\n")
	aliases := t.TempDir()
	symlinkAlias := filepath.Join(aliases, "schema-symlink.hcl")
	hardLinkAlias := filepath.Join(aliases, "schema-hardlink.hcl")
	c.Assert(os.Symlink(source, symlinkAlias), qt.IsNil)
	c.Assert(os.Link(source, hardLinkAlias), qt.IsNil)
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	alias, err := snapshot.SourceAlias(symlinkAlias)
	c.Assert(err, qt.IsNil)
	c.Assert(alias, qt.Equals, source)

	alias, err = snapshot.SourceAlias(hardLinkAlias)
	c.Assert(err, qt.IsNil)
	c.Assert(alias, qt.Equals, source)
}
