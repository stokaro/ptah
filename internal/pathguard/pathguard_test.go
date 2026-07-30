package pathguard_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/pathguard"
)

func TestResolveWithinRootAllowsMissingChild(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := filepath.Join(root, "migrations")

	resolved, err := pathguard.ResolveWithinRoot(path, root)
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Base(resolved), qt.Equals, "migrations")
}

func TestResolveWithinRootRejectsTraversal(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := filepath.Join(root, "..", "outside")

	_, err := pathguard.ResolveWithinRoot(path, root)
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
}

func TestResolveWithinRootRejectsSymlinkEscape(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "link")
	c.Assert(os.Symlink(outside, link), qt.IsNil)

	_, err := pathguard.ResolveWithinRoot(filepath.Join(link, "migrations"), root)
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
}

func TestResolveCLIPathRejectsRelativeTraversal(t *testing.T) {
	c := qt.New(t)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	root := t.TempDir()
	c.Assert(os.Chdir(root), qt.IsNil)
	t.Cleanup(func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	})

	_, err = pathguard.ResolveCLIPath("../outside")
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
}

func TestOpenCLIDirectoryPreservesExplicitAbsolutePath(t *testing.T) {
	c := qt.New(t)
	outside := t.TempDir()

	opened, err := pathguard.OpenCLIDirectory(outside)

	c.Assert(err, qt.IsNil)
	absolute, err := filepath.Abs(outside)
	c.Assert(err, qt.IsNil)
	c.Assert(opened.Path(), qt.Equals, absolute)
	c.Assert(opened.Close(), qt.IsNil)
}
