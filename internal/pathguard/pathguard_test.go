package pathguard

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestResolveWithinRootAllowsMissingChild(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := filepath.Join(root, "migrations")

	resolved, err := ResolveWithinRoot(path, root)
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Base(resolved), qt.Equals, "migrations")
}

func TestResolveWithinRootRejectsTraversal(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := filepath.Join(root, "..", "outside")

	_, err := ResolveWithinRoot(path, root)
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
}

func TestResolveWithinRootRejectsSymlinkEscape(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink setup is not available: %v", err)
	}

	_, err := ResolveWithinRoot(filepath.Join(link, "migrations"), root)
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
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

	_, err = ResolveCLIPath("../outside")
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
}
