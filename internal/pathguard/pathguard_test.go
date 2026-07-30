package pathguard_test

import (
	"io/fs"
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
}

func TestResolveWithinRootRejectsSymlinkEscape(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "link")
	c.Assert(os.Symlink(outside, link), qt.IsNil)

	_, err := pathguard.ResolveWithinRoot(filepath.Join(link, "migrations"), root)
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

	_, err = pathguard.ResolveCLIPath("../outside")
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
}

func TestOpenDirectoryWithinRootAnchorsOpenedDirectory(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrations := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(filepath.Join(migrations, "1.sql"), []byte("SELECT 'safe';\n"), 0o600),
		qt.IsNil,
	)

	opened, err := pathguard.OpenDirectoryWithinRoot(migrations, root)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	outside := t.TempDir()
	c.Assert(
		os.WriteFile(filepath.Join(outside, "1.sql"), []byte("SELECT 'outside';\n"), 0o600),
		qt.IsNil,
	)
	c.Assert(os.Rename(migrations, filepath.Join(root, "captured")), qt.IsNil)
	c.Assert(os.Symlink(outside, migrations), qt.IsNil)

	contents, err := fs.ReadFile(opened.FS(), "1.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 'safe';\n")
}

func TestOpenDirectoryWithinRootRejectsEscapingSymlink(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "migrations")
	c.Assert(os.Symlink(outside, link), qt.IsNil)

	opened, err := pathguard.OpenDirectoryWithinRoot(link, root)

	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(opened, qt.IsNil)
}

func TestOpenCLIDirectoryPreservesExplicitAbsolutePath(t *testing.T) {
	c := qt.New(t)
	outside := t.TempDir()

	opened, err := pathguard.OpenCLIDirectory(outside)

	c.Assert(err, qt.IsNil)
	resolved, err := filepath.EvalSymlinks(outside)
	c.Assert(err, qt.IsNil)
	c.Assert(opened.Path(), qt.Equals, resolved)
	c.Assert(opened.Close(), qt.IsNil)
}
