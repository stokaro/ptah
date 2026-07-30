//go:build !windows

package pathguard_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/pathguard"
)

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
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
	c.Assert(opened, qt.IsNil)
}

func TestOpenDirectoryWithinRootAnchorsAllowedRoot(t *testing.T) {
	c := qt.New(t)
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	migrations := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(filepath.Join(migrations, "1.sql"), []byte("SELECT 'safe';\n"), 0o600),
		qt.IsNil,
	)

	opened, err := pathguard.OpenDirectoryWithinRoot(migrations, root)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	capturedRoot := filepath.Join(parent, "captured-root")
	c.Assert(os.Rename(root, capturedRoot), qt.IsNil)
	outside := t.TempDir()
	c.Assert(os.Mkdir(filepath.Join(outside, "migrations"), 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(filepath.Join(outside, "migrations", "1.sql"), []byte("SELECT 'outside';\n"), 0o600),
		qt.IsNil,
	)
	c.Assert(os.Symlink(outside, root), qt.IsNil)

	contents, err := fs.ReadFile(opened.FS(), "1.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 'safe';\n")
}
