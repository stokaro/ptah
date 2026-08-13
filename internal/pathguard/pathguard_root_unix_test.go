//go:build !windows

package pathguard_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/pathguard"
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

func TestOpenCLIDirectoryKeepsLexicalDisplayPathAfterSymlinkReplacement(t *testing.T) {
	c := qt.New(t)
	parent := t.TempDir()
	original := filepath.Join(parent, "original")
	replacement := filepath.Join(parent, "replacement")
	link := filepath.Join(parent, "migrations")
	c.Assert(os.Mkdir(original, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(replacement, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(original, "1.sql"), []byte("SELECT 'original';\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(replacement, "1.sql"), []byte("SELECT 'replacement';\n"), 0o600), qt.IsNil)
	c.Assert(os.Symlink(original, link), qt.IsNil)

	opened, err := pathguard.OpenCLIDirectory(link)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	c.Assert(os.Remove(link), qt.IsNil)
	c.Assert(os.Symlink(replacement, link), qt.IsNil)

	contents, err := fs.ReadFile(opened.FS(), "1.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 'original';\n")
	c.Assert(opened.Path(), qt.Equals, link)
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

func TestOpenedDirectoryMkdirAllNamesEscapingSymlink(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	c.Assert(os.Symlink(outside, filepath.Join(root, "link")), qt.IsNil)
	opened, err := pathguard.OpenDirectory(root)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	err = opened.MkdirAll(filepath.Join(root, "link", "migrations"), 0o700)

	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
	_, err = os.Stat(filepath.Join(outside, "migrations"))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

func TestOpenedDirectoryMkdirAllNamesEscapingFinalSymlink(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	c.Assert(os.Symlink(outside, filepath.Join(root, "link")), qt.IsNil)
	opened, err := pathguard.OpenDirectory(root)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	err = opened.MkdirAll(filepath.Join(root, "link"), 0o700)

	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
}

func TestOpenedDirectoryMkdirAllRefusesContainedDirectorySymlink(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	target := filepath.Join(root, "target")
	c.Assert(os.Mkdir(target, 0o700), qt.IsNil)
	c.Assert(os.Symlink(target, filepath.Join(root, "link")), qt.IsNil)
	opened, err := pathguard.OpenDirectory(root)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	err = opened.MkdirAll(filepath.Join(root, "link", "migrations"), 0o700)

	c.Assert(err, qt.ErrorMatches, `.*path escapes from parent.*`)
	_, err = os.Stat(filepath.Join(target, "migrations"))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
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

func TestOpenCurrentDirectoryAnchorsBeforePathReplacement(t *testing.T) {
	c := qt.New(t)
	parent := t.TempDir()
	current := filepath.Join(parent, "current")
	migrations := filepath.Join(current, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1.sql"), []byte("SELECT 'safe';\n"), 0o600), qt.IsNil)
	t.Chdir(current)

	root, err := pathguard.OpenCurrentDirectory()
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(root.Close(), qt.IsNil)
	})

	captured := filepath.Join(parent, "captured")
	c.Assert(os.Rename(current, captured), qt.IsNil)
	outside := t.TempDir()
	c.Assert(os.Mkdir(filepath.Join(outside, "migrations"), 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(filepath.Join(outside, "migrations", "1.sql"), []byte("SELECT 'outside';\n"), 0o600),
		qt.IsNil,
	)
	c.Assert(os.Symlink(outside, current), qt.IsNil)

	opened, err := root.OpenDirectory("migrations")
	c.Assert(err, qt.IsNil)
	contents, err := fs.ReadFile(opened.FS(), "1.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 'safe';\n")
	c.Assert(opened.Close(), qt.IsNil)
}

func TestOpenedDirectoryReplaceFileAnchorsBeforeAncestorReplacement(t *testing.T) {
	c := qt.New(t)
	parent := c.TempDir()
	directory := filepath.Join(parent, "root")
	c.Assert(os.Mkdir(directory, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "target"), []byte("old"), 0o600), qt.IsNil)
	opened, err := pathguard.OpenDirectory(directory)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	staged, stagedName, err := opened.CreateTemp("staged-*")
	c.Assert(err, qt.IsNil)
	_, err = staged.WriteString("new")
	c.Assert(err, qt.IsNil)
	c.Assert(staged.Sync(), qt.IsNil)
	c.Assert(staged.Close(), qt.IsNil)

	captured := filepath.Join(parent, "captured")
	c.Assert(os.Rename(directory, captured), qt.IsNil)
	outside := filepath.Join(parent, "outside")
	c.Assert(os.Mkdir(outside, 0o700), qt.IsNil)
	outsideContents := []byte("outside")
	c.Assert(os.WriteFile(filepath.Join(outside, "target"), outsideContents, 0o600), qt.IsNil)
	c.Assert(os.Symlink(outside, directory), qt.IsNil)

	c.Assert(opened.ReplaceFile(stagedName, "target"), qt.IsNil)
	c.Assert(opened.Sync(), qt.IsNil)

	contents, err := os.ReadFile(filepath.Join(captured, "target"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "new")
	contents, err = os.ReadFile(filepath.Join(outside, "target"))
	c.Assert(err, qt.IsNil)
	c.Assert(contents, qt.DeepEquals, outsideContents)
	_, err = os.Stat(filepath.Join(captured, stagedName))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
