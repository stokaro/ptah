//go:build !windows

package pathguard_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pathguard"
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

func TestReadFileWithinRoot_RefusesASymlinkLeavingTheRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	outside := filepath.Join(parent, "outside")
	setup := qt.New(t)
	setup.Assert(os.Mkdir(root, 0o750), qt.IsNil)
	setup.Assert(os.Mkdir(outside, 0o750), qt.IsNil)
	setup.Assert(os.WriteFile(filepath.Join(outside, "rows.yaml"), []byte("outside"), 0o600), qt.IsNil)
	setup.Assert(os.Symlink(filepath.Join(outside, "rows.yaml"), filepath.Join(root, "file-link.yaml")), qt.IsNil)
	setup.Assert(os.Symlink(outside, filepath.Join(root, "dir-link")), qt.IsNil)

	tests := []struct {
		name string
		path string
	}{
		{name: "the file is the link", path: filepath.Join(root, "file-link.yaml")},
		{name: "a directory on the way is the link", path: filepath.Join(root, "dir-link", "rows.yaml")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := pathguard.ReadFileWithinRoot(test.path, root)
			c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
			c.Assert(data, qt.IsNil)
		})
	}
}

// TestReadFileWithinRoot_FollowsASymlinkStayingInside keeps the boundary from
// being a ban on links. An absolute link is what a rooted open alone would refuse,
// so this row is what tells the resolved read from a plain rooted one.
func TestReadFileWithinRoot_FollowsASymlinkStayingInside(t *testing.T) {
	root := t.TempDir()
	setup := qt.New(t)
	setup.Assert(os.WriteFile(filepath.Join(root, "rows.yaml"), []byte("inside"), 0o600), qt.IsNil)
	setup.Assert(os.Symlink("rows.yaml", filepath.Join(root, "relative-link.yaml")), qt.IsNil)
	setup.Assert(os.Symlink(filepath.Join(root, "rows.yaml"), filepath.Join(root, "absolute-link.yaml")), qt.IsNil)

	tests := []struct {
		name string
		path string
	}{
		{name: "relative link", path: filepath.Join(root, "relative-link.yaml")},
		{name: "absolute link", path: filepath.Join(root, "absolute-link.yaml")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := pathguard.ReadFileWithinRoot(test.path, root)
			c.Assert(err, qt.IsNil)
			c.Assert(string(data), qt.Equals, "inside")
		})
	}
}

// TestReadFileWithinRoot_ReachesThroughARootThatIsALink covers a root spelled
// through a link, the shape /tmp is on macOS: the path and the root have to meet
// after both are resolved.
func TestReadFileWithinRoot_ReachesThroughARootThatIsALink(t *testing.T) {
	c := qt.New(t)
	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	c.Assert(os.Mkdir(target, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(target, "rows.yaml"), []byte("inside"), 0o600), qt.IsNil)
	link := filepath.Join(parent, "link")
	c.Assert(os.Symlink(target, link), qt.IsNil)

	data, err := pathguard.ReadFileWithinRoot(filepath.Join(target, "rows.yaml"), link)

	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, "inside")
}
