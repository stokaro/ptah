package pathguard_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/pathguard"
)

func openTestDirectory(c *qt.C, dir string) *pathguard.OpenedDirectory {
	c.Helper()
	opened, err := pathguard.OpenDirectory(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(opened.Close(), qt.IsNil)
	})
	return opened
}

// TestOpenedDirectoryPublicationOperations_HappyPath covers the rooted
// operations a journaled publication needs beyond replace-and-sync: opening a
// staged file for writing, hard-linking it into place, moving it without
// replacing, and listing the directory's own entries.
func TestOpenedDirectoryPublicationOperations_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "staged"), []byte("contents"), 0o600), qt.IsNil)
	opened := openTestDirectory(c, dir)

	file, err := opened.OpenFile("staged", os.O_RDWR, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(file.Sync(), qt.IsNil)
	c.Assert(file.Close(), qt.IsNil)

	c.Assert(opened.Link("staged", "linked"), qt.IsNil)
	c.Assert(opened.Link("staged", "linked"), qt.ErrorIs, os.ErrExist)
	c.Assert(opened.MoveFileNoReplace("staged", "moved"), qt.IsNil)
	c.Assert(opened.Sync(), qt.IsNil)

	entries, err := opened.ReadDir()
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	c.Assert(names, qt.DeepEquals, []string{"linked", "moved"})
	contents, err := opened.ReadFile("moved")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "contents")
}

// TestOpenedDirectoryPublicationOperations_FollowTheRetainedDirectory pins that
// the publication operations keep acting on the directory the handle was opened
// on after a replacement takes over its pathname.
func TestOpenedDirectoryPublicationOperations_FollowTheRetainedDirectory(t *testing.T) {
	c := qt.New(t)
	parent := c.TempDir()
	dir := filepath.Join(parent, "retained")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "staged"), []byte("contents"), 0o600), qt.IsNil)
	opened := openTestDirectory(c, dir)
	moved := filepath.Join(parent, "moved")
	c.Assert(os.Rename(dir, moved), qt.IsNil)
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)

	c.Assert(opened.Link("staged", "linked"), qt.IsNil)
	c.Assert(opened.MoveFileNoReplace("staged", "published"), qt.IsNil)

	entries, err := opened.ReadDir()
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
	replacementEntries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(replacementEntries, qt.HasLen, 0)
	contents, err := os.ReadFile(filepath.Join(moved, "published"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "contents")
}
