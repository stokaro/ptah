package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

func openTestRoot(c *qt.C, dir string) *os.Root {
	c.Helper()
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})
	return root
}

func TestMoveFileNoReplaceAt_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "staged"), []byte("contents"), 0o600), qt.IsNil)
	root := openTestRoot(c, dir)

	err := fsdurable.MoveFileNoReplaceAt(root, "staged", "published")

	c.Assert(err, qt.IsNil)
	_, err = os.Stat(filepath.Join(dir, "staged"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(filepath.Join(dir, "published"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "contents")
}

func TestMoveFileNoReplaceAt_RefusesExistingDestination(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "staged"), []byte("new"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "published"), []byte("existing"), 0o600), qt.IsNil)
	root := openTestRoot(c, dir)

	err := fsdurable.MoveFileNoReplaceAt(root, "staged", "published")

	c.Assert(err, qt.ErrorIs, os.ErrExist)
	staged, readErr := os.ReadFile(filepath.Join(dir, "staged"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(staged), qt.Equals, "new")
	published, readErr := os.ReadFile(filepath.Join(dir, "published"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(published), qt.Equals, "existing")
}

// TestMoveFileNoReplaceAt_FollowsTheRetainedDirectory pins the property the
// rooted variant exists for: after the directory is renamed away from the
// pathname it was opened on, the move still lands inside the directory the
// handle refers to.
func TestMoveFileNoReplaceAt_FollowsTheRetainedDirectory(t *testing.T) {
	c := qt.New(t)
	parent := c.TempDir()
	dir := filepath.Join(parent, "retained")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "staged"), []byte("contents"), 0o600), qt.IsNil)
	root := openTestRoot(c, dir)
	moved := filepath.Join(parent, "moved")
	c.Assert(os.Rename(dir, moved), qt.IsNil)
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)

	c.Assert(fsdurable.MoveFileNoReplaceAt(root, "staged", "published"), qt.IsNil)

	contents, err := os.ReadFile(filepath.Join(moved, "published"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "contents")
	_, err = os.Stat(filepath.Join(dir, "published"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
