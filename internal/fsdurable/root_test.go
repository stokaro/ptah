package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/fsdurable"
)

func TestReplaceFileAt_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "staged"), []byte("new"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "published"), []byte("old"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(root.Close(), qt.IsNil)
	})

	c.Assert(fsdurable.ReplaceFileAt(root, "staged", "published"), qt.IsNil)

	contents, err := os.ReadFile(filepath.Join(dir, "published"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "new")
	_, err = os.Stat(filepath.Join(dir, "staged"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestReplaceFileAt_FailurePath(t *testing.T) {
	c := qt.New(t)
	root, err := os.OpenRoot(c.TempDir())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(root.Close(), qt.IsNil)
	})

	err = fsdurable.ReplaceFileAt(root, "missing", "published")

	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	var linkErr *os.LinkError
	c.Assert(err, qt.ErrorAs, &linkErr)
	c.Assert(linkErr.Op, qt.Equals, "renameat")
	c.Assert(linkErr.Old, qt.Equals, "missing")
	c.Assert(linkErr.New, qt.Equals, "published")
}

func TestSyncRoot_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "entry"), []byte("value"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(root.Close(), qt.IsNil)
	})

	c.Assert(fsdurable.SyncRoot(root), qt.IsNil)
}
