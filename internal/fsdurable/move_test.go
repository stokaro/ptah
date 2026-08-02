package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/fsdurable"
)

func TestMoveFileNoReplace_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	oldPath := filepath.Join(dir, "staged")
	newPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(oldPath, []byte("contents"), 0o600), qt.IsNil)

	err := fsdurable.MoveFileNoReplace(oldPath, newPath)

	c.Assert(err, qt.IsNil)
	_, err = os.Stat(oldPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(newPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "contents")
}

func TestMoveFileNoReplace_RefusesExistingDestination(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	oldPath := filepath.Join(dir, "staged")
	newPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(oldPath, []byte("new"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(newPath, []byte("existing"), 0o600), qt.IsNil)

	err := fsdurable.MoveFileNoReplace(oldPath, newPath)

	c.Assert(err, qt.ErrorIs, os.ErrExist)
	oldContents, readErr := os.ReadFile(oldPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(oldContents), qt.Equals, "new")
	newContents, readErr := os.ReadFile(newPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(newContents), qt.Equals, "existing")
}
