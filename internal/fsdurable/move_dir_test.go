package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

func TestMoveDirNoReplace_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	oldPath := filepath.Join(dir, "staged")
	newPath := filepath.Join(dir, "published")
	c.Assert(os.Mkdir(oldPath, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(oldPath, "file.sql"), []byte("contents"), 0o600), qt.IsNil)

	err := fsdurable.MoveDirNoReplace(oldPath, newPath)

	c.Assert(err, qt.IsNil)
	_, err = os.Stat(oldPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(filepath.Join(newPath, "file.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "contents")
}

func TestMoveDirNoReplace_RefusesExistingFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	oldPath := stagedDirectory(c, dir)
	newPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(newPath, []byte("existing"), 0o600), qt.IsNil)

	err := fsdurable.MoveDirNoReplace(oldPath, newPath)

	c.Assert(err, qt.ErrorIs, os.ErrExist)
	contents, readErr := os.ReadFile(newPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "existing")
}

// An empty directory is the destination a move is likeliest to take without
// complaint, because it is the one a plain rename is allowed to replace.
func TestMoveDirNoReplace_RefusesExistingEmptyDirectory(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	oldPath := stagedDirectory(c, dir)
	newPath := filepath.Join(dir, "published")
	c.Assert(os.Mkdir(newPath, 0o755), qt.IsNil)

	err := fsdurable.MoveDirNoReplace(oldPath, newPath)

	c.Assert(err, qt.ErrorIs, os.ErrExist)
	entries, readErr := os.ReadDir(newPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
	_, statErr := os.Stat(filepath.Join(oldPath, "file.sql"))
	c.Assert(statErr, qt.IsNil)
}

// The conditional rename resolves a relative path against a directory the
// caller did not name, so a relative path is refused rather than resolved
// against whichever directory the implementation happens to hold.
func TestMoveDirNoReplace_RefusesRelativePaths(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	oldPath := stagedDirectory(c, dir)

	err := fsdurable.MoveDirNoReplace(oldPath, "published")

	c.Assert(err, qt.ErrorMatches, `move directory without replacing: absolute paths required.*`)
	_, statErr := os.Stat(oldPath)
	c.Assert(statErr, qt.IsNil)
}

func stagedDirectory(c *qt.C, parent string) string {
	c.Helper()
	staged := filepath.Join(parent, "staged")
	c.Assert(os.Mkdir(staged, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(staged, "file.sql"), []byte("new"), 0o600), qt.IsNil)
	return staged
}
