//go:build unix

package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

func TestPublishFileAt_HappyPath_AppliesExactUnixMode(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(stagedPath, []byte("new"), 0o600), qt.IsNil)
	stagedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.PublishFileAt(root, "staged", "published", stagedInfo, 0o640)

	c.Assert(err, qt.IsNil)
	publishedInfo, err := os.Stat(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(publishedInfo.Mode().Perm(), qt.Equals, os.FileMode(0o640))
}

func TestFinalizeFileAt_HappyPath_AppliesExactUnixMode(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	c.Assert(os.WriteFile(stagedPath, []byte("backup"), 0o600), qt.IsNil)
	stagedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.FinalizeFileAt(root, "staged", stagedInfo, 0o640)

	c.Assert(err, qt.IsNil)
	finalInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(finalInfo.Mode().Perm(), qt.Equals, os.FileMode(0o640))
}
