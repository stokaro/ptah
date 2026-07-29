package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/fsdurable"
)

func TestSyncDir_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "entry"), []byte("value"), 0o600), qt.IsNil)

	c.Assert(fsdurable.SyncDir(dir), qt.IsNil)
}

func TestSyncDir_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := fsdurable.SyncDir(filepath.Join(c.TempDir(), "missing"))

	c.Assert(err, qt.ErrorMatches, `open directory for sync: .*`)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
