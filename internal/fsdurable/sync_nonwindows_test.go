//go:build !windows

package fsdurable_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

func TestSyncDir_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := fsdurable.SyncDir(filepath.Join(c.TempDir(), "missing"))

	c.Assert(err, qt.ErrorMatches, `open directory for sync: .*`)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestSyncRoot_FailurePath(t *testing.T) {
	c := qt.New(t)
	root, err := os.OpenRoot(c.TempDir())
	c.Assert(err, qt.IsNil)
	c.Assert(root.Close(), qt.IsNil)

	err = fsdurable.SyncRoot(root)

	c.Assert(err, qt.ErrorMatches, `open rooted directory for sync: .*`)
	c.Assert(err, qt.ErrorIs, os.ErrClosed)
	var pathErr *fs.PathError
	c.Assert(err, qt.ErrorAs, &pathErr)
	c.Assert(pathErr.Op, qt.Equals, "openat")
	c.Assert(pathErr.Path, qt.Equals, ".")
}
