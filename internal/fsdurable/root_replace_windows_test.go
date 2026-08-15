package fsdurable_test

import (
	"io/fs"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

func TestReplaceFileAt_FailurePath(t *testing.T) {
	c := qt.New(t)
	root, err := os.OpenRoot(c.TempDir())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(root.Close(), qt.IsNil)
	})

	err = fsdurable.ReplaceFileAt(root, "missing", "published")

	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	c.Assert(err, qt.Not(qt.ErrorIs), fsdurable.ErrReplacementCommitted)
	var pathErr *fs.PathError
	c.Assert(err, qt.ErrorAs, &pathErr)
	c.Assert(pathErr.Op, qt.Equals, "openat")
	c.Assert(pathErr.Path, qt.Equals, "missing")
}
