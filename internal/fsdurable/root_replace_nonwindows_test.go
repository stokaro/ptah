//go:build !windows

package fsdurable_test

import (
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
	var linkErr *os.LinkError
	c.Assert(err, qt.ErrorAs, &linkErr)
	c.Assert(linkErr.Op, qt.Equals, "renameat")
	c.Assert(linkErr.Old, qt.Equals, "missing")
	c.Assert(linkErr.New, qt.Equals, "published")
}
