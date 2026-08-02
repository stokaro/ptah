//go:build !windows

package pathguard_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/pathguard"
)

func TestOpenedDirectoryReplaceFile_FailurePath(t *testing.T) {
	c := qt.New(t)
	opened, err := pathguard.OpenDirectory(c.TempDir())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	err = opened.ReplaceFile("missing", "published")

	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	var linkErr *os.LinkError
	c.Assert(err, qt.ErrorAs, &linkErr)
	c.Assert(linkErr.Op, qt.Equals, "renameat")
	c.Assert(linkErr.Old, qt.Equals, "missing")
	c.Assert(linkErr.New, qt.Equals, "published")
}
