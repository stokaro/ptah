package pathguard_test

import (
	"errors"
	"io/fs"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/fsdurable"
	"github.com/stokaro/ptah/internal/pathguard"
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
	c.Assert(errors.Is(err, fsdurable.ErrReplacementCommitted), qt.IsFalse)
	var pathErr *fs.PathError
	c.Assert(err, qt.ErrorAs, &pathErr)
	c.Assert(pathErr.Op, qt.Equals, "openat")
	c.Assert(pathErr.Path, qt.Equals, "missing")
}
