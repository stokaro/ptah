package pathguard_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/pathguard"
)

func TestOpenedDirectoryRevalidate_FailurePath_RejectsDirectoryPathReplacement(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	selected := filepath.Join(root, "selected")
	captured := filepath.Join(root, "captured")
	c.Assert(os.Mkdir(captured, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(captured, "inside"), []byte("captured"), 0o600), qt.IsNil)
	c.Assert(os.Symlink(captured, selected), qt.IsNil)
	opened, err := pathguard.OpenDirectory(selected)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(opened.Close(), qt.IsNil)
	})
	c.Assert(os.Remove(selected), qt.IsNil)
	c.Assert(os.Mkdir(selected, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(selected, "inside"), []byte("replacement"), 0o600), qt.IsNil)

	err = opened.Revalidate()

	c.Assert(err, qt.ErrorIs, pathguard.ErrDirectoryChanged)
	contents, err := opened.ReadFile("inside")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "captured")
	replacementContents, err := os.ReadFile(filepath.Join(selected, "inside"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(replacementContents), qt.Equals, "replacement")
}
