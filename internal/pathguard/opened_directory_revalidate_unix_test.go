//go:build unix

package pathguard_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/pathguard"
)

func TestOpenedDirectoryRevalidate_FailurePath_RejectsAncestorReplacement(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	selected := filepath.Join(root, "selected")
	captured := filepath.Join(root, "captured")
	outside := c.TempDir()
	c.Assert(os.Mkdir(selected, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(selected, "inside"), []byte("captured"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(outside, "inside"), []byte("outside"), 0o600), qt.IsNil)
	opened, err := pathguard.OpenDirectory(selected)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(opened.Close(), qt.IsNil)
	})
	c.Assert(os.Rename(selected, captured), qt.IsNil)
	c.Assert(os.Symlink(outside, selected), qt.IsNil)

	err = opened.Revalidate()

	c.Assert(err, qt.ErrorIs, pathguard.ErrDirectoryChanged)
	contents, err := opened.ReadFile("inside")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "captured")
	outsideContents, err := os.ReadFile(filepath.Join(selected, "inside"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(outsideContents), qt.Equals, "outside")
}
