//go:build windows

package fsdurable_test

// MoveDirNoReplace exists on this platform for one reason: os.Rename does not
// refuse an existing destination here, while it does on Unix. That premise is
// what the OCI install relied on and did not have (stokaro/ptah#1547), so it is
// pinned rather than left as a remark in a comment -- if a future Go release
// makes os.Rename refuse, this test says so, and the reason to keep a separate
// primitive is gone.
//
// It also answers the question the original failure left open. The test that
// caught this stopped at its first failed assertion, so nothing established
// whether the entry that appeared at the destination survived. This one reads
// it back.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestOSRenameDoesNotRefuseAnExistingDestination(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	staged := filepath.Join(dir, "staged")
	c.Assert(os.Mkdir(staged, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(staged, "file.sql"), []byte("new"), 0o600), qt.IsNil)
	destination := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(destination, []byte("keep"), 0o600), qt.IsNil)

	err := os.Rename(staged, destination)

	c.Assert(err, qt.IsNil)
	contents, readErr := os.ReadFile(filepath.Join(destination, "file.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "new")
}
