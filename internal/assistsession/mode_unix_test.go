//go:build !windows

package assistsession_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// The mode assertion lives in its own file because it is real on one platform
// and vacuous on the other. Windows synthesizes a mode from the read-only
// attribute and reported ----rwxrwx against the zero this asks for, which would
// otherwise have been "fixed" by deleting the assertion that works.

func TestCreate_KeepsTheFileToItsOwner(t *testing.T) {
	// The file holds the conversation and everything Ptah read on the model's
	// behalf. On a shared machine the mode is what stands between that and
	// everyone else.
	c := qt.New(t)
	store, root := newStore(c)

	write(c, store, "session-1", "hello", "hi")

	dir, err := os.Stat(filepath.Join(root, ".ptah", "sessions"))
	c.Assert(err, qt.IsNil)
	c.Assert(dir.Mode().Perm()&0o077, qt.Equals, os.FileMode(0))
	file, err := os.Stat(filepath.Join(root, ".ptah", "sessions", "session-1.jsonl"))
	c.Assert(err, qt.IsNil)
	c.Assert(file.Mode().Perm()&0o077, qt.Equals, os.FileMode(0))
}
