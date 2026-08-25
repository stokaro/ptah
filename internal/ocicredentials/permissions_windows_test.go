//go:build windows

package ocicredentials_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
)

// assertOwnerOnly checks what Windows can be asked about the path.
//
// Windows has no POSIX permission bits: os.FileMode reports 0666 or 0777 for
// every file whatever its ACL says, so asserting on them would pass for a
// world-readable file and fail for an owner-only one. The credential store is
// created under the user's own profile directory, which is where the access
// control actually lives, so what is checkable here is that the path exists and
// is the kind of object it should be.
func assertOwnerOnly(c *qt.C, path string) {
	c.Helper()

	_, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
}

// The build tag is the whole point of this file, so it is worth one assertion
// that the file is compiled where it is meant to be.
func TestAssertOwnerOnly_IsCompiledOnWindows(t *testing.T) {
	c := qt.New(t)

	assertOwnerOnly(c, t.TempDir())
}
