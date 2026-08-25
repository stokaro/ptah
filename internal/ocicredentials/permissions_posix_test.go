//go:build !windows

package ocicredentials_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
)

// assertOwnerOnly checks that nothing outside the owner can read the path.
//
// The permission bits are the mechanism on a POSIX system and are meaningless
// on Windows, where os.FileMode carries no group or other bits at all and
// access is decided by an ACL Go does not report here. The property is asserted
// where it can be asserted rather than watered down to something true
// everywhere, which would stop testing the thing that matters.
func assertOwnerOnly(c *qt.C, path string) {
	c.Helper()

	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm()&0o077, qt.Equals, os.FileMode(0),
		qt.Commentf("%s has mode %v", path, info.Mode().Perm()))
}

// The build tag is the whole point of this file, so it is worth one assertion
// that the file is compiled where it is meant to be.
func TestAssertOwnerOnly_IsCompiledOnPOSIX(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.Chmod(dir, 0o700), qt.IsNil)

	assertOwnerOnly(c, dir)
}
