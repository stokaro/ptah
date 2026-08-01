//go:build windows

package goannotationcleanup_test

import (
	"io/fs"

	qt "github.com/frankban/quicktest"
)

func assertFileMode(c *qt.C, got, want fs.FileMode) {
	c.Helper()
	c.Assert(got.Perm()&0o200, qt.Equals, want.Perm()&0o200)
}
