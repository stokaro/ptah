//go:build unix

package goannotationexport_test

import (
	"io/fs"

	qt "github.com/frankban/quicktest"
)

func assertFileMode(c *qt.C, got, want fs.FileMode) {
	c.Helper()
	c.Assert(got.Perm(), qt.Equals, want.Perm())
}
