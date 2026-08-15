//go:build unix

package goannotationexport_test

import (
	"io/fs"

	qt "github.com/frankban/quicktest"
)

// assertOwnerOnly requires that nobody but the owner can read the file.
//
// It is separate from assertFileMode because the two ask different questions.
// assertFileMode asks whether a mode survived a write, and its Windows arm
// compares only the write bit because that is all Windows represents. This one
// asks whether a secret is restricted, and comparing write bits would answer
// 0o666&0o200 == 0o600&0o200 -- true, while asserting the opposite of the
// property under test.
func assertOwnerOnly(c *qt.C, got fs.FileMode) {
	c.Helper()
	c.Assert(got.Perm(), qt.Equals, fs.FileMode(0o600))
}
