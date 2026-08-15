//go:build unix

package ociartifact_test

import (
	"io/fs"

	qt "github.com/frankban/quicktest"
)

// assertExtractedMode requires that a file pulled from a registry is not
// created group- or world-readable. extract.go opens each one with 0o600 for
// exactly that reason.
//
// The check is unix-only because Windows reports 0o666 for any normal file and
// 0o444 for a read-only one; a write-bit comparison would pass for 0o666 too,
// which is the opposite of what this asserts.
func assertExtractedMode(c *qt.C, got fs.FileMode) {
	c.Helper()
	c.Assert(got.Perm(), qt.Equals, fs.FileMode(0o600))
}
