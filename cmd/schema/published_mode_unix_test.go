//go:build unix

package schema_test

import (
	"io/fs"

	qt "github.com/frankban/quicktest"
)

// assertPublishedMode requires the exact permissions a published artifact
// carries. Windows has no POSIX mode bits, so the check lives here; the
// portable half of the same claim -- that the file is readable at all -- is
// asserted in the test itself, on every platform.
func assertPublishedMode(c *qt.C, got fs.FileMode) {
	c.Helper()
	c.Assert(got.Perm(), qt.Equals, fs.FileMode(0o644))
}
