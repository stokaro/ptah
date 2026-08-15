//go:build windows

package schema_test

import (
	"io/fs"

	qt "github.com/frankban/quicktest"
)

// assertPublishedMode checks nothing here. os.Stat synthesizes 0o666 for any
// normal file on Windows and 0o444 for a read-only one, so 0644 is not a value
// this platform can report, and comparing only the write bit would pass for
// 0600 as well -- the very distinction the assertion exists to draw.
func assertPublishedMode(*qt.C, fs.FileMode) {}
