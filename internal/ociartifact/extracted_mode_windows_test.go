//go:build windows

package ociartifact_test

import (
	"io/fs"

	qt "github.com/frankban/quicktest"
)

// assertExtractedMode checks nothing here, and the empty body is the point:
// "not readable by anyone else" is not a property Windows represents, so this
// test does not verify it there rather than pretending to.
func assertExtractedMode(*qt.C, fs.FileMode) {}
