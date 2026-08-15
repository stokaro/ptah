//go:build windows

package goannotationexport_test

import (
	"io/fs"

	qt "github.com/frankban/quicktest"
)

// assertOwnerOnly checks nothing here, deliberately and visibly.
//
// Windows has no POSIX mode bits: os.Stat synthesizes 0o666 for a normal file
// and 0o444 for a read-only one, and nothing else is representable. So
// "readable by the owner alone" cannot be observed, and it cannot be imposed
// either -- os.Chmod on a normal file only moves the read-only attribute.
//
// The previous spelling compared write bits, which passed by asserting the
// credential file IS writable: the exact opposite of the property, green, and
// silent about it. An empty body with this comment is worth more, because the
// next reader learns that the restriction is unverified on this platform
// rather than believing a check ran.
func assertOwnerOnly(*qt.C, fs.FileMode) {}
