//go:build windows

package testutils

import "testing"

// SkipWithoutPOSIXShell skips a test whose fixture is a /bin/sh script.
//
// The behavior these tests exercise is portable -- Ptah spawns whatever the
// environment names, and Windows runs a .bat or an .exe the same way. The
// fixture is what is not: several of them take an arbitrary shell body from
// the test, and a batch translation of that would be a second program to keep
// in step with the first, in a language none of these tests are about.
//
// Skipping says so out loud. The alternative is a Windows job that stays red
// for something that is not a defect, which teaches everyone to ignore it.
func SkipWithoutPOSIXShell(tb testing.TB) {
	tb.Helper()
	tb.Skip("fixture is a /bin/sh script; the program spawn it exercises is portable, this fixture is not")
}
