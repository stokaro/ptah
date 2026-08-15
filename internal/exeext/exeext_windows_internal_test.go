//go:build windows

package exeext

// White-box testing required: this file asserts the value of the
// build-tagged Suffix constant, which no external caller can observe as a
// constant rather than through its effect.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSuffixAndWiring_OnWindows is the half of the contract only Windows can
// answer, which is why it is build-tagged rather than written against the
// constant from everywhere.
//
// A test that asserted TrimmedBase("atlas"+Suffix) == "atlas" on every platform
// would pass on Unix by trimming nothing, and would keep passing if TrimmedBase
// stopped reading Suffix at all. Here both halves are real: the constant is
// ".exe", and the exported entry point has to use it.
func TestSuffixAndWiring_OnWindows(t *testing.T) {
	c := qt.New(t)

	c.Assert(Suffix, qt.Equals, ".exe")
	c.Assert(TrimmedBase(`C:\Program Files\ptah\atlas.exe`), qt.Equals, "atlas")
}
