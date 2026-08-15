//go:build !windows

package exeext

// White-box testing required: the same reason as the Windows half -- the
// build-tagged Suffix constant is the subject, not the exported behavior.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSuffixAndWiring_OffWindows pins the other half: everywhere else a name is
// a name, and a file that happens to be called atlas.exe keeps that name.
func TestSuffixAndWiring_OffWindows(t *testing.T) {
	c := qt.New(t)

	c.Assert(Suffix, qt.Equals, "")
	c.Assert(TrimmedBase("/usr/local/bin/atlas.exe"), qt.Equals, "atlas.exe")
}
