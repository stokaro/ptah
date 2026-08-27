//go:build windows

package embedprovider_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedprovider"
)

// TestCredentialRef_PermissionsAreNotEnforcedOnWindows pins what this platform
// can and cannot answer.
//
// Windows does not carry POSIX permission bits: what Go reports for a file on
// it is a fixed 0666, derived from the read-only attribute rather than from any
// access control. A check over those bits refuses every credential file on the
// platform -- which is what it did, until CI ran the suite on Windows -- while
// measuring nothing about who can read it.
//
// So the file resolves, and FilePermissionsEnforced says the check was not
// made. That pairing is the point: an unenforceable check that stayed silent
// would read, in a run record, exactly like one that passed.
func TestCredentialRef_PermissionsAreNotEnforcedOnWindows(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "token")
	c.Assert(os.WriteFile(path, []byte("the-value"), 0o644), qt.IsNil) // #nosec G306 -- the fixture is about the mode
	reference, err := embedprovider.ParseCredentialRef("file:" + path)
	c.Assert(err, qt.IsNil)

	value, err := reference.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(value, qt.Equals, "the-value")
	c.Assert(embedprovider.FilePermissionsEnforced, qt.IsFalse)
}
