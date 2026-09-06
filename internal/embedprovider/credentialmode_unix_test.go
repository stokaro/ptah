//go:build !windows

package embedprovider_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedprovider"
)

// TestCredentialRef_AFileReadableByOthersIsRefused is the check that stops Ptah
// being the reason a token leaks.
//
// A token in a world-readable file is a token every process on the host has.
// Reading it anyway would be Ptah treating a broken permission as acceptable,
// and the error names the fix rather than the rule.
func TestCredentialRef_AFileReadableByOthersIsRefused(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "token")
	c.Assert(os.WriteFile(path, []byte("the-value"), 0o644), qt.IsNil) // #nosec G306 -- a world-readable credential file is the fixture
	reference, err := embedprovider.ParseCredentialRef("file:" + path)
	c.Assert(err, qt.IsNil)

	_, err = reference.Resolve()

	c.Assert(err, qt.ErrorMatches, `.*readable beyond its owner; chmod 600 it.*`)
	c.Assert(embedprovider.FilePermissionsEnforced, qt.IsTrue)
}
