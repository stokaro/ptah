//go:build !windows

package embedprovider_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedprovider"
)

// TestProbe_AWorldReadableCredentialFileIsAlsoNothingSent is the case
// stokaro/ptah#2641 measured on a live host, and the one with no sentinel of
// its own.
//
// It is behind a build tag rather than a runtime condition on
// FilePermissionsEnforced. A test that asked the constant and skipped would
// pass on Windows while asserting nothing, and a platform-conditional assertion
// that quietly becomes a tautology is worse than no assertion at all: the
// Windows half is its own file, where the refusal does not happen and the file
// resolves.
func TestProbe_AWorldReadableCredentialFileIsAlsoNothingSent(t *testing.T) {
	c := qt.New(t)
	c.Assert(embedprovider.FilePermissionsEnforced, qt.IsTrue,
		qt.Commentf("this file is built where permissions are enforced"))
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "token.txt")
	// #nosec G306 -- the loose mode is the fixture: the refusal is what is
	// being asserted.
	c.Assert(os.WriteFile(path, []byte("a-token"), 0o644), qt.IsNil)
	provider := providerWithCredential(c,
		embedprovider.CredentialRef{Scheme: "file", Locator: path})

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(report.Checks, qt.HasLen, 1)
	c.Assert(report.Checks[0].Name, qt.Equals, embedprovider.CheckAuthorized)
	c.Assert(report.Checks[0].Detail, qt.Contains, "readable beyond its owner")
}
