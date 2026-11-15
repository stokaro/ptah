package devdocker_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/replaytrust"
)

// TestResolve_TrustRecordsTheURLItHandsBack pins the seam the replay guard
// reads trust through: a context that trusts the SQL records the URL Resolve
// hands back, for a named server and for a server Resolve provisions, until
// the release runs. For a docker URL that is the connectable URL, not the one
// the operator wrote, since the guard reads the URL the connection opened.
func TestResolve_TrustRecordsTheURLItHandsBack(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
	}{
		{"a named server", "postgres://dev@trust-named:5432/app"},
		{"a provisioned server", "docker://postgres/16/trust_provisioned"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runner := &fakeRunner{hostPort: "127.0.0.1:15433"}
			ctx := replaytrust.With(t.Context(), true)

			resolved, release, err := devdocker.Resolve(ctx, test.rawURL, devdocker.Options{Runner: runner, Ready: alwaysReady})
			c.Assert(err, qt.IsNil)
			c.Assert(replaytrust.Trusted(resolved), qt.IsTrue)

			release()
			c.Assert(replaytrust.Trusted(resolved), qt.IsFalse)
		})
	}
}

// TestResolve_TrustIsNotOwnership pins the two answers apart: a named server
// whose SQL the run trusts stays a server the run does not own, and a server
// declared disposable is owned without being trusted.
func TestResolve_TrustIsNotOwnership(t *testing.T) {
	c := qt.New(t)
	trusted, releaseTrusted, err := devdocker.Resolve(replaytrust.With(t.Context(), true),
		"postgres://dev@trust-not-owned:5432/app", devdocker.Options{})
	c.Assert(err, qt.IsNil)
	t.Cleanup(releaseTrusted)
	owned, releaseOwned, err := devdocker.Resolve(t.Context(),
		"postgres://dev@owned-not-trusted:5432/app", devdocker.Options{DeclaredDisposable: true})
	c.Assert(err, qt.IsNil)
	t.Cleanup(releaseOwned)

	c.Assert(devdocker.RunOwned(trusted), qt.IsFalse)
	c.Assert(replaytrust.Trusted(trusted), qt.IsTrue)
	c.Assert(devdocker.RunOwned(owned), qt.IsTrue)
	c.Assert(replaytrust.Trusted(owned), qt.IsFalse)
}

// TestResolve_WithoutTrustRecordsNothing is the control for the tests above: a
// context that carries no answer, or a false one, records no trust.
func TestResolve_WithoutTrustRecordsNothing(t *testing.T) {
	tests := []struct {
		name    string
		trusted []bool
	}{
		{"no answer", nil},
		{"not trusted", []bool{false}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := t.Context()
			for _, trusted := range test.trusted {
				ctx = replaytrust.With(ctx, trusted)
			}
			resolved, release, err := devdocker.Resolve(ctx, "postgres://dev@trust-absent:5432/app", devdocker.Options{})
			c.Assert(err, qt.IsNil)
			t.Cleanup(release)
			c.Assert(replaytrust.Trusted(resolved), qt.IsFalse)
		})
	}
}
