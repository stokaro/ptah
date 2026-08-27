package embedprovider_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedprovider"
)

// TestParseCredentialRef_ReadsWhatItSupportsAndRefusesTheRest pins the surface.
//
// An unsupported scheme is refused at parse rather than silently ignored: a
// reference nobody can resolve is a provider that authenticates with nothing,
// and the endpoint's 401 is a worse place to learn it.
func TestParseCredentialRef_ReadsWhatItSupportsAndRefusesTheRest(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantSet   bool
		wantValue string
		wantError bool
	}{
		{name: "an environment variable", raw: "env:PTAH_TOKEN", wantSet: true, wantValue: "env:PTAH_TOKEN"},
		{name: "a file", raw: "file:/etc/ptah/token", wantSet: true, wantValue: "file:/etc/ptah/token"},
		{name: "case is not significant in the scheme", raw: "ENV:PTAH_TOKEN", wantSet: true, wantValue: "env:PTAH_TOKEN"},
		{name: "absent, which a local endpoint may want", raw: "", wantSet: false},
		{name: "blank is absent", raw: "   ", wantSet: false},
		{name: "a scheme this build cannot resolve", raw: "vault:secret/ptah", wantError: true},
		{name: "no locator", raw: "env:", wantError: true},
		{name: "no scheme at all", raw: "PTAH_TOKEN", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reference, err := embedprovider.ParseCredentialRef(test.raw)

			c.Assert(err != nil, qt.Equals, test.wantError)
			c.Assert(reference.Set(), qt.Equals, test.wantSet)
			c.Assert(reference.String(), qt.Equals, test.wantValue)
		})
	}
}

// TestCredentialRef_ResolvesAtUseAndReturnsTheValue is the point of a
// reference: the secret is read when it is needed and is not carried around
// before that.
func TestCredentialRef_ResolvesAtUseAndReturnsTheValue(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_TEST_TOKEN", "the-value")
	reference, err := embedprovider.ParseCredentialRef("env:PTAH_TEST_TOKEN")
	c.Assert(err, qt.IsNil)

	value, err := reference.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(value, qt.Equals, "the-value")
	// And the reference itself still says only where to look.
	c.Assert(reference.String(), qt.Equals, "env:PTAH_TEST_TOKEN")
	c.Assert(reference.String(), qt.Not(qt.Contains), "the-value")
}

// TestCredentialRef_AnUnsetVariableIsItsOwnError separates "no credential was
// configured" from "the configured one is not there".
//
// The first is a local endpoint working as intended; the second is a
// misconfiguration whose symptom would otherwise be a 401 from a host the
// operator believed was authenticated.
func TestCredentialRef_AnUnsetVariableIsItsOwnError(t *testing.T) {
	c := qt.New(t)
	reference, err := embedprovider.ParseCredentialRef("env:PTAH_TEST_ABSENT")
	c.Assert(err, qt.IsNil)

	_, err = reference.Resolve()

	c.Assert(err, qt.ErrorIs, embedprovider.ErrCredentialUnset)
}

// TestCredentialRef_AnEmptyFileIsItsOwnError is the same distinction for the
// other scheme: a file that exists and holds nothing is not a credential.
func TestCredentialRef_AnEmptyFileIsItsOwnError(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "token")
	c.Assert(os.WriteFile(path, []byte("   \n"), 0o600), qt.IsNil)
	reference, err := embedprovider.ParseCredentialRef("file:" + path)
	c.Assert(err, qt.IsNil)

	_, err = reference.Resolve()

	c.Assert(err, qt.ErrorIs, embedprovider.ErrCredentialUnset)
}

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
}

// TestCredentialRef_AFileTheOwnerAloneCanReadIsAccepted is the control: a check
// that refused every file would satisfy the row above and make the scheme
// useless.
func TestCredentialRef_AFileTheOwnerAloneCanReadIsAccepted(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "token")
	c.Assert(os.WriteFile(path, []byte("  the-value\n"), 0o600), qt.IsNil)
	reference, err := embedprovider.ParseCredentialRef("file:" + path)
	c.Assert(err, qt.IsNil)

	value, err := reference.Resolve()

	c.Assert(err, qt.IsNil)
	// Trimmed, because a trailing newline is how an editor saves a token and
	// not part of it.
	c.Assert(value, qt.Equals, "the-value")
}

// TestCredentialRef_AbsentResolvesToNothingWithoutAnError is the local-endpoint
// case, which is a first-class architecture requirement rather than a
// degradation.
func TestCredentialRef_AbsentResolvesToNothingWithoutAnError(t *testing.T) {
	c := qt.New(t)

	value, err := embedprovider.CredentialRef{}.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(value, qt.Equals, "")
}
