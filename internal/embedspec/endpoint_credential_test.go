package embedspec_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedspec"
)

// withEndpoint is the complete specification with one endpoint substituted.
func withEndpoint(endpoint string) []byte {
	return []byte(strings.Replace(
		complete, "endpoint: http://localhost:11434/v1", "endpoint: "+endpoint, 1))
}

// TestParse_RefusesACredentialInTheEndpoint is stokaro/ptah#2644.
//
// `model.credential` refuses a literal because a key must not appear in project
// configuration at all. `model.endpoint` had no such check, and Go's http
// client turns userinfo into `Authorization: Basic …` on every provider
// request — so the same value reached the same wire through the field nobody
// was checking, and then the terminal output of `plan` and the
// specification.yaml layer of a published release.
func TestParse_RefusesACredentialInTheEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{
			// The measured case: a service account and a token.
			// #nosec G101 -- the credential in this URL is the fixture: the
			// test exists because such a URL was accepted, and a rule that
			// forbids writing one here would forbid asserting it is refused.
			name:     "user and password",
			endpoint: "http://svc:s3cr3t-token@127.0.0.1:18105/v1",
		},
		{
			// A bare username is credential material too: Go sends it as
			// `Basic dXNlcjo=`, which is the same header with an empty half.
			name:     "user alone",
			endpoint: "https://user@api.example.com/v1",
		},
		{
			// An empty password is spelled differently and reaches the same
			// header, so the check cannot key on the password being present.
			name:     "user and empty password",
			endpoint: "https://user:@api.example.com/v1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := embedspec.Parse(withEndpoint(test.endpoint), "spec.yaml")

			c.Assert(err, qt.ErrorMatches,
				`spec.yaml: model.endpoint carries a credential in its userinfo.*`)
		})
	}
}

// TestParse_TheRefusalDoesNotPrintTheCredential is the half that is easy to get
// wrong while fixing the other one.
//
// An error that quoted the URL back would write the token to the terminal and
// into whatever collects the log. That is the exposure this refusal exists to
// end, not a report of it, so the message names the field and the host alone.
func TestParse_TheRefusalDoesNotPrintTheCredential(t *testing.T) {
	c := qt.New(t)

	_, err := embedspec.Parse(
		withEndpoint("http://svc:s3cr3t-token@127.0.0.1:18105/v1"), "spec.yaml")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "s3cr3t-token")
	c.Assert(err.Error(), qt.Not(qt.Contains), "svc")
	// Non-vacuity: the message does identify which endpoint, by its host.
	c.Assert(err.Error(), qt.Contains, "127.0.0.1:18105")
}

// TestParse_AcceptsAnEndpointWithNoCredential is the control.
//
// Without it the refusal could be satisfied by rejecting every endpoint, and
// the local-endpoint case the documentation calls a complete configuration —
// "an endpoint on localhost with no credential" — is the one that must keep
// working.
func TestParse_AcceptsAnEndpointWithNoCredential(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "a local endpoint", endpoint: "http://localhost:11434/v1"},
		{name: "a hosted endpoint", endpoint: "https://api.example.com/v1"},
		{
			// An `@` inside the path or the query is not userinfo, and a check
			// that scanned for the character rather than parsing would refuse
			// this.
			name:     "an at sign past the host",
			endpoint: "https://api.example.com/v1/models/text@2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			loaded, err := embedspec.Parse(withEndpoint(test.endpoint), "spec.yaml")

			c.Assert(err, qt.IsNil)
			c.Assert(loaded.Endpoint, qt.Equals, test.endpoint)
		})
	}
}

// TestParse_RefusesAnEndpointItCannotRead states the fail-closed half.
//
// The check cannot establish that an endpoint it cannot parse carries no
// credential, and an endpoint no URL parser accepts has no request to reach
// anyway, so it is refused here rather than left to fail later for a reason
// that says nothing about the credential.
func TestParse_RefusesAnEndpointItCannotRead(t *testing.T) {
	c := qt.New(t)

	_, err := embedspec.Parse(withEndpoint(`"http://%zz@host/v1"`), "spec.yaml")

	c.Assert(err, qt.ErrorMatches, `spec.yaml: model.endpoint is not a URL.*`)
}
