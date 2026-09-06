package embedprovider_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedprovider"
)

// TestProbe_ACredentialThatNeverResolvedSaysNothingWasSent is
// stokaro/ptah#2641's headline.
//
// `Embed` resolves the credential before it builds the request, so a failure
// there returns before a byte leaves the process. The probe reported
// `ok reachable: the endpoint at HOST answered` and
// `ok authorized: the credential from env:X was accepted` anyway — for a port
// with nothing listening on it, and for a credential Ptah itself refused, with
// the contradiction printed on the line directly beneath.
//
// The rows are the ways a credential fails, and they are separate rows because
// each returns a different error and a fix keyed on one of them would leave the
// others reporting a dead endpoint as alive.
func TestProbe_ACredentialThatNeverResolvedSaysNothingWasSent(t *testing.T) {
	tests := []struct {
		name       string
		credential embedprovider.CredentialRef
		wantDetail string
	}{
		{
			name:       "an unset variable",
			credential: embedprovider.CredentialRef{Scheme: "env", Locator: "PTAH_2641_UNSET"},
			wantDetail: "is unset or empty",
		},
		{
			name:       "a scheme this build does not know",
			credential: embedprovider.CredentialRef{Scheme: "vault", Locator: "secret/token"},
			wantDetail: "unsupported credential reference scheme",
		},
		{
			name:       "a file that is not there",
			credential: embedprovider.CredentialRef{Scheme: "file", Locator: "/nonexistent/ptah-2641"},
			wantDetail: "read credential file",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := providerWithCredential(c, test.credential)

			report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
				Provider: provider, Dimension: 4, Absent: refusing(),
			})

			c.Assert(report.Passed(), qt.IsFalse)
			// One check, and it is the credential. Reachability is not among
			// them at all: the endpoint was never asked.
			c.Assert(report.Checks, qt.HasLen, 1)
			c.Assert(report.Checks[0].Name, qt.Equals, embedprovider.CheckAuthorized)
			c.Assert(report.Checks[0].Passed, qt.IsFalse)
			c.Assert(report.Checks[0].Detail, qt.Contains, test.wantDetail)
			c.Assert(namesOf(report.Checks), qt.Not(qt.Contains), embedprovider.CheckReachable)
			// And the report says reachability was not measured rather than
			// leaving it out in silence.
			c.Assert(report.Unmeasured, qt.HasLen, 1)
			c.Assert(report.Unmeasured[0], qt.Contains, "no request was sent")
		})
	}
}

// TestProbe_ACredentialThatDoesResolveReachesTheEndpointQuestion is the control
// for the tests above, and the discrimination the fix is actually about.
//
// A probe that reported "the credential could not be used" for every
// specification would satisfy every assertion above while telling an operator
// nothing. With a credential that resolves, the probe gets past it and answers
// about the endpoint — and here the endpoint is a port nothing listens on, so
// the answer is a FAILED reachability check rather than the passed one the
// defect printed for exactly this case.
func TestProbe_ACredentialThatDoesResolveReachesTheEndpointQuestion(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "token.txt")
	c.Assert(os.WriteFile(path, []byte("a-token"), 0o600), qt.IsNil)
	provider := providerWithCredential(c,
		embedprovider.CredentialRef{Scheme: "file", Locator: path})

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Checks, qt.HasLen, 1)
	c.Assert(report.Checks[0].Name, qt.Equals, embedprovider.CheckReachable)
	c.Assert(report.Checks[0].Passed, qt.IsFalse,
		qt.Commentf("nothing listens on the fixture's port; a pass here is the defect"))
}

// providerWithCredential is the real provider, holding a reference that will
// not resolve.
//
// The real one rather than a fake: what the probe now classifies is the error
// Embed returns, and Embed's habit of resolving the credential BEFORE it builds
// a request is the whole reason nothing was sent. A fake returning a
// hand-written error would agree with a provider that had stopped doing that.
//
// The endpoint names a port nothing listens on, so a probe that got as far as
// dialing would answer with a transport error instead — which is the
// non-vacuity this needs.
func providerWithCredential(c *qt.C, credential embedprovider.CredentialRef) embedprovider.Provider {
	c.Helper()
	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name: "local", BaseURL: "http://127.0.0.1:9/v1", Model: "test-embed",
		Credential: credential, EndpointClass: "local", Dimension: 4,
		Timeout: 2 * time.Second,
	})
	c.Assert(err, qt.IsNil)
	return provider
}

// namesOf is the check names a report carries.
func namesOf(checks []embedprovider.Check) []embedprovider.CheckName {
	names := make([]embedprovider.CheckName, 0, len(checks))
	for _, check := range checks {
		names = append(names, check.Name)
	}
	return names
}

// TestResolve_TheMarkerDoesNotChangeTheMessage holds what the wrapper promises.
//
// These strings tell an operator which variable to export and which file to
// chmod, and they are what the probe prints. Marking the error so a caller can
// ask "was anything sent?" must not prefix them with a category that says the
// same thing twice — and a comment claiming so is not a thing that holds
// (stokaro/ptah#2641).
func TestResolve_TheMarkerDoesNotChangeTheMessage(t *testing.T) {
	tests := []struct {
		name       string
		credential embedprovider.CredentialRef
		want       string
	}{
		{
			name:       "an unset variable",
			credential: embedprovider.CredentialRef{Scheme: "env", Locator: "PTAH_2641_UNSET"},
			want: "credential reference resolves to nothing: " +
				"environment variable PTAH_2641_UNSET is unset or empty",
		},
		{
			name:       "a scheme this build does not know",
			credential: embedprovider.CredentialRef{Scheme: "vault", Locator: "secret/token"},
			want:       `unsupported credential reference scheme: "vault"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := test.credential.Resolve()

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.want)
			// And the specific sentinel is still reachable through it, so the
			// marker adds an answer rather than replacing one.
			c.Assert(err, qt.ErrorIs, embedprovider.ErrCredentialUnusable)
		})
	}
}

// TestResolve_TheSpecificSentinelsSurviveTheMarker is the other half.
//
// A marker that swallowed the sentinel underneath would make every credential
// failure look alike to a caller that branches on them.
func TestResolve_TheSpecificSentinelsSurviveTheMarker(t *testing.T) {
	c := qt.New(t)

	_, unset := embedprovider.CredentialRef{Scheme: "env", Locator: "PTAH_2641_UNSET"}.Resolve()
	_, scheme := embedprovider.CredentialRef{Scheme: "vault", Locator: "x"}.Resolve()

	c.Assert(unset, qt.ErrorIs, embedprovider.ErrCredentialUnset)
	c.Assert(unset, qt.Not(qt.ErrorIs), embedprovider.ErrCredentialScheme)
	c.Assert(scheme, qt.ErrorIs, embedprovider.ErrCredentialScheme)
	c.Assert(scheme, qt.Not(qt.ErrorIs), embedprovider.ErrCredentialUnset)
}
