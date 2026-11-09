//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
	"ptah.run/provider-sdk/provider"
)

// The error-reasons fixture refuses to inspect a database opened with the
// fault "refuse". Its reason is the fixture's own sentence, and its status
// message carries a fake credential, which no host prints.
const (
	fixtureReason     = "the fixture refuses to read this database"
	fixtureCredential = "hunter2"
)

// errorReasonsCLI selects the error-reasons fixture, started with the given
// environment, on a database it refuses to inspect.
func errorReasonsCLI(c *qt.C, environment ...string) providerCLI {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "error-reasons")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	f := newProviderCLI(c, providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture", Environment: environment})
	f.address += "?fault=refuse"
	return f
}

// checkedFeatures returns the features `providers check` reports for the
// fixture.
func checkedFeatures(c *qt.C, f providerCLI) []string {
	c.Helper()
	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"providers", "check", "--provider-config", f.manifestPath, "--provider-engine", "fixture", "--json"})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	var report struct {
		Provider struct {
			Features []string `json:"features"`
		} `json:"provider"`
	}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	return report.Provider.Features
}

// planError returns the error message of a failed `schema plan --json`.
func planError(c *qt.C, out string) string {
	c.Helper()
	var document struct {
		Outcome string `json:"outcome"`
		Error   struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	c.Assert(json.Unmarshal([]byte(out), &document), qt.IsNil, qt.Commentf("%s", out))
	c.Assert(document.Outcome, qt.Equals, "failed")
	return document.Error.Message
}

// A provider that lists errors.reasons says why it refused, and the host
// prints the reason after the code, on standard error and in the plan
// document. The status message stays out of both.
func TestProviderErrorReasons_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := errorReasonsCLI(c)
	c.Assert(checkedFeatures(c, f), qt.Contains, provider.ErrorReasonsFeature)

	_, inspected, inspectCode := f.run(c, c.Context(), "inspect", "--format", "json")
	out, planned, planCode := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

	c.Assert(inspectCode, qt.Equals, 2)
	c.Assert(inspected, qt.Contains, "provider inspect failed (FailedPrecondition): "+fixtureReason)
	c.Assert(inspected, qt.Not(qt.Contains), fixtureCredential)
	c.Assert(planCode, qt.Equals, 2)
	c.Assert(planError(c, out), qt.Contains, "provider inspect failed (FailedPrecondition): "+fixtureReason)
	c.Assert(out+planned, qt.Not(qt.Contains), fixtureCredential)
}

// A provider that does not list errors.reasons reaches the host as the code
// alone, though it sends the same reason: the SDK server removes it, and the
// host would not print it.
func TestProviderErrorReasons_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := errorReasonsCLI(c, "PTAH_FIXTURE_OMITTED="+provider.ErrorReasonsFeature)
	c.Assert(checkedFeatures(c, f), qt.Not(qt.Contains), provider.ErrorReasonsFeature)

	_, inspected, inspectCode := f.run(c, c.Context(), "inspect", "--format", "json")
	out, planned, planCode := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

	c.Assert(inspectCode, qt.Equals, 2)
	c.Assert(inspected, qt.Contains, "provider inspect failed (FailedPrecondition)")
	c.Assert(inspected, qt.Not(qt.Contains), fixtureReason)
	c.Assert(inspected, qt.Not(qt.Contains), fixtureCredential)
	c.Assert(planCode, qt.Equals, 2)
	c.Assert(planError(c, out), qt.Matches, `.*provider inspect failed \(FailedPrecondition\)`)
	c.Assert(out+planned, qt.Not(qt.Contains), fixtureReason)
	c.Assert(out+planned, qt.Not(qt.Contains), fixtureCredential)
}
