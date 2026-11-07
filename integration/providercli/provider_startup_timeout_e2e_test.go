//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// A provider that does not finish its handshake within the startup timeout is
// refused with a sentence naming the engine, the provider, its executable, the
// timeout and how long it ran, and saying what to try. The fixture sleeps
// longer than the timeout before it serves.
func TestProviderStartupTimeout_FailurePath(t *testing.T) {
	c := qt.New(t)
	binary := testutils.BuildFixtureProvider(c, "provider")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	f := newProviderCLI(c, providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture",
		Environment: []string{"PTAH_FIXTURE_STARTUP_DELAY=30s"}})

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"providers", "check", "--provider-config", f.manifestPath, "--provider-engine", "fixture", "--json"})

	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", diagnostics))
	var report struct {
		Status     string `json:"status"`
		Diagnostic struct {
			Code     string `json:"code"`
			EngineID string `json:"engine_id"`
			Message  string `json:"message"`
		} `json:"diagnostic"`
	}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
	c.Assert(report.Status, qt.Equals, "failed")
	c.Assert(report.Diagnostic.Code, qt.Equals, "provider_start_timeout")
	c.Assert(report.Diagnostic.EngineID, qt.Equals, "fixture")
	c.Assert(report.Diagnostic.Message, qt.Matches, `the fixture provider example\.org/fixture \(`+regexp.QuoteMeta(binary)+
		`\) did not finish its handshake within 10s; it ran for 1\d(\.\d)?s\. The first start of a new executable can take longer `+
		`while the system checks it, so run the command again, and if it fails again, check the executable with `+"`ptah providers check`")
}

// A provider that finishes its handshake in time starts as before: the delay
// alone, shorter than the timeout, is the only difference from the refusal.
func TestProviderStartupWithinTheTimeout_HappyPath(t *testing.T) {
	c := qt.New(t)
	binary := testutils.BuildFixtureProvider(c, "provider")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	f := newProviderCLI(c, providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture",
		Environment: []string{"PTAH_FIXTURE_STARTUP_DELAY=1s"}})

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"providers", "check", "--provider-config", f.manifestPath, "--provider-engine", "fixture", "--json"})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, `"status": "compatible"`)
}
