//go:build integration

package providercli_test

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
)

// applyWithSessionFacts applies a one-table YAML schema to a fresh fixture
// database of engine, through the session-capabilities fixture started with
// environment. The apply connects first and converts the schema after, so
// the fixture's creation projection sees the facts the host converted it
// with.
func applyWithSessionFacts(c *qt.C, engine string, environment []string) (stdout, stderr string, code int) {
	c.Helper()
	dir := c.TempDir()
	spec := fixtureSpec(c, "session-capabilities")
	spec.Environment = append(slices.Clone(spec.Environment), environment...)
	data, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: engine}}}}})
	c.Assert(err, qt.IsNil)
	manifest := filepath.Join(dir, "providers.json")
	c.Assert(os.WriteFile(manifest, data, 0o600), qt.IsNil)
	schema := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte("tables:\n  items:\n    columns:\n      id: { type: INTEGER, primary: true }\n"), 0o600), qt.IsNil)
	address := (&url.URL{Scheme: engine, Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(dir, "app.db")), "/")}).String()
	return executeProviderCLI(c.Context(), []string{"schema", "apply", "--provider-config", manifest, "--db-url", address,
		"--schema-file", schema, "--tx-mode", "none", "--auto-approve"})
}

// A provider that states no capabilities to plan its engine with offline
// plans a command that connects with the capabilities its session states:
// the apply converts its source after it connects, and the fixture's creation
// projection receives transactional_ddl as the session established it. The
// fixture plans no change, so the apply stops after the conversion.
func TestProviderConvertsWithTheSessionsCapabilities_HappyPath(t *testing.T) {
	c := qt.New(t)
	projected := filepath.Join(c.TempDir(), "projected.log")

	_, diagnostics, _ := applyWithSessionFacts(c, "fixture-session-facts", []string{"FIXTURE_PROJECTION_LOG=" + projected})

	c.Assert(diagnostics, qt.Not(qt.Contains), "provider project table creations failed")
	log, err := os.ReadFile(projected)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", diagnostics))
	c.Assert(string(log), qt.Equals, "transactional_ddl=false\n")
}

// A session that states no capability establishes nothing, and the provider
// refuses the projection, as it refuses one against unknown facts.
func TestProviderConvertsWithTheSessionsCapabilities_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, diagnostics, code := applyWithSessionFacts(c, "fixture-session-silent", []string{"FIXTURE_SESSION_SILENT=1"})

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "provider project table creations failed (FailedPrecondition)")
}
