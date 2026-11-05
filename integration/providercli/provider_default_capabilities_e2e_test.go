//go:build integration

package providercli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
)

// renderDefaults renders a one-table YAML schema offline on engine, through
// the default-capabilities fixture started with environment.
func renderDefaults(c *qt.C, engine string, environment ...string) (stdout, stderr string, code int) {
	c.Helper()
	dir := c.TempDir()
	spec := fixtureSpec(c, "default-capabilities")
	spec.Environment = append(slices.Clone(spec.Environment), environment...)
	data, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: engine}}}}})
	c.Assert(err, qt.IsNil)
	manifest := filepath.Join(dir, "providers.json")
	c.Assert(os.WriteFile(manifest, data, 0o600), qt.IsNil)
	schema := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte("tables:\n  items:\n    columns:\n      id: { type: INTEGER, primary: true }\n"), 0o600), qt.IsNil)
	return executeProviderCLI(c.Context(), []string{"schema", "render", "--provider-config", manifest, "--dialect", engine, "--schema-file", schema})
}

// An offline render plans the engine with the capabilities its provider
// states for it: the fixture renders only against an established
// transactional_ddl, and the stated facts establish it.
func TestProviderRendersWithTheEnginesStatedCapabilities_HappyPath(t *testing.T) {
	c := qt.New(t)

	out, diagnostics, code := renderDefaults(c, "fixture-defaults")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "-- transactional_ddl=false\nCREATE TABLE \"items\" (id INTEGER);")
}

// Without the stated facts no capability is established offline, and the
// provider refuses the render, as it refuses one against unknown facts. The
// fixture lists no errors.reasons, so the host reports the refusal's code.
func TestProviderRendersWithTheEnginesStatedCapabilities_FailurePath(t *testing.T) {
	c := qt.New(t)

	out, diagnostics, code := renderDefaults(c, "fixture-no-defaults", "FIXTURE_NO_DEFAULTS=1")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "error rendering fixture-no-defaults schema: provider render failed (FailedPrecondition)")
	c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE")
}
