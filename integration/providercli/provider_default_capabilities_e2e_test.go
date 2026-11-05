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

// defaultsManifest writes a manifest that selects the default-capabilities
// fixture for engine, started with environment, in dir.
func defaultsManifest(c *qt.C, dir, engine string, environment []string) string {
	c.Helper()
	spec := fixtureSpec(c, "default-capabilities")
	spec.Environment = append(slices.Clone(spec.Environment), environment...)
	data, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: engine}}}}})
	c.Assert(err, qt.IsNil)
	manifest := filepath.Join(dir, "providers.json")
	c.Assert(os.WriteFile(manifest, data, 0o600), qt.IsNil)
	return manifest
}

// renderDefaults renders a one-table YAML schema offline on engine, through
// the default-capabilities fixture started with environment, with the
// command's further arguments.
func renderDefaults(c *qt.C, engine string, environment []string, args ...string) (stdout, stderr string, code int) {
	c.Helper()
	dir := c.TempDir()
	manifest := defaultsManifest(c, dir, engine, environment)
	schema := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte("tables:\n  items:\n    columns:\n      id: { type: INTEGER, primary: true }\n"), 0o600), qt.IsNil)
	return executeProviderCLI(c.Context(), append([]string{"schema", "render", "--provider-config", manifest, "--dialect", engine, "--schema-file", schema}, args...))
}

// An offline render plans the engine with the capabilities its provider
// states for it: the fixture renders only against an established
// transactional_ddl, and the stated facts establish it.
func TestProviderRendersWithTheEnginesStatedCapabilities_HappyPath(t *testing.T) {
	c := qt.New(t)

	out, diagnostics, code := renderDefaults(c, "fixture-defaults", nil)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "-- transactional_ddl=false\nCREATE TABLE \"items\" (id INTEGER);")
}

// Without the stated facts no capability is established offline, and the
// provider refuses the render, as it refuses one against unknown facts. The
// fixture lists no errors.reasons, so the host reports the refusal's code.
func TestProviderRendersWithTheEnginesStatedCapabilities_FailurePath(t *testing.T) {
	c := qt.New(t)

	out, diagnostics, code := renderDefaults(c, "fixture-no-defaults", []string{"FIXTURE_NO_DEFAULTS=1"})

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "error rendering fixture-no-defaults schema: provider render failed (FailedPrecondition)")
	c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE")
}

// A --server-version plans the engine with the facts of the release line it
// names: one equal to the version, or one the version starts with followed by
// a dot.
func TestProviderRendersWithAReleaseLinesCapabilities_HappyPath(t *testing.T) {
	c := qt.New(t)

	out, diagnostics, code := renderDefaults(c, "fixture-defaults", nil, "--server-version", "1.4.2")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "-- transactional_ddl=true\nCREATE TABLE \"items\" (id INTEGER);")
}

// A release that names no declared line is planned with the line the declared
// lines place it on, as a built-in dialect's version ladder places it, and the
// render says so: a newer release with the newest line, an older one with the
// oldest.
func TestProviderRendersWithAReleaseLinesCapabilities_HappyPath_PlacesAReleaseOnTheLines(t *testing.T) {
	for _, test := range []struct{ version, rendered, warning string }{
		{"3.0", "-- transactional_ddl=false\n",
			"warning: fixture-defaults 3.0 is newer than the newest measured release line 2.1; capabilities were planned as 2.1\n"},
		{"1.0.7", "-- transactional_ddl=true\n",
			"warning: fixture-defaults 1.0.7 is not a measured release line; capabilities fall back to the preset its ladder assigns " +
				"(newest measured line: 2.1)\n"},
	} {
		t.Run(test.version, func(t *testing.T) {
			c := qt.New(t)

			out, diagnostics, code := renderDefaults(c, "fixture-defaults", nil, "--server-version", test.version)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(out, qt.Contains, test.rendered+"CREATE TABLE \"items\" (id INTEGER);")
			c.Assert(diagnostics, qt.Contains, test.warning)
		})
	}
}

// A version that is no release is refused with the lines to choose from, never
// planned with the engine's default.
func TestProviderRendersWithAReleaseLinesCapabilities_FailurePath(t *testing.T) {
	c := qt.New(t)

	out, diagnostics, code := renderDefaults(c, "fixture-defaults", nil, "--server-version", "trunk")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, `"trunk" names no release line the fixture-defaults provider declares: expected 1.4, 2.1, `+
		`or a version that starts with one followed by a dot`)
	c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE")
}

// `ptah db capabilities` certifies a server of an engine a provider serves
// against the release lines the provider declares: the line the server's
// version names gives the preset its label and the support level, and a
// version newer than every line is planned with the newest and reported as
// best-effort, as a built-in dialect's server is.
func TestProviderDatabaseCapabilitiesReportTheDeclaredLine_HappyPath(t *testing.T) {
	for _, test := range []struct {
		version string
		want    []string
	}{
		{"2.1.7", []string{
			"Server version:     2.1.7\n",
			"Capability preset:  Fixture 2.1 (fixture-defaults)\n",
			"Preset source:      version-ladder\n",
			"Support level:      best-effort\n",
			"Release line:       2.1 (Fixture 2.1)\n",
			"  transactional_ddl                       unsupported\n",
		}},
		{"1.4.2", []string{
			"Capability preset:  Fixture 1.4 (fixture-defaults)\n",
			"Release line:       1.4 (Fixture 1.4)\n",
			"  transactional_ddl                       supported\n",
		}},
		{"3.0", []string{
			"Capability preset:  (unnamed, resolved for fixture-defaults)\n",
			"Preset source:      newer-than-measured\n",
			"Release line:       none\n",
			"Note: fixture-defaults 3.0 is newer than the newest measured release line 2.1; capabilities were planned as 2.1\n",
			"  transactional_ddl                       unsupported\n",
		}},
	} {
		t.Run(test.version, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			manifest := defaultsManifest(c, dir, "fixture-defaults", []string{"FIXTURE_SERVER_VERSION=" + test.version})
			address := (&url.URL{Scheme: "fixture-defaults", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(dir, "server.db")), "/")}).String()

			out, diagnostics, code := executeProviderCLI(c.Context(), []string{"db", "capabilities", "--provider-config", manifest,
				"--db-url", address})

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			for _, line := range test.want {
				c.Assert(out, qt.Contains, line)
			}
		})
	}
}
