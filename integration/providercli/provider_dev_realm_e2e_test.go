//go:build integration

package providercli_test

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
)

// spannerProviderID is the provider Ptah's catalog lists for the spanner
// engine, which a manifest that selects one for it must name.
const spannerProviderID = "github.com/stokaro/ptah-spanner-plugin"

// builtInNameCLI runs the fixture's dev database engine under the name of an
// engine Ptah also builds in, spanner, whose built-in descriptor names a dev
// database with a SQL query, as the Spanner provider is selected while the
// built-in engine still exists.
type builtInNameCLI struct {
	manifest, target, dev, schema string
}

func newBuiltInNameCLI(c *qt.C) builtInNameCLI {
	c.Helper()
	dir := c.TempDir()
	spec := migrationProviderCommandSpec(c, "devdb")
	spec.ProviderID = spannerProviderID
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_DIR="+dir, "FIXTURE_DEV_ENGINE=spanner", "FIXTURE_DEV_PROVIDER_ID="+spannerProviderID)
	manifest, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: "spanner"}}}}})
	c.Assert(err, qt.IsNil)
	f := builtInNameCLI{manifest: filepath.Join(dir, "providers.json"), target: filepath.Join(dir, "target.db"), dev: filepath.Join(dir, "dev.db"),
		schema: filepath.Join(dir, "schema.yaml")}
	c.Assert(os.WriteFile(f.manifest, manifest, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(f.schema, []byte("tables:\n  items:\n    columns:\n      id:\n        type: INTEGER\n        not_null: true\n"), 0o600), qt.IsNil)
	return f
}

// spannerAddress is a spanner URL of the fixture database at path. The
// address pins its schema, because a spanner URL that pins none selects the
// built-in engine's realm read.
func spannerAddress(path string) string {
	return (&url.URL{Scheme: "spanner", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/"), RawQuery: "search_path=main"}).String()
}

func (f builtInNameCLI) apply(c *qt.C, devURL string) (string, int) {
	c.Helper()
	_, diagnostics, code := executeProviderCLI(c.Context(), []string{"schema", "apply", "--provider-config", f.manifest, "--provider-engine", "spanner",
		"--db-url", spannerAddress(f.target), "--schema-file", f.schema, "--dev-url", devURL, "--auto-approve"})
	return diagnostics, code
}

// The provider's session names the dev database, whatever the built-in
// descriptor of the same engine name says: the rehearsal runs on the dev
// database and hands it back empty, where a host that ran the descriptor's
// query on the provider's connection stopped before the target.
func TestProviderOfABuiltInEngineNameRehearsesOnItsDevDatabase(t *testing.T) {
	c := qt.New(t)
	f := newBuiltInNameCLI(c)
	diagnostics, code := f.apply(c, spannerAddress(f.dev))
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(tables(c, f.target), qt.DeepEquals, []string{"items"})
	c.Assert(tables(c, f.dev), qt.HasLen, 0)
}

// The two sessions are compared by what each provider session selects, so a
// dev URL that names the target is refused before either database changes.
func TestProviderOfABuiltInEngineNameRefusesTheTargetAsItsDevDatabase(t *testing.T) {
	c := qt.New(t)
	f := newBuiltInNameCLI(c)
	diagnostics, code := f.apply(c, spannerAddress(f.target))
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "--dev-url must not point at the target database")
	c.Assert(tables(c, f.target), qt.HasLen, 0)
}
