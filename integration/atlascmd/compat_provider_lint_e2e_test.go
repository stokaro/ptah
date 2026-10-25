//go:build integration

package atlas_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// newCompatLintFixture is a private directory holding a manifest that names
// the fixture command as the provider of engine under providerID, and a
// hashed migration directory whose second version drops the table the first
// creates.
func newCompatLintFixture(c *qt.C, command, providerID, engine string) compatProviderFixture {
	c.Helper()
	binaries := compatProviderBinaries{
		compat:   buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat"),
		provider: testutils.BuildFixtureProvider(c, command),
	}
	dir := c.TempDir()
	f := compatProviderFixture{binaries: binaries, dir: dir, manifest: filepath.Join(dir, "providers.json"), pidFile: filepath.Join(dir, "provider.pid")}
	data, err := os.ReadFile(binaries.provider)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	manifest, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion, Providers: []providerconfig.Installation{{
		Spec: providerhost.Spec{Executable: binaries.provider, SHA256: hex.EncodeToString(digest[:]), ProviderID: providerID,
			Environment: []string{"PTAH_FIXTURE_PID_FILE=" + f.pidFile}},
		Engines: []providerconfig.Engine{{ID: engine}},
	}}})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.manifest, manifest, 0o600), qt.IsNil)
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_init.sql"), []byte("CREATE TABLE widgets (id integer NOT NULL);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20240102000000_drop.sql"), []byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)
	f.migrationsURL = "file://" + filepath.ToSlash(migrations)
	_, stderr, code := f.run(nil, "migrate", "hash", "--dir", f.migrationsURL)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	return f
}

// TestCompatMigrateLintSelectsTheProviderOfTheDevURL runs `migrate lint` with
// a manifest naming a provider for ydb, the engine the dev URL names. The
// compatibility surface resolves it through the native pipeline and starts it
// to build the runtime the run lints with. The bundled YDB engine is still
// registered beside it, so the two claims on the ydb target are refused
// rather than resolved by precedence: the run is not linted by the bundled
// engine instead, and the provider process ends with the command.
func TestCompatMigrateLintSelectsTheProviderOfTheDevURL(t *testing.T) {
	c := qt.New(t)
	f := newCompatLintFixture(c, "ydb-lint", "github.com/stokaro/ptah-ydb-plugin", "ydb")

	out, stderr, code := f.run(f.withManifest(), "migrate", "lint", "--dir", f.migrationsURL, "--dev-url", "ydb://localhost:2136/local", "--latest", "1")

	c.Assert(code, qt.Equals, 1)
	c.Assert(out, qt.Equals, "")
	c.Assert(stderr, qt.Equals, `Error: invalid provider registration: target name "ydb" is claimed by "ptah.run/ydb" and "github.com/stokaro/ptah-ydb-plugin"`+"\n")
	f.assertStopped(c)
}

// TestCompatSchemaApplyLintsThroughTheProvider plans a drop on a database the
// fixture provider serves, under an atlas.hcl policy that rates destructive
// changes as errors. The lint pass runs in the runtime the verb planned with,
// so the provider splits the planned statements: DS107 reports the drop
// because the fixture marked it as one that loses state, and the bundled
// runtime has no splitter for the fixture engine at all.
func TestCompatSchemaApplyLintsThroughTheProvider(t *testing.T) {
	c := qt.New(t)
	f := newCompatLintFixture(c, "lint", "example.org/fixture", "fixture")
	db := filepath.Join(f.dir, "target.db")
	createSchemaInspectSQLiteDatabase(c, db, `CREATE TABLE "widgets" ("id" INTEGER)`, `CREATE TABLE "gadgets" ("id" INTEGER)`)
	address := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(db), "/")}).String()
	c.Assert(os.WriteFile(filepath.Join(f.dir, "schema.yaml"), []byte("tables:\n  widgets:\n    columns:\n      id:\n        type: INTEGER\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(f.dir, "atlas.hcl"), []byte("lint {\n  destructive {\n    error = true\n  }\n}\n"), 0o600), qt.IsNil)

	out, stderr, code := f.run(f.withManifest(), "schema", "apply", "--url", address, "--to", "file://schema.yaml", "--dry-run")

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
	c.Assert(out, qt.Equals, "Planned schema changes:\nDROP TABLE \"gadgets\";\n", qt.Commentf("%s", stderr))
	c.Assert(stderr, qt.Contains, "Error: the planned changes are refused by the atlas.hcl lint policy:\n")
	c.Assert(stderr, qt.Contains, "\n  - DS101 error: ")
	c.Assert(stderr, qt.Contains, "\n  - DS107 error: ")
	f.assertStopped(c)
}

// TestCompatMigrateLintReachesTheProviderOfADevURLOnlyItServes lints a
// migration directory whose dev URL names the fixture engine, which no
// built-in dialect is. The run reads the engine from the provider selection
// rather than from the built-in URL table and takes it as the lint dialect, so
// it reaches the provider's dev database. There it stops: the provider
// contract has no check that a dev database is clean, and the replay refuses
// to run on one it cannot check. The provider process ends with the command.
func TestCompatMigrateLintReachesTheProviderOfADevURLOnlyItServes(t *testing.T) {
	c := qt.New(t)
	f := newCompatLintFixture(c, "lint", "example.org/fixture", "fixture")
	dev := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(f.dir, "dev.db")), "/")}).String()

	out, stderr, code := f.run(f.withManifest(), "migrate", "lint", "--dir", f.migrationsURL, "--dev-url", dev, "--latest", "1")

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
	c.Assert(out, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: error validating migration SQL on dev database: check that the dev database is clean: "+
		"clean check: unsupported feature: the experimental provider contract does not support database cleanup\n")
	f.assertStopped(c)
}
