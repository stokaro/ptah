//go:build integration

package providercli_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/testutils"
)

// A rehearsal of a comparison narrowed to some schemas reads the target's
// other schemas only where a planned statement names them. The namespaces
// program is the engine, with FIXTURE_UNINSPECTABLE_SCHEMAS=archive: it fails
// any read whose scope covers schema archive, a read of the whole database
// included. The target holds a table in archive, so a rehearsal that reads
// the whole target to recreate what lies outside the comparison fails there,
// though nothing the plan runs names archive. The expected state comes from
// reading the database files and the schema files back.

// scopedSeedHCL declares schema app with the table the target holds and one
// the plan creates.
const scopedSeedHCL = `schema "app" {
}

table "t" {
  schema = schema.app
  column "id" {
    null = false
    type = INTEGER
  }
}

table "orders" {
  schema = schema.app
  column "id" {
    null = false
    type = INTEGER
  }
}
`

// scopedSeedYAML declares the table the target holds in main and one the plan
// creates.
const scopedSeedYAML = "tables:\n  items:\n    columns:\n      id:\n        type: INTEGER\n        not_null: true\n" +
	"  second:\n    columns:\n      value:\n        type: TEXT\n"

// holdObjects writes a fixture database as the namespaces program keeps one:
// the tables, named in its stored form "schema.name", and the file of the
// schemas it holds beside main.
func holdObjects(c *qt.C, path string, schemas []string, statements ...string) {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	for _, statement := range statements {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	data, err := json.Marshal(schemas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(path+".schemas.json", data, 0o600), qt.IsNil)
}

// scopedSeedFixture is the CLI over the namespaces program in mode, serving
// its own engine name, with a target that holds app.t and archive.old.
func scopedSeedFixture(c *qt.C, mode string) devCLI {
	c.Helper()
	dir := c.TempDir()
	spec := fixtureReleaseSpec(c, "namespaces", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_DIR="+dir, "FIXTURE_NAMESPACES="+mode, "FIXTURE_UNINSPECTABLE_SCHEMAS=archive")
	f := devCLI{providerCLI: newProviderCLI(c, spec), dir: dir}
	f.devPath = filepath.Join(filepath.Dir(f.manifestPath), "dev.db")
	f.schemaPath = filepath.Join(filepath.Dir(f.schemaPath), "schema.hcl")
	c.Assert(os.WriteFile(f.schemaPath, []byte(scopedSeedHCL), 0o600), qt.IsNil)
	holdObjects(c, f.databasePath, []string{"app", "archive"}, `CREATE TABLE "app.t" ("id" INTEGER NOT NULL)`, `CREATE TABLE "archive.old" ("id" INTEGER NOT NULL)`)
	return f
}

// scopedSeedViewsHCL declares schema app with the table the target holds, one
// the plan creates, and the view app.orders the target holds, whose query
// reads the view identity.users, which reads the table auth.accounts.
const scopedSeedViewsHCL = `schema "app" {
}

table "t" {
  schema = schema.app
  column "id" {
    null = false
    type = INTEGER
  }
}

table "added" {
  schema = schema.app
  column "id" {
    null = false
    type = INTEGER
  }
}

view "orders" {
  schema = schema.app
  as     = "SELECT id FROM identity.users"
}
`

// heldViews are the views the namespaces program keeps for a database, each
// as schema.name: query.
func heldViews(c *qt.C, database string) []string {
	c.Helper()
	data, err := os.ReadFile(database + ".views.json")
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	c.Assert(err, qt.IsNil)
	var views []struct{ Schema, Name, Body string }
	c.Assert(json.Unmarshal(data, &views), qt.IsNil)
	held := make([]string, 0, len(views))
	for _, view := range views {
		held = append(held, view.Schema+"."+view.Name+": "+view.Body)
	}
	return held
}

// targetViews are the views scopedSeedViewsFixture's target holds.
var targetViews = []string{"app.orders: SELECT id FROM identity.users", "identity.users: SELECT id FROM auth.accounts"}

// scopedSeedViewsFixture is scopedSeedFixture with views: a target that holds
// app.t, the view app.orders over the view identity.users over the table
// auth.accounts, and archive.old. The program cannot inspect the schemas
// uninspectable names, comma-separated.
func scopedSeedViewsFixture(c *qt.C, uninspectable string) devCLI {
	c.Helper()
	dir := c.TempDir()
	spec := fixtureReleaseSpec(c, "namespaces", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_DIR="+dir, "FIXTURE_NAMESPACE_VIEWS=true", "FIXTURE_UNINSPECTABLE_SCHEMAS="+uninspectable)
	f := devCLI{providerCLI: newProviderCLI(c, spec), dir: dir}
	f.devPath = filepath.Join(filepath.Dir(f.manifestPath), "dev.db")
	f.schemaPath = filepath.Join(filepath.Dir(f.schemaPath), "schema.hcl")
	c.Assert(os.WriteFile(f.schemaPath, []byte(scopedSeedViewsHCL), 0o600), qt.IsNil)
	holdObjects(c, f.databasePath, []string{"app", "archive", "auth", "identity"}, `CREATE TABLE "app.t" ("id" INTEGER NOT NULL)`,
		`CREATE TABLE "auth.accounts" ("id" INTEGER NOT NULL)`, `CREATE TABLE "archive.old" ("id" INTEGER NOT NULL)`)
	views, err := json.Marshal([]map[string]string{
		{"schema": "app", "name": "orders", "body": "SELECT id FROM identity.users"},
		{"schema": "identity", "name": "users", "body": "SELECT id FROM auth.accounts"},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.databasePath+".views.json", views, 0o600), qt.IsNil)
	return f
}

// assertRehearsedDependencies asserts what a rehearsed apply of
// scopedSeedViewsHCL leaves: the target gets the planned table and keeps
// every view and archive.old, and the dev database is handed back empty,
// without the objects and schemas the rehearsal recreated there.
func assertRehearsedDependencies(c *qt.C, f devCLI) {
	c.Helper()
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"app.added", "app.t", "archive.old", "auth.accounts"})
	c.Assert(heldViews(c, f.databasePath), qt.DeepEquals, targetViews)
	c.Assert(heldSchemas(c, f.databasePath), qt.DeepEquals, []string{"app", "archive", "auth", "identity"})
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
	c.Assert(heldViews(c, f.devPath), qt.HasLen, 0)
	c.Assert(heldSchemas(c, f.devPath), qt.HasLen, 0)
}

// The compared view app.orders reads identity.users, which reads
// auth.accounts. The dev database refuses a view over a relation it does not
// hold, so the rehearsal recreates both from the target: it reads schema
// identity for the name app.orders uses, then schema auth for the name the
// added view uses in turn, and never schema archive.
func TestProviderRehearsalReadsTheSchemasOfTransitiveDependencies_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := scopedSeedViewsFixture(c, "archive")

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--schemas", "app",
		"--dev-url", fixtureAddress("", f.devPath), "--auto-approve")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	assertRehearsedDependencies(c, f)
}

// The compatibility surface recreates the same dependencies.
func TestProviderCompatRehearsalReadsTheSchemasOfTransitiveDependencies_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := scopedSeedViewsFixture(c, "archive")

	_, diagnostics, code := compatApply(c, f, "--schema", "app", "--dev-url", fixtureAddress("", f.devPath))

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	assertRehearsedDependencies(c, f)
}

// A rehearsal in a realm of a database the operator names recreates the same
// dependencies there. The release removes the realm with what the run created
// in it, and the operator's database keeps the schema and the table it held.
func TestProviderRehearsalInARealmKeepsTheOperatorsObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := scopedSeedViewsFixture(c, "archive")
	operator := filepath.Join(f.dir, "operator.db")
	holdObjects(c, operator, []string{"keep"}, `CREATE TABLE "keep.k" ("id" INTEGER NOT NULL)`)

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--schemas", "app",
		"--dev-url", fixtureAddress("localhost", operator), "--auto-approve")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"app.added", "app.t", "archive.old", "auth.accounts"})
	c.Assert(heldViews(c, f.databasePath), qt.DeepEquals, targetViews)
	c.Assert(tables(c, operator), qt.DeepEquals, []string{"keep.k"})
	c.Assert(heldSchemas(c, operator), qt.DeepEquals, []string{"keep"})
	c.Assert(entries(c, f.dir), qt.DeepEquals, []string{"operator.db", "operator.db.schemas.json"})
}

// A schema a statement needs is read even when its inspection fails, and the
// provider's error stops the rehearsal with the schemas the read asked for.
// Nothing reaches the target, and the dev database is handed back empty.
func TestProviderRehearsalOfASchemaItCannotInspect_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := scopedSeedViewsFixture(c, "identity")

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--schemas", "app",
		"--dev-url", fixtureAddress("", f.devPath), "--auto-approve")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, `of the target through its provider: provider inspect failed`)
	c.Assert(diagnostics, qt.Contains, "the plan was not applied to the target database")
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"app.t", "archive.old", "auth.accounts"})
	c.Assert(heldViews(c, f.databasePath), qt.DeepEquals, targetViews)
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
	c.Assert(heldViews(c, f.devPath), qt.HasLen, 0)
	c.Assert(heldSchemas(c, f.devPath), qt.HasLen, 0)
}

// compatApply runs `ptah-compat schema apply` of the fixture's schema file
// against its database, with the manifest selected the only way that surface
// selects one.
func compatApply(c *qt.C, f devCLI, args ...string) (stdout, stderr string, exitCode int) {
	c.Helper()
	c.Setenv("PTAH_PROVIDER_CONFIG", f.manifestPath)
	cmd := atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.Full())
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetIn(strings.NewReader(""))
	exitCode = root.RunContext(c.Context(), cmd, append([]string{"schema", "apply", "--url", f.address,
		"--to", "file://" + filepath.ToSlash(f.schemaPath), "--auto-approve"}, args...)...)
	f.assertStopped(c)
	return out.String(), diagnostics.String(), exitCode
}

// assertScopedRehearsal asserts what a rehearsed apply of scopedSeedHCL
// leaves: the target gets the planned table and keeps archive.old, and the
// dev database is handed back empty, without the schema app the run created
// there.
func assertScopedRehearsal(c *qt.C, f devCLI) {
	c.Helper()
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"app.orders", "app.t", "archive.old"})
	c.Assert(heldSchemas(c, f.databasePath), qt.DeepEquals, []string{"app", "archive"})
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
	c.Assert(heldSchemas(c, f.devPath), qt.HasLen, 0)
}

// The rehearsal of a plan compared over schema app reads app and the
// session's own schema through the provider's filtered inspection, so the
// apply succeeds with schema archive never inspected.
func TestProviderRehearsalReadsOnlyTheSchemasItsStatementsName_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := scopedSeedFixture(c, "")

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--schemas", "app",
		"--dev-url", fixtureAddress("", f.devPath), "--auto-approve")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	assertScopedRehearsal(c, f)
}

// The compatibility surface rehearses through the same reader.
func TestProviderCompatRehearsalReadsOnlyTheSchemasItsStatementsName_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := scopedSeedFixture(c, "")

	_, diagnostics, code := compatApply(c, f, "--schema", "app", "--dev-url", fixtureAddress("", f.devPath))

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	assertScopedRehearsal(c, f)
}

// A control of the fixture rather than of Ptah, for the tests above: the
// fixture fails a read that covers schema archive, so a rehearsal that read it
// could not succeed. Without it, those tests would pass against a fixture that
// inspects archive without complaint.
func TestProviderRehearsalUninspectableSchemaFailsAWholeRead_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := scopedSeedFixture(c, "")

	_, diagnostics, code := f.run(c, c.Context(), "inspect", "--schemas", "archive", "--format", "json")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "provider inspect failed")
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"app.t", "archive.old"})
}

// An engine that carries no namespaces has one read of its whole database for
// what lies outside the comparison, which covers schema archive here. The
// rehearsal fails and names the feature the engine lacks, the target is left
// as it was, and the dev database is handed back empty.
func TestProviderRehearsalWithoutNamespacesReadsTheWholeTarget_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := scopedSeedFixture(c, "off")
	holdObjects(c, f.databasePath, []string{"archive"}, `CREATE TABLE "items" ("id" INTEGER NOT NULL)`, `DROP TABLE "app.t"`)
	f.schemaPath = filepath.Join(filepath.Dir(f.schemaPath), "schema.yaml")
	c.Assert(os.WriteFile(f.schemaPath, []byte(scopedSeedYAML), 0o600), qt.IsNil)

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--schemas", "main",
		"--dev-url", fixtureAddress("", f.devPath), "--auto-approve")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, `the provider of engine "fixture" carries no namespaces (schema.common-namespaces), so the rehearsal reads the target's whole database`)
	c.Assert(diagnostics, qt.Contains, "and that read failed: provider inspect failed")
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"archive.old", "items"})
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
}

// A provider serving the built-in engine name postgres, whose URL pins schema
// main, reads the schemas its statements name in the same way: the session
// offers no realm list, and schema archive is never inspected.
func TestProviderOfABuiltInEngineNameRehearsalReadsOnlyTheSchemasItsStatementsName_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	spec := fixtureReleaseSpec(c, "namespaces", testutils.FixtureRelease{})
	spec.ProviderID = builtInNameProviderID
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_DIR="+dir, "FIXTURE_DEV_ENGINE=postgres", "FIXTURE_DEV_PROVIDER_ID="+builtInNameProviderID,
		"FIXTURE_UNINSPECTABLE_SCHEMAS=archive")
	manifest, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: "postgres"}}}}})
	c.Assert(err, qt.IsNil)
	f := builtInNameCLI{manifest: filepath.Join(dir, "providers.json"), target: filepath.Join(dir, "target.db"), dev: filepath.Join(dir, "dev.db"),
		schema: filepath.Join(dir, "schema.yaml")}
	c.Assert(os.WriteFile(f.manifest, manifest, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(f.schema, []byte(scopedSeedYAML), 0o600), qt.IsNil)
	holdObjects(c, f.target, []string{"archive"}, `CREATE TABLE "items" ("id" INTEGER NOT NULL)`, `CREATE TABLE "archive.old" ("id" INTEGER NOT NULL)`)

	diagnostics, code := f.apply(c, builtInNameAddress(f.dev))

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(tables(c, f.target), qt.DeepEquals, []string{"archive.old", "items", "second"})
	c.Assert(heldSchemas(c, f.target), qt.DeepEquals, []string{"archive"})
	c.Assert(tables(c, f.dev), qt.HasLen, 0)
	c.Assert(heldSchemas(c, f.dev), qt.HasLen, 0)
}
