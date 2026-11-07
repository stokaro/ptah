//go:build integration

package providercli_test

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// The fixture's namespaces program is the engine: it keeps the schemas it
// creates in a file beside the database, stores a table of schema app as the
// SQLite table "app.t", and refuses a table of a schema it does not hold and
// the removal of a schema that holds a table, as a server with schemas does.
// The expected state comes from reading the database and that file back.

// namespacesFixture is the CLI over an empty fixture database served by the
// namespaces program in mode, with a SQL schema document.
func namespacesFixture(c *qt.C, mode string) devCLI {
	c.Helper()
	dir := c.TempDir()
	spec := fixtureReleaseSpec(c, "namespaces", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_DIR="+dir, "FIXTURE_NAMESPACES="+mode)
	f := devCLI{providerCLI: newProviderCLI(c, spec), dir: dir}
	f.devPath = filepath.Join(filepath.Dir(f.manifestPath), "dev.db")
	f.schemaPath = filepath.Join(filepath.Dir(f.schemaPath), "schema.sql")
	return f
}

// heldSchemas are the schemas the namespaces program created in a database,
// other than main.
func heldSchemas(c *qt.C, database string) []string {
	c.Helper()
	data, err := os.ReadFile(database + ".schemas.json")
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	c.Assert(err, qt.IsNil)
	var names []string
	c.Assert(json.Unmarshal(data, &names), qt.IsNil)
	return names
}

const (
	// schemaDocument declares two schemas, a table in one of them, and a
	// table in main.
	schemaDocument = `CREATE SCHEMA app;
CREATE SCHEMA empty_one;
CREATE TABLE app.t (id INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY);
`
	// schemaDocumentWithoutEmpty is schemaDocument without empty_one.
	schemaDocumentWithoutEmpty = `CREATE SCHEMA app;
CREATE TABLE app.t (id INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY);
`
)

// A declared schema is created before the table in it, an empty one is
// created too, and the next plan finds nothing to do: the read lists both
// schemas, so the comparison does not add them again. The engine refuses a
// table of a schema it does not hold, so the apply succeeding is the order.
func TestProviderNamespaces_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := namespacesFixture(c, "")
	f.declare(c, schemaDocument)

	planned := f.plan(c)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(planned, qt.Contains, `"outcome": "changes"`)
	c.Assert(strings.Index(planned, `FIXTURE CREATE SCHEMA \"app\"`) < strings.Index(planned, `CREATE TABLE \"app.t\"`), qt.IsTrue, qt.Commentf("%s", planned))
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"app.t", "p"})
	c.Assert(heldSchemas(c, f.databasePath), qt.DeepEquals, []string{"app", "empty_one"})
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// A table in a schema no document declares is created with its schema,
// which the read does not hold, as the PostgreSQL planner creates the schema
// of a created table.
func TestProviderNamespacesOfACreatedTable(t *testing.T) {
	c := qt.New(t)
	f := namespacesFixture(c, "")
	f.declare(c, "CREATE TABLE app.t (id INTEGER NOT NULL PRIMARY KEY);\n")

	f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"app.t"})
	c.Assert(heldSchemas(c, f.databasePath), qt.DeepEquals, []string{"app"})
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// A schema the database holds and the declaration no longer names stays, as
// on PostgreSQL: dropping it would take what it holds with it.
func TestProviderNamespacesKeepAnUndeclaredSchema(t *testing.T) {
	c := qt.New(t)
	f := namespacesFixture(c, "")
	f.declare(c, schemaDocument)
	f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
	f.declare(c, schemaDocumentWithoutEmpty)

	planned := f.plan(c)

	c.Assert(planned, qt.Contains, `"outcome": "no-changes"`)
	c.Assert(heldSchemas(c, f.databasePath), qt.DeepEquals, []string{"app", "empty_one"})
}

// A migration that creates a schema has a down file that drops it after the
// table in it.
func TestProviderNamespacesInADownFile(t *testing.T) {
	c := qt.New(t)
	f := namespacesFixture(c, "")
	f.declare(c, schemaDocumentWithoutEmpty)

	_, diagnostics, code, dir := f.generateMigration(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	up, down := upMigration(c, dir), downMigration(c, dir)
	c.Assert(strings.Index(up, `FIXTURE CREATE SCHEMA "app"`) < strings.Index(up, `CREATE TABLE "app.t"`), qt.IsTrue, qt.Commentf("%s", up))
	c.Assert(strings.Index(down, `DROP TABLE "app.t"`) < strings.Index(down, `FIXTURE DROP SCHEMA "app"`), qt.IsTrue, qt.Commentf("%s", down))
	c.Assert(down, qt.Contains, `FIXTURE DROP SCHEMA "app"`)
}

// A rehearsal on the dev database creates the schemas the plan creates, and
// the cleanup after it removes them with what they hold. A cleanup of the
// session's own schema alone would leave them, tables included, on the dev
// database.
func TestProviderNamespacesLeaveNoSchemaOnTheDevDatabase(t *testing.T) {
	c := qt.New(t)
	f := namespacesFixture(c, "")
	f.declare(c, schemaDocument)

	diagnostics, code := f.applyWithDev(c, fixtureAddress("", f.devPath))

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"app.t", "p"})
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
	c.Assert(heldSchemas(c, f.devPath), qt.HasLen, 0)
}

// Without the feature nothing can create a declared schema, and the host
// refuses the declaration before anything runs, as it did before an engine
// could carry one.
func TestProviderNamespacesWithoutTheFeature_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := namespacesFixture(c, "off")
	f.declare(c, schemaDocumentWithoutEmpty)

	_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Contains, uncarriedAppSchema)
	c.Assert(tables(c, f.databasePath), qt.HasLen, 0)
	c.Assert(heldSchemas(c, f.databasePath), qt.HasLen, 0)
}

// uncarriedAppSchema is the host's refusal of schema app on the fixture
// engine when its provider does not carry namespaces.
const uncarriedAppSchema = `does not support schema "app" on the fixture engine, whose provider does not carry namespaces (schema.common-namespaces)`

// emptySchemaDocuments declare schema app, which holds nothing, beside table
// p, in each source format that declares a schema; the YAML format declares
// none. argument is what the flag names, relative to the directory the
// document is written to.
var emptySchemaDocuments = []struct {
	name, file, flag, argument, text string
}{
	{name: "SQL", file: "schema.sql", flag: "--schema-file", argument: "schema.sql",
		text: "CREATE SCHEMA app;\nCREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY);\n"},
	{name: "HCL", file: "schema.hcl", flag: "--schema-file", argument: "schema.hcl",
		text: "schema \"main\" {\n}\n\nschema \"app\" {\n}\n\ntable \"p\" {\n  schema = schema.main\n  column \"id\" {\n    type = INTEGER\n  }\n}\n"},
	{name: "Go", file: "p.go", flag: "--root-dir", argument: ".",
		text: "package p\n\n//ptah:schema:schema name=\"app\"\ntype _ struct{}\n\n//ptah:schema:table name=\"p\"\ntype P struct {\n" +
			"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" not_null=\"true\" primary=\"true\"\n\tID int\n}\n"},
}

// An engine that does not carry namespaces has no step that creates a declared
// schema, and where no object of the declaration or of the read is in it,
// nothing hands it to the provider. The host refuses the declaration by the
// schema's name and the feature the engine lacks, and writes nothing. The
// constraints fixture validates each declaration itself, as a provider of
// contract 25.28 with schema.validate did, and reads schema app as one holding
// nothing; without the refusal the plan reported the database synced.
func TestProviderDeclaredSchemaWithoutNamespaces_FailurePath(t *testing.T) {
	for _, document := range emptySchemaDocuments {
		t.Run(document.name, func(t *testing.T) {
			c := qt.New(t)
			f := constraintsFixture(c, "schemas", `CREATE TABLE "p" ("id" INTEGER NOT NULL PRIMARY KEY)`)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, document.file), []byte(document.text), 0o600), qt.IsNil)

			out, diagnostics, code := f.run(c, c.Context(), "plan", document.flag, filepath.Join(dir, document.argument), "--output", f.planPath, "--json")

			c.Assert(code, qt.Equals, 2)
			c.Assert(out, qt.Contains, `"outcome": "failed"`)
			c.Assert(diagnostics, qt.Contains, uncarriedAppSchema)
			c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"p"})
		})
	}
}

// The engine's default schema exists, so a declaration of it is kept on an
// engine that does not carry namespaces, and the plan finds nothing to do.
func TestProviderDeclaredSchemaWithoutNamespaces_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "schemas", `CREATE TABLE "p" ("id" INTEGER NOT NULL PRIMARY KEY)`)
	f.schemaPath = filepath.Join(filepath.Dir(f.schemaPath), "schema.sql")
	f.declare(c, "CREATE SCHEMA IF NOT EXISTS main;\nCREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY);\n")

	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}
