//go:build integration

package atlas_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// newCompatLintFixture is a private directory holding a manifest that names
// the fixture command as the provider of engine under providerID, and a
// hashed migration directory whose second version drops the table the first
// creates. The statements are in the grammar the fixture's SQL analysis reads.
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
	f.migrationsURL = "file://" + filepath.ToSlash(filepath.Join(dir, "migrations"))
	f.writeMigrations(c, `CREATE TABLE "widgets" ("id" INTEGER NOT NULL);`, `DROP TABLE "widgets";`)
	return f
}

// writeMigrations replaces the migration directory with one version for each
// statement, in order, and hashes it.
func (f compatProviderFixture) writeMigrations(c *qt.C, statements ...string) {
	c.Helper()
	migrations := filepath.Join(f.dir, "migrations")
	c.Assert(os.RemoveAll(migrations), qt.IsNil)
	c.Assert(os.MkdirAll(migrations, 0o700), qt.IsNil)
	for i, statement := range statements {
		name := fmt.Sprintf("2024010%d000000_v%d.sql", i+1, i+1)
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(statement+"\n"), 0o600), qt.IsNil)
	}
	_, stderr, code := f.run(nil, "migrate", "hash", "--dir", f.migrationsURL)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
}

// fixtureURL is the fixture provider's address of the database file name in
// the fixture's directory.
func (f compatProviderFixture) fixtureURL(name string) string {
	return (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(f.dir, name)), "/")}).String()
}

// sqliteTables lists the tables a fixture database file holds, read through a
// driver of the test's own, so the answer does not pass through the provider.
func sqliteTables(c *qt.C, path string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
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

// TestCompatMigrateLintRefusesADevDatabaseItsProviderCannotClean lints a
// migration directory whose dev URL names the fixture engine, which no
// built-in dialect is, on a provider that declares no dev database cleanup.
// The run reads the engine from the provider selection rather than from the
// built-in URL table and takes it as the lint dialect, so it reaches the
// provider's dev database. There it stops: nothing can say the database is
// clean, and the replay refuses to run on one nobody checked. The provider
// process ends with the command.
func TestCompatMigrateLintRefusesADevDatabaseItsProviderCannotClean(t *testing.T) {
	c := qt.New(t)
	f := newCompatLintFixture(c, "lint", "example.org/fixture", "fixture")

	out, stderr, code := f.run(f.withManifest(), "migrate", "lint", "--dir", f.migrationsURL, "--dev-url", f.fixtureURL("dev.db"), "--latest", "1")

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
	c.Assert(out, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: error validating migration SQL on dev database: check that the dev database is clean: "+
		"clean check: unsupported feature: the experimental provider contract does not support database cleanup\n")
	f.assertStopped(c)
}

// TestCompatMigrateLintReplaysOnTheProvidersDevDatabase lints a migration
// directory on a dev database that the provider cleans and guards. The run
// replays the directory there, the provider's rules report the drop, and the
// database is empty again when the command returns.
func TestCompatMigrateLintReplaysOnTheProvidersDevDatabase(t *testing.T) {
	c := qt.New(t)
	f := newCompatLintFixture(c, "devdb-lint", "example.org/fixture", "fixture")

	out, stderr, code := f.run(f.withManifest(), "migrate", "lint", "--dir", f.migrationsURL, "--dev-url", f.fixtureURL("dev.db"), "--latest", "1")

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
	c.Assert(stderr, qt.Equals, "")
	c.Assert(out, qt.Contains, "  -- analyzing version 20240102000000\n")
	c.Assert(out, qt.Contains, " [DS102]: DROP TABLE permanently deletes the table")
	c.Assert(out, qt.Contains, " [FX101]: DROP TABLE removes every row\n")
	c.Assert(sqliteTables(c, filepath.Join(f.dir, "dev.db")), qt.HasLen, 0)
	f.assertStopped(c)
}

// TestCompatMigrateLintReplayFailsOnTheProvidersDevDatabase_FailurePath holds
// the two refusals of a replay on a provider's dev database: a statement the
// dev database refuses fails the lint in the replay, and a dev database the
// provider lists an object in is refused before anything runs and keeps the
// object.
func TestCompatMigrateLintReplayFailsOnTheProvidersDevDatabase_FailurePath(t *testing.T) {
	t.Run("a statement the dev database refuses", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatLintFixture(c, "devdb-lint", "example.org/fixture", "fixture")
		f.writeMigrations(c, `CREATE TABLE "widgets" ("id" INTEGER NOT NULL);`, `DROP TABLE "gadgets";`)

		out, stderr, code := f.run(f.withManifest(), "migrate", "lint", "--dir", f.migrationsURL, "--dev-url", f.fixtureURL("dev.db"), "--latest", "1")

		c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
		c.Assert(out, qt.Equals, "")
		c.Assert(stderr, qt.Contains, "Error: error validating migration SQL on dev database: replay migration 20240102000000 on dev database: ")
		c.Assert(stderr, qt.Contains, "\nSQL: DROP TABLE \"gadgets\"\n")
		c.Assert(sqliteTables(c, filepath.Join(f.dir, "dev.db")), qt.HasLen, 0)
		f.assertStopped(c)
	})
	t.Run("a dev database that is not clean", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatLintFixture(c, "devdb-lint", "example.org/fixture", "fixture")
		createSchemaInspectSQLiteDatabase(c, filepath.Join(f.dir, "dev.db"), `CREATE TABLE "leftover" ("id" INTEGER)`)

		out, stderr, code := f.run(f.withManifest(), "migrate", "lint", "--dir", f.migrationsURL, "--dev-url", f.fixtureURL("dev.db"), "--latest", "1")

		c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
		c.Assert(out, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: taking database snapshot: sql/migrate: connected database is not clean: found table \"leftover\"\n")
		c.Assert(sqliteTables(c, filepath.Join(f.dir, "dev.db")), qt.DeepEquals, []string{"leftover"})
		f.assertStopped(c)
	})
}

// TestCompatSchemaPlanLintsAPlanOfTheProvidersDatabase plans a drop on a
// database the fixture provider serves, then lints and validates the saved
// plan. `schema plan` connects to its --from and --dev-url through the
// provider like its subcommands do, and the plan verbs replay the plan on the
// provider's dev database. Neither command changes the target.
func TestCompatSchemaPlanLintsAPlanOfTheProvidersDatabase(t *testing.T) {
	c := qt.New(t)
	f := newCompatLintFixture(c, "devdb-lint", "example.org/fixture", "fixture")
	target := filepath.Join(f.dir, "target.db")
	createSchemaInspectSQLiteDatabase(c, target, `CREATE TABLE "widgets" ("id" INTEGER)`, `CREATE TABLE "gadgets" ("id" INTEGER)`)
	c.Assert(os.WriteFile(filepath.Join(f.dir, "schema.yaml"), []byte("tables:\n  widgets:\n    columns:\n      id:\n        type: INTEGER\n"), 0o600), qt.IsNil)
	plan := filepath.Join(f.dir, "drop.plan.json")
	transition := []string{"--from", f.fixtureURL("target.db"), "--to", "file://schema.yaml", "--dev-url", f.fixtureURL("dev.db")}

	out, stderr, code := f.run(f.withManifest(), append([]string{"schema", "plan", "--save", "--output", plan, "--auto-approve"}, transition...)...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(out, qt.Contains, "Planned schema changes:\nDROP TABLE \"gadgets\";\n")
	f.assertStopped(c)

	out, stderr, code = f.run(f.withManifest(), append([]string{"schema", "plan", "lint", "--file", "file://" + filepath.ToSlash(plan)}, transition...)...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(out, qt.Contains, "Analyzing planned statements (1 in total):\n")
	c.Assert(out, qt.Contains, " [DS102]: DROP TABLE permanently deletes the table")
	c.Assert(out, qt.Contains, " [FX101]: DROP TABLE removes every row\n")
	f.assertStopped(c)

	_, stderr, code = f.run(f.withManifest(), append([]string{"schema", "plan", "validate", "--file", "file://" + filepath.ToSlash(plan)}, transition...)...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	f.assertStopped(c)

	c.Assert(sqliteTables(c, target), qt.DeepEquals, []string{"gadgets", "widgets"})
	c.Assert(sqliteTables(c, filepath.Join(f.dir, "dev.db")), qt.HasLen, 0)
}

// TestCompatSchemaPlanReadsTheManifestVariable holds where the provider
// session sits. `schema plan` runs a verb of its own and reads the manifest
// variable as its subcommands do, so a blank one is refused before anything
// connects; the `schema` group only prints its help and reads none.
func TestCompatSchemaPlanReadsTheManifestVariable(t *testing.T) {
	c := qt.New(t)
	f := compatProviderFixture{binaries: compatProviderBinaries{compat: buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat")}, dir: c.TempDir()}

	_, stderr, code := f.run([]string{"PTAH_PROVIDER_CONFIG="}, "schema", "plan", "--from", "fixture:///target.db", "--to", "file://schema.yaml")
	c.Assert(code, qt.Equals, 1)
	c.Assert(stderr, qt.Equals, "Error: PTAH_PROVIDER_CONFIG requires a manifest path\n")

	out, stderr, code := f.run([]string{"PTAH_PROVIDER_CONFIG="}, "schema")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(out, qt.Contains, "Use \"ptah-compat schema [command] --help\" for more information about a command.\n")
}

// scriptFixture is a fixture database holding one note in "notes", and a
// script file whose exec script sets it and asserts one row changed.
func scriptFixture(c *qt.C, f compatProviderFixture) string {
	c.Helper()
	createSchemaInspectSQLiteDatabase(c, filepath.Join(f.dir, "notes.db"), `CREATE TABLE "notes" ("id" INTEGER, "note" TEXT)`,
		`INSERT INTO "notes" VALUES (1, 'old')`)
	scripts := filepath.Join(f.dir, "scripts.hcl")
	c.Assert(os.WriteFile(scripts, []byte("script \"exec\" \"checked\" {\n  exec \"set\" {\n"+
		"    sql         = \"UPDATE notes SET note = 'new' WHERE id = 1\"\n    expect_rows = 1\n  }\n}\n"+
		"script \"query\" \"report\" {\n  query \"notes\" {\n    sql = \"SELECT note FROM notes\"\n  }\n}\n"), 0o600), qt.IsNil)
	return scripts
}

// sqliteNote reads the note back through the test's own driver.
func sqliteNote(c *qt.C, path string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var note string
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT note FROM notes WHERE id = 1`).Scan(&note), qt.IsNil)
	return note
}

// TestCompatScriptRunsThroughTheProvidersStatements runs an exec script that
// asserts the row it changes, and a query script, on a database the fixture
// provider serves. The provider measured the count, so the assertion is
// judged; the transaction commits.
func TestCompatScriptRunsThroughTheProvidersStatements(t *testing.T) {
	c := qt.New(t)
	f := newCompatLintFixture(c, "devdb-lint", "example.org/fixture", "fixture")
	scripts := scriptFixture(c, f)

	_, stderr, code := f.run(f.withManifest(), "script", "exec", "--url", f.fixtureURL("notes.db"), "--file", scripts, "--run", "checked")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stderr, qt.Contains, "1 rows affected")
	c.Assert(sqliteNote(c, filepath.Join(f.dir, "notes.db")), qt.Equals, "new")
	f.assertStopped(c)

	out, stderr, code := f.run(f.withManifest(), "script", "query", "--url", f.fixtureURL("notes.db"), "--file", scripts, "--run", "report")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(out, qt.Contains, "new")
	f.assertStopped(c)
}

// An engine that does not measure the count leaves expect_rows unjudged: the
// script is refused and its transaction rolls back, as on YDB.
func TestCompatScriptRunsThroughTheProvidersStatements_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newCompatLintFixture(c, "devdb-lint", "example.org/fixture", "fixture")
	scripts := scriptFixture(c, f)

	_, stderr, code := f.run(f.withManifest(), "script", "exec", "--url", f.fixtureURL("notes.db")+"?fault=uncounted-rows", "--file", scripts, "--run", "checked")

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
	c.Assert(stderr, qt.Contains, "expect_rows is 1, and this driver does not report a row count, so the assertion cannot be made")
	c.Assert(sqliteNote(c, filepath.Join(f.dir, "notes.db")), qt.Equals, "old")
	f.assertStopped(c)
}
