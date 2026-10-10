//go:build integration

package integration_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migratesum"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// A dev database is claimed before a run resets it, and the claim records
// what the resets leave in place: the extensions it held, and, when its URL
// pins one schema, the database's other schemas. The pinned community binary
// v1.3.0 leaves both where they are, measured on 2026-09-27 against
// PostgreSQL 18: with `?search_path=public` on the dev URL, a table in another
// schema survives every verb at exit 0, and `schema apply` rehearses beside an
// extension installed in public. Without the baseline, the replay's reset of
// the whole database dropped that table (stokaro/ptah#3808), and the
// rehearsal's reset of the pinned schema refused the extension
// (stokaro/ptah#3810). Each test reads the dev database back afterwards.

// pinnedDevURL is url with search_path set to schema, so the dev database's
// scope is that schema.
func pinnedDevURL(c *qt.C, rawURL, schema string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// devBaselineVerbs are the verbs that reset a dev database, spelled with a
// run's databases and files.
var devBaselineVerbs = []struct {
	name string
	args []string
}{
	{name: "migrate diff", args: []string{"migrate", "diff", "x", "--dir", "{out}", "--to", "{schema}", "--dev-url", "{dev}"}},
	{name: "migrate lint", args: []string{"migrate", "lint", "--dir", "{dir}", "--dev-url", "{dev}", "--latest", "1"}},
	{name: "migrate validate", args: []string{"migrate", "validate", "--dir", "{dir}", "--dev-url", "{dev}"}},
	{name: "schema inspect", args: []string{"schema", "inspect", "-u", "{schema}", "--dev-url", "{dev}"}},
	{name: "schema diff", args: []string{"schema", "diff", "--from", "{schema}", "--to", "{target}", "--dev-url", "{dev}"}},
	{name: "schema apply", args: []string{"schema", "apply", "-u", "{target}", "--to", "{schema}", "--dev-url", "{dev}", "--auto-approve"}},
}

// TestCompatVerbsKeepASchemaOutsideThePinnedDevSchemaE2E runs every verb that
// resets a dev database whose URL pins public, while another schema of it
// holds a table with a row. The table and its row survive, and no output
// names the schema: the run reads the pinned schema alone.
func TestCompatVerbsKeepASchemaOutsideThePinnedDevSchemaE2E(t *testing.T) {
	for _, verb := range devBaselineVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
			c.Assert(atlasschema.ApplySQL(c.Context(), dev.conn, migrator.MigrationTxModeNone,
				"DROP TABLE kept;\nCREATE SCHEMA keep_other;\n"+
					"CREATE TABLE keep_other.kept (id int NOT NULL, PRIMARY KEY (id));\n"+
					"INSERT INTO keep_other.kept (id) VALUES (1);\n"), qt.IsNil)
			target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
			schemaFile, dir := writeDevIdentitySources(c)
			out := filepath.Join(c.TempDir(), "out")
			c.Assert(os.MkdirAll(out, 0o755), qt.IsNil)

			output, err := runCompatVerb(devNotCleanArgs(verb.args, pinnedDevURL(c, dev.url, "public"), target.url, schemaFile, dir, out)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			c.Assert(output, qt.Not(qt.Contains), "keep_other")
			var rows int
			c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT count(*) FROM keep_other.kept").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 1)
		})
	}
}

// TestCompatVerbsKeepPublicOutsideThePinnedDevSchemaE2E pins a schema other
// than public while public holds `kept` with a row. public is outside the scope
// as any other schema is, so every verb leaves the table and its row alone.
// The target pins the same schema, which `schema apply` requires.
func TestCompatVerbsKeepPublicOutsideThePinnedDevSchemaE2E(t *testing.T) {
	for _, verb := range devBaselineVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
			c.Assert(atlasschema.ApplySQL(c.Context(), dev.conn, migrator.MigrationTxModeNone, "CREATE SCHEMA app;\n"), qt.IsNil)
			target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
			c.Assert(atlasschema.ApplySQL(c.Context(), target.conn, migrator.MigrationTxModeNone, "CREATE SCHEMA app;\n"), qt.IsNil)
			schemaFile, dir := writeDevIdentitySources(c)
			out := filepath.Join(c.TempDir(), "out")
			c.Assert(os.MkdirAll(out, 0o755), qt.IsNil)
			args := devNotCleanArgs(verb.args, pinnedDevURL(c, dev.url, "app"), pinnedDevURL(c, target.url, "app"), schemaFile, dir, out)

			output, err := runCompatVerb(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			c.Assert(dev.keptRows(c), qt.Equals, 1)
		})
	}
}

// TestReplayRemovesASchemaItCreatesBesideAKeptOneE2E is the control: a schema
// the run creates is not in the baseline, and the cleanup after the replay
// removes it while the kept schema stays.
func TestReplayRemovesASchemaItCreatesBesideAKeptOneE2E(t *testing.T) {
	c := qt.New(t)
	dev := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
	c.Assert(atlasschema.ApplySQL(c.Context(), dev.conn, migrator.MigrationTxModeNone,
		"DROP TABLE kept;\nCREATE SCHEMA keep_other;\nCREATE TABLE keep_other.kept (id int NOT NULL, PRIMARY KEY (id));\n"), qt.IsNil)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_audit.sql"),
		[]byte("CREATE SCHEMA audit;\nCREATE TABLE audit.x (id int NOT NULL, PRIMARY KEY (id));\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)

	output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", pinnedDevURL(c, dev.url, "public"))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	var schemas string
	c.Assert(dev.conn.QueryRowContext(c.Context(), `
		SELECT string_agg(nspname, ',' ORDER BY nspname)
		FROM pg_namespace
		WHERE nspname IN ('audit', 'keep_other')`).Scan(&schemas), qt.IsNil)
	c.Assert(schemas, qt.Equals, "keep_other")
}

// installedExtensions lists the extensions a database holds besides plpgsql.
func installedExtensions(c *qt.C, target devIdentityTarget) string {
	c.Helper()
	var names string
	c.Assert(target.conn.QueryRowContext(c.Context(),
		"SELECT coalesce(string_agg(extname, ',' ORDER BY extname), '') FROM pg_extension WHERE extname <> 'plpgsql'").Scan(&names), qt.IsNil)
	return names
}

// TestSchemaApplyRehearsesBesideAnExtensionInThePinnedDevSchemaE2E rehearses a
// plan on a dev database whose URL pins public, where citext is installed. The
// rehearsal resets public and keeps citext, and the target is applied. Native
// and Atlas-compatible verbs rehearse through the same code.
func TestSchemaApplyRehearsesBesideAnExtensionInThePinnedDevSchemaE2E(t *testing.T) {
	tests := []struct {
		name string
		// run is the binary the row drives, in process.
		run  func(args ...string) (string, error)
		args []string
	}{
		{
			name: "ptah-compat schema apply",
			run:  runCompatVerb,
			args: []string{"schema", "apply", "-u", "{target}", "--to", "{schema}", "--dev-url", "{dev}", "--auto-approve"},
		},
		{
			name: "ptah schema apply",
			run:  runPtahNativeWithError,
			args: []string{"schema", "apply", "--db-url", "{target}", "--schema-file", "{rawschema}", "--dev-url", "{dev}", "--auto-approve"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
			c.Assert(atlasschema.ApplySQL(c.Context(), dev.conn, migrator.MigrationTxModeNone,
				"DROP TABLE kept;\nCREATE EXTENSION citext SCHEMA public;\n"), qt.IsNil)
			target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
			schemaFile, dir := writeDevIdentitySources(c)
			args := devNotCleanArgs(test.args, pinnedDevURL(c, dev.url, "public"), target.url, schemaFile, dir, "")

			output, err := test.run(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			c.Assert(installedExtensions(c, dev), qt.Equals, "citext")
			c.Assert(target.keptRows(c), qt.Equals, 1)
			var added int
			c.Assert(target.conn.QueryRowContext(c.Context(),
				"SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'added'").Scan(&added), qt.IsNil)
			c.Assert(added, qt.Equals, 1)
		})
	}
}

// TestRehearsePlanStatementsVerifiesBesideAnExtensionInThePinnedDevSchemaLive
// is the plan-file path, which compares the rehearsed dev database with the
// desired state after it runs. The kept extension is the dev database's, not a
// difference from the desired state.
func TestRehearsePlanStatementsVerifiesBesideAnExtensionInThePinnedDevSchemaLive(t *testing.T) {
	c := qt.New(t)
	dev := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
	c.Assert(atlasschema.ApplySQL(c.Context(), dev.conn, migrator.MigrationTxModeNone,
		"DROP TABLE kept;\nCREATE EXTENSION citext SCHEMA public;\n"), qt.IsNil)
	target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
	schemaFile, _ := writeDevIdentitySources(c)
	desired, err := schemafile.LoadPath(schemaFile, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "postgres"})
	c.Assert(err, qt.IsNil)

	err = atlasschema.RehearsePlanStatements(c.Context(), target.conn,
		[]string{`CREATE TABLE "added" ("id" integer NOT NULL, PRIMARY KEY ("id"))`},
		desired,
		atlasschema.PlanRehearsalOptions{DevURL: pinnedDevURL(c, dev.url, "public"), TargetURL: target.url, TxMode: migrator.MigrationTxModeFile, Runtime: must.Must(builtin.New())})

	c.Assert(err, qt.IsNil)
	c.Assert(installedExtensions(c, dev), qt.Equals, "citext")
	c.Assert(installedExtensions(c, target), qt.Not(qt.Contains), "citext")
}
