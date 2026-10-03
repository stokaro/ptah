//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A dev database's extensions are its environment: the claim keeps them, and
// every reset leaves them installed. The comparisons that read the dev
// database back counted them as the replay's state, so a desired schema that
// does not declare one planned `DROP EXTENSION` for it, and the next replay
// executed that drop on the dev database (stokaro/ptah#4070). Measured on
// PostgreSQL 18 with hstore installed in the dev database, the pinned
// community binary v1.3.0 plans only the table. The Supabase image installs
// five extensions this way. Each test reads pg_extension back afterwards.

// extensionsDev returns the URL of a scratch dev database pinned to public that
// holds hstore, and a connection to it.
func extensionsDev(c *qt.C) (string, *sql.DB) {
	c.Helper()
	devURL := postgresScratchDevURL(c, "ptah_env_ext_dev")
	db, err := sql.Open("pgx", devURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), "CREATE EXTENSION hstore SCHEMA public")
	c.Assert(err, qt.IsNil)
	return pinnedDevURL(c, devURL, "public"), db
}

// installedExtensionNames lists the extensions db holds besides plpgsql.
func installedExtensionNames(c *qt.C, db *sql.DB) string {
	c.Helper()
	var names string
	c.Assert(db.QueryRowContext(c.Context(),
		"SELECT coalesce(string_agg(extname, ',' ORDER BY extname), '') FROM pg_extension WHERE extname <> 'plpgsql'").Scan(&names), qt.IsNil)
	return names
}

// writtenMigrations joins every file the directory holds.
func writtenMigrations(c *qt.C, dir string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	var all strings.Builder
	for _, file := range files {
		body, err := os.ReadFile(file)
		c.Assert(err, qt.IsNil)
		all.Write(body)
	}
	return all.String()
}

// TestDevEnvironmentExtensionsE2E_MigrateDiffLeavesThem runs `migrate diff`
// twice against the dev database, once with a desired schema that does not
// declare hstore and once with one that does. Neither run writes a statement
// for hstore, the second run of each is in sync, and hstore is still
// installed.
func TestDevEnvironmentExtensionsE2E_MigrateDiffLeavesThem(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{name: "undeclared", schema: "CREATE TABLE t (id int);\n"},
		{name: "declared", schema: "CREATE EXTENSION hstore;\nCREATE TABLE t (id int);\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev, db := extensionsDev(c)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.schema), 0o600), qt.IsNil)
			dir := c.TempDir()
			diff := func(name string) []string {
				return []string{"migrate", "diff", name, "--dir", "file://" + filepath.ToSlash(dir),
					"--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev}
			}

			first, err := runCompatVerb(diff("init")...)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
			second, err := runCompatVerb(diff("second")...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
			c.Assert(second, qt.Contains, "The migration directory is synced with the desired state")
			c.Assert(writtenMigrations(c, dir), qt.Contains, `CREATE TABLE`)
			c.Assert(writtenMigrations(c, dir), qt.Not(qt.Contains), "EXTENSION")
			c.Assert(installedExtensionNames(c, db), qt.Equals, "hstore")
		})
	}
}

// TestDevEnvironmentExtensionsE2E_NativeGenerateLeavesThem is the native
// verb: `ptah migrations generate --replay` writes the table and nothing
// about hstore, and leaves it installed.
func TestDevEnvironmentExtensionsE2E_NativeGenerateLeavesThem(t *testing.T) {
	c := qt.New(t)
	dev, db := extensionsDev(c)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id int);\n"), 0o600), qt.IsNil)
	dir := c.TempDir()

	runPtahNative(c, "migrations", "generate", "--replay", "--dev-url", dev, "--schema-file", schema,
		"--migrations-dir", dir, "--name", "init")

	c.Assert(writtenMigrations(c, dir), qt.Contains, `CREATE TABLE`)
	c.Assert(writtenMigrations(c, dir), qt.Not(qt.Contains), "EXTENSION")
	c.Assert(installedExtensionNames(c, db), qt.Equals, "hstore")
}

// TestDevEnvironmentExtensionsE2E_SchemaDiffFromADirectoryLeavesThem compares
// a migration directory, replayed on the dev database, with a schema file.
// hstore is the dev database's, so the comparison plans nothing for it.
func TestDevEnvironmentExtensionsE2E_SchemaDiffFromADirectoryLeavesThem(t *testing.T) {
	c := qt.New(t)
	dev, db := extensionsDev(c)
	schemaFile, dir := writeDevIdentitySources(c)

	output, err := runCompatVerb("schema", "diff", "--from", "file://"+filepath.ToSlash(dir),
		"--to", "file://"+filepath.ToSlash(schemaFile), "--dev-url", dev)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Not(qt.Contains), "EXTENSION")
	c.Assert(installedExtensionNames(c, db), qt.Equals, "hstore")
}

// TestDevEnvironmentExtensionsE2E_SchemaApplyFromADirectoryLeavesThem applies
// a migration directory, replayed on the dev database, to a target that does
// not hold hstore. The directory never created it, so the target does not get
// it, and the dev database keeps it.
func TestDevEnvironmentExtensionsE2E_SchemaApplyFromADirectoryLeavesThem(t *testing.T) {
	c := qt.New(t)
	dev, db := extensionsDev(c)
	target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
	_, dir := writeDevIdentitySources(c)

	output, err := runCompatVerb("schema", "apply", "-u", pinnedDevURL(c, target.url, "public"),
		"--to", "file://"+filepath.ToSlash(dir), "--dev-url", dev, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Not(qt.Contains), "EXTENSION")
	var targetExtensions int
	c.Assert(target.conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM pg_extension WHERE extname = 'hstore'").Scan(&targetExtensions), qt.IsNil)
	c.Assert(targetExtensions, qt.Equals, 0)
	c.Assert(installedExtensionNames(c, db), qt.Equals, "hstore")
}

// TestDevEnvironmentExtensionsE2E_SchemaInspectLeavesThemOut inspects a schema
// file and a migration directory through the dev database. What the output
// describes is the source, so the dev database's hstore is not in it.
func TestDevEnvironmentExtensionsE2E_SchemaInspectLeavesThemOut(t *testing.T) {
	sources := []struct {
		name string
		// source picks the file or the directory writeDevIdentitySources wrote.
		source func(schemaFile, dir string) string
	}{
		{name: "schema file", source: func(schemaFile, _ string) string { return schemaFile }},
		{name: "migration directory", source: func(_, dir string) string { return dir }},
	}

	for _, test := range sources {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev, db := extensionsDev(c)
			schemaFile, dir := writeDevIdentitySources(c)

			output, err := runCompatVerb("schema", "inspect", "-u", "file://"+filepath.ToSlash(test.source(schemaFile, dir)),
				"--dev-url", dev, "--format", "{{ sql . }}")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			c.Assert(output, qt.Contains, `CREATE TABLE`)
			c.Assert(output, qt.Not(qt.Contains), "hstore")
			c.Assert(installedExtensionNames(c, db), qt.Equals, "hstore")
		})
	}
}

// TestDevEnvironmentExtensionsE2E_SchemaDiffFromAMaterializedFileLeavesThem
// compares a schema file with a target database that does not hold hstore.
// The file is created on the dev database and read back before the
// comparison, and that read holds the dev database's hstore, which the file
// never declared, so the comparison plans nothing for it.
func TestDevEnvironmentExtensionsE2E_SchemaDiffFromAMaterializedFileLeavesThem(t *testing.T) {
	c := qt.New(t)
	dev, db := extensionsDev(c)
	target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
	schemaFile, _ := writeDevIdentitySources(c)

	output, err := runCompatVerb("schema", "diff", "--from", "file://"+filepath.ToSlash(schemaFile),
		"--to", pinnedDevURL(c, target.url, "public"), "--dev-url", dev)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Not(qt.Contains), "EXTENSION")
	c.Assert(installedExtensionNames(c, db), qt.Equals, "hstore")
}
