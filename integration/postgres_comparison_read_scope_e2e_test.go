//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// Every verb that compares a desired schema with a live database reads the
// schemas `schema apply` reads for the same URL (stokaro/ptah#5030). The
// database holds public.a, reporting.b and extra.c, and the desired schema
// declares public.a unchanged and reporting.b with a new column. A verb that
// reads only the connection's default schema finds no reporting.b, plans
// `CREATE TABLE "reporting"."b"` for a table that exists, and the migration
// `migrations generate` writes fails with 42P07.

// readScopeDeclared is the desired schema both tests compare with.
const readScopeDeclared = `CREATE SCHEMA reporting;
CREATE TABLE public.a (id integer PRIMARY KEY);
CREATE TABLE reporting.b (id integer PRIMARY KEY, note text);
`

const (
	readScopeAddNote  = `ALTER TABLE "reporting"."b" ADD COLUMN "note" text;`
	readScopeDropC    = `DROP TABLE IF EXISTS "extra"."c" CASCADE;`
	readScopeCreateB  = `CREATE TABLE "reporting"."b"`
	readScopeSynced   = "No schema differences detected.\n"
	readScopeNoDrift  = "No schema drift detected.\n"
	readScopeNoPlan   = "Safety: no executable migration statements\n"
	readScopeNoChange = "no migration files generated"
)

// TestComparisonVerbsReadWhatApplyReadsE2E runs every comparison verb over a
// URL that pins no search_path. Each plans what `schema apply` plans: the new
// column on reporting.b, and the drop of extra.c, which the desired schema
// does not describe. The migration `generate` writes applies, and every verb
// run again plans nothing.
func TestComparisonVerbsReadWhatApplyReadsE2E(t *testing.T) {
	c := qt.New(t)
	database := newReadScopeDatabase(c)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(readScopeDeclared), 0o600), qt.IsNil)
	migrations := c.TempDir()

	applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")
	compared := runPtahIn(c, "schema", "compare", "--db-url", database, "--schema-file", schema)
	planned := runPtahIn(c, "migrations", "plan", "--db-url", database, "--schema-file", schema)
	drifted := runPtahIn(c, "schema", "drift", "--db-url", database, "--schema-file", schema)
	generated := runPtahIn(c, "migrations", "generate", "--db-url", database, "--schema-file", schema, "--migrations-dir", migrations)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, readScopeAddNote)
	c.Assert(applied.Stdout, qt.Contains, readScopeDropC)
	c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
	c.Assert(compared.Stdout, qt.Contains, readScopeAddNote)
	c.Assert(compared.Stdout, qt.Contains, readScopeDropC)
	c.Assert(compared.Stdout, qt.Not(qt.Contains), readScopeCreateB)
	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(planned.Stdout, qt.Contains, readScopeAddNote)
	c.Assert(planned.Stdout, qt.Contains, readScopeDropC)
	c.Assert(planned.Stdout, qt.Not(qt.Contains), readScopeCreateB)
	c.Assert(drifted.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", drifted.Stdout, drifted.Stderr))
	c.Assert(drifted.Stdout, qt.Contains, "- tables_removed: 1 (destructive)\n")
	c.Assert(drifted.Stdout, qt.Contains, "- columns_added: 1 (warning)\n")
	c.Assert(drifted.Stdout, qt.Not(qt.Contains), "tables_added")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := readOneUpMigration(c, migrations)
	c.Assert(up, qt.Contains, readScopeAddNote)
	c.Assert(up, qt.Contains, readScopeDropC)
	c.Assert(up, qt.Not(qt.Contains), readScopeCreateB)

	migrated := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", migrations, "--allow-destructive")

	c.Assert(migrated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", migrated.Stdout, migrated.Stderr))
	c.Assert(readScopeTables(c, database), qt.Equals, "public.a(id), reporting.b(id,note)")

	regenerated := runPtahIn(c, "migrations", "generate", "--db-url", database, "--schema-file", schema, "--migrations-dir", migrations)
	recompared := runPtahIn(c, "schema", "compare", "--db-url", database, "--schema-file", schema, "--exit-code")
	replanned := runPtahIn(c, "migrations", "plan", "--db-url", database, "--schema-file", schema)
	redrifted := runPtahIn(c, "schema", "drift", "--db-url", database, "--schema-file", schema)
	reapplied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

	c.Assert(regenerated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", regenerated.Stdout, regenerated.Stderr))
	c.Assert(regenerated.Stdout, qt.Contains, readScopeNoChange)
	c.Assert(recompared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", recompared.Stdout, recompared.Stderr))
	c.Assert(recompared.Stdout, qt.Contains, readScopeSynced)
	c.Assert(replanned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replanned.Stdout, replanned.Stderr))
	c.Assert(replanned.Stdout, qt.Contains, readScopeNoPlan)
	c.Assert(redrifted.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", redrifted.Stdout, redrifted.Stderr))
	c.Assert(redrifted.Stdout, qt.Equals, readScopeNoDrift)
	c.Assert(reapplied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied.Stdout, reapplied.Stderr))
	c.Assert(reapplied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// TestComparisonVerbsReadTheDeclaredSchemasBeyondAPinnedPathE2E pins the URL
// to public. The read covers reporting too, because the desired schema names
// it, and leaves extra alone, because neither the URL nor the desired schema
// does.
func TestComparisonVerbsReadTheDeclaredSchemasBeyondAPinnedPathE2E(t *testing.T) {
	c := qt.New(t)
	database := newReadScopeDatabase(c)
	target := withURLParameter(database, "search_path", "public")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(readScopeDeclared), 0o600), qt.IsNil)
	migrations := c.TempDir()

	compared := runPtahIn(c, "schema", "compare", "--db-url", target, "--schema-file", schema)
	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", migrations)

	c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
	c.Assert(compared.Stdout, qt.Contains, readScopeAddNote)
	c.Assert(compared.Stdout, qt.Not(qt.Contains), readScopeCreateB)
	c.Assert(compared.Stdout, qt.Not(qt.Contains), `"extra"`)
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := readOneUpMigration(c, migrations)
	c.Assert(up, qt.Contains, readScopeAddNote)
	c.Assert(up, qt.Not(qt.Contains), `"extra"`)

	migrated := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", migrations)

	c.Assert(migrated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", migrated.Stdout, migrated.Stderr))
	c.Assert(readScopeTables(c, database), qt.Equals, "extra.c(id), public.a(id), reporting.b(id,note)")

	regenerated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", migrations)
	recompared := runPtahIn(c, "schema", "compare", "--db-url", target, "--schema-file", schema, "--exit-code")

	c.Assert(regenerated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", regenerated.Stdout, regenerated.Stderr))
	c.Assert(regenerated.Stdout, qt.Contains, readScopeNoChange)
	c.Assert(recompared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", recompared.Stdout, recompared.Stderr))
	c.Assert(recompared.Stdout, qt.Contains, readScopeSynced)
}

// TestGenerateVerifiesAMultiSchemaDeclarationOnAShadowE2E generates the first
// migration of an empty database with --shadow-db. The shadow check replays
// the migration and compares the shadow with the desired schema, so it reads
// the shadow the way the target is read: a read of public alone finds no
// reporting.b and refuses the migration with `missing table reporting.b`.
func TestGenerateVerifiesAMultiSchemaDeclarationOnAShadowE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "target")
	shadow := newEmptyReadScopeDatabase(c, "shadow")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(readScopeDeclared), 0o600), qt.IsNil)
	migrations := c.TempDir()
	generate := []string{"migrations", "generate", "--db-url", database, "--shadow-db", shadow, "--schema-file", schema, "--migrations-dir", migrations}

	generated := runPtahIn(c, generate...)

	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	c.Assert(readOneUpMigration(c, migrations), qt.Contains, readScopeCreateB)

	migrated := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", migrations)

	c.Assert(migrated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", migrated.Stdout, migrated.Stderr))
	c.Assert(readScopeTables(c, database), qt.Equals, "public.a(id), reporting.b(id,note)")

	regenerated := runPtahIn(c, generate...)

	c.Assert(regenerated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", regenerated.Stdout, regenerated.Stderr))
	c.Assert(regenerated.Stdout, qt.Contains, readScopeNoChange)
}

// newReadScopeDatabase creates a database, dropped when the test ends, holding
// public.a, reporting.b and extra.c, and returns its URL, which pins no
// search_path.
func newReadScopeDatabase(c *qt.C) string {
	c.Helper()
	database := newEmptyReadScopeDatabase(c, "tables")
	for _, statement := range []string{
		"CREATE SCHEMA reporting",
		"CREATE SCHEMA extra",
		"CREATE TABLE public.a (id integer PRIMARY KEY)",
		"CREATE TABLE reporting.b (id integer PRIMARY KEY)",
		"CREATE TABLE extra.c (id integer PRIMARY KEY)",
	} {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	return database
}

// newEmptyReadScopeDatabase creates an empty database, dropped when the test
// ends, and returns its URL, which pins no search_path.
func newEmptyReadScopeDatabase(c *qt.C, role string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin := openReadScopeDatabase(c, adminURL)
	name := fmt.Sprintf("ptah_read_scope_%s_%d", role, time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	return replaceDatabaseName(c, adminURL, name)
}

// openReadScopeDatabase opens a connection closed when the test ends.
func openReadScopeDatabase(c *qt.C, database string) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, database))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

// readScopeTables lists the user tables outside the revision bookkeeping as
// schema.table(columns), in name order.
func readScopeTables(c *qt.C, database string) string {
	c.Helper()
	var tables string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(t.table_schema || '.' || t.table_name || '(' || (
			SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
			FROM information_schema.columns k
			WHERE k.table_schema = t.table_schema AND k.table_name = t.table_name) || ')', ', '
			ORDER BY t.table_schema, t.table_name), '')
		FROM information_schema.tables t
		WHERE t.table_schema IN ('public', 'reporting', 'extra')
		  AND t.table_type = 'BASE TABLE'
		  AND t.table_name NOT LIKE 'schema_migrations%'
		  AND t.table_name <> 'ptah_migration_tags'`).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables
}

// readOneUpMigration returns the body of the one up migration in dir.
func readOneUpMigration(c *qt.C, dir string) string {
	c.Helper()
	ups, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1)
	body, err := os.ReadFile(ups[0])
	c.Assert(err, qt.IsNil)
	return string(body)
}
