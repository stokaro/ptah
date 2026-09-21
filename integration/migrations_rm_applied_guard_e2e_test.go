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
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/testutils"
)

// rmGuardMigration is one version of the fixture history, written as the two
// files `ptah migrations rm` deletes together.
type rmGuardMigration struct {
	name  string
	table string
}

// rmGuardMigrations is the history this run applies and then tries to rewrite.
// Two versions, because the refusal only means something beside a version the
// same command removes: one applied, one pending.
var rmGuardMigrations = []rmGuardMigration{
	{name: "0000000001_create_widgets", table: "widgets"},
	{name: "0000000002_create_gadgets", table: "gadgets"},
}

// TestMigrationsRmAppliedGuardPostgresE2E drives the built `ptah` binary against
// a live PostgreSQL server to exercise the applied-migration guard that
// `migrations rm | edit | rebase` share.
//
// Offline the guard cannot be measured at all. Without --db-url it prints
// "applied migration state was not verified" and returns, so every test that
// omits the flag measures the warning rather than the refusal. With the flag it
// connects, and the answer comes from the server: the guard reads through
// GetAppliedMigrations, which calls Initialize first, so a revision table it
// cannot find is created empty and reports that nothing is applied. Whether
// --migrations-schema and --migrations-table reach that read is therefore the
// difference between a guard that refuses and a guard that deletes applied
// history, and only a database that holds the history under those names can say
// which one shipped.
//
// The history lives in a schema of this run's own, so the assertions describe
// objects no other test writes to.
func TestMigrationsRmAppliedGuardPostgresE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	// Neither name is a default: the revision table Ptah reaches for when
	// nothing is configured is public.schema_migrations, and a guard that
	// silently fell back to it would find an empty table here and allow the
	// removal this test asserts is refused.
	schema := fmt.Sprintf("ptah_rm_guard_%d", time.Now().UnixNano())
	const revisionTable = "applied_history"
	createRmGuardSchema(c, ctx, db, schema)
	defer dropRmGuardSchema(c, context.Background(), db, schema)

	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binary := filepath.Join(workDir, "ptah"+testutils.ExecutableSuffix)
	buildPtah(c, ctx, repoRoot, binary)

	migrationsDir := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o750), qt.IsNil)
	for _, migration := range rmGuardMigrations {
		writeRmGuardMigration(c, migrationsDir, schema, migration)
	}

	_, hashStderr, hashErr := runCLIProcess(ctx, workDir, binary,
		"migrations", "hash", "--dir", migrationsDir)
	c.Assert(exitStatusOf(c, hashErr), qt.Equals, 0, qt.Commentf("stderr:\n%s", hashStderr))

	upStdout, upStderr, upErr := runCLIProcess(ctx, workDir, binary,
		"migrations", "up",
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
		"--migrations-schema", schema,
		"--migrations-table", revisionTable,
		"--limit", "1")
	c.Assert(exitStatusOf(c, upErr), qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", upStdout, upStderr))

	// The precondition, read back from the server rather than assumed: version 1
	// is recorded as applied under the configured names, and version 2 is not.
	c.Assert(rmGuardAppliedVersions(c, ctx, db, schema, revisionTable), qt.DeepEquals, []int64{1})
	// The log table sits beside the revision table it was named after, so a
	// configured revision name moves both.
	c.Assert(rmGuardSchemaTables(c, ctx, db, schema), qt.DeepEquals,
		[]string{revisionTable, revisionTable + "_log", "widgets"})

	t.Run("the applied version is refused", func(t *testing.T) {
		c := qt.New(t)

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"migrations", "rm",
			"--version", "1",
			"--migrations-dir", migrationsDir,
			"--db-url", dbURL,
			"--migrations-schema", schema,
			"--migrations-table", revisionTable)

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 2)
		c.Assert(stderr, qt.Equals,
			"error: migration version 1 is already applied; refusing to modify applied history (use --force to override)\n")
		// The branch taken, asserted as well as the outcome: the warning belongs
		// to the run that never looked at a database, and a guard that printed
		// it here would be refusing for some other reason.
		c.Assert(stdout, qt.Equals, "")

		for _, name := range []string{"0000000001_create_widgets.up.sql", "0000000001_create_widgets.down.sql"} {
			_, statErr := os.Stat(filepath.Join(migrationsDir, name))
			c.Assert(statErr, qt.IsNil, qt.Commentf("refused run removed %s", name))
		}
		// The refusal is not a rewrite either: the integrity file still
		// describes the directory it was written for.
		_, verifyStderr, verifyErr := runCLIProcess(ctx, workDir, binary,
			"migrations", "validate", "--dir", migrationsDir)
		c.Assert(exitStatusOf(c, verifyErr), qt.Equals, 0, qt.Commentf("stderr:\n%s", verifyStderr))
	})

	// The control. Same command, same connection, same revision table: only the
	// version differs, so the refusal above is the applied state and not a guard
	// that refuses whatever it is given once --db-url is present.
	t.Run("the pending version is removed", func(t *testing.T) {
		c := qt.New(t)

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"migrations", "rm",
			"--version", "2",
			"--migrations-dir", migrationsDir,
			"--db-url", dbURL,
			"--migrations-schema", schema,
			"--migrations-table", revisionTable)

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
		c.Assert(stdout, qt.Contains, "0000000002_create_gadgets.up.sql")
		c.Assert(stdout, qt.Not(qt.Contains), "applied migration state was not verified")

		for _, name := range []string{"0000000002_create_gadgets.up.sql", "0000000002_create_gadgets.down.sql"} {
			_, statErr := os.Stat(filepath.Join(migrationsDir, name))
			c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("%s survived the removal", name))
		}
		for _, name := range []string{"0000000001_create_widgets.up.sql", "0000000001_create_widgets.down.sql"} {
			_, statErr := os.Stat(filepath.Join(migrationsDir, name))
			c.Assert(statErr, qt.IsNil, qt.Commentf("removing version 2 took %s with it", name))
		}
	})

	// Nothing either run wrote to the database. A read that creates its own
	// revision table is the failure this whole test is about, so the schema is
	// read back afterwards: the same tables, and the same one applied row.
	c.Assert(rmGuardAppliedVersions(c, ctx, db, schema, revisionTable), qt.DeepEquals, []int64{1})
	c.Assert(rmGuardSchemaTables(c, ctx, db, schema), qt.DeepEquals,
		[]string{revisionTable, revisionTable + "_log", "widgets"})
}

// writeRmGuardMigration writes one version's up/down pair. Every statement
// names the run's own schema, so the history the guard reads and the objects it
// creates live and die together.
func writeRmGuardMigration(c *qt.C, dir, schema string, migration rmGuardMigration) {
	c.Helper()
	qualified := quoteE2EIdent(schema) + "." + quoteE2EIdent(migration.table)
	c.Assert(os.WriteFile(
		filepath.Join(dir, migration.name+".up.sql"),
		[]byte("CREATE TABLE "+qualified+" (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, migration.name+".down.sql"),
		[]byte("DROP TABLE "+qualified+";\n"),
		0o600,
	), qt.IsNil)
}

// createRmGuardSchema creates the schema this run keeps its history and its
// tables in.
func createRmGuardSchema(c *qt.C, ctx context.Context, db *sql.DB, schema string) {
	c.Helper()
	_, err := db.ExecContext(ctx, "CREATE SCHEMA "+quoteE2EIdent(schema))
	c.Assert(err, qt.IsNil)
}

// dropRmGuardSchema removes the schema and everything the run put in it.
func dropRmGuardSchema(c *qt.C, ctx context.Context, db *sql.DB, schema string) {
	c.Helper()
	_, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteE2EIdent(schema)+" CASCADE")
	c.Assert(err, qt.IsNil)
}

// rmGuardAppliedVersions reads the applied versions out of the revision table
// the run was pointed at, which is the answer the guard is supposed to be
// reading.
func rmGuardAppliedVersions(c *qt.C, ctx context.Context, db *sql.DB, schema, table string) []int64 {
	c.Helper()
	rows, err := db.QueryContext(ctx,
		"SELECT version FROM "+quoteE2EIdent(schema)+"."+quoteE2EIdent(table)+
			" WHERE state = 'applied' ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		version := int64(0)
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

// rmGuardSchemaTables names every table the schema holds, so a revision table a
// command created for itself shows up as an extra name.
func rmGuardSchemaTables(c *qt.C, ctx context.Context, db *sql.DB, schema string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name", schema)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var names []string
	for rows.Next() {
		name := ""
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
