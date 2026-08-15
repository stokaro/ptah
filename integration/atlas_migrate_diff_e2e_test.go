//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestAtlasMigrateDiffConcurrentIndexAndQualifierE2E runs the real ptah-compat
// binary against a disposable PostgreSQL database and verifies the
// Atlas-compatible migrate diff behavior added for issue #815:
// concurrent-index migration-file metadata (`-- atlas:txmode none`), the
// transactional/concurrent safety split, atomic atlas.sum updates, and
// --qualifier artifact naming.
func TestAtlasMigrateDiffConcurrentIndexAndQualifierE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "atlas")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_diff_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	testDBURL := replaceDatabaseName(c, dbURL, testDBName)
	devDB, err := sql.Open("pgx", testDBURL)
	c.Assert(err, qt.IsNil)
	defer devDB.Close()
	_, err = devDB.ExecContext(ctx, `
CREATE VIEW stale_dev_view AS SELECT 1 AS value;
CREATE MATERIALIZED VIEW stale_dev_materialized_view AS SELECT 1 AS value;
CREATE FUNCTION stale_dev_function() RETURNS integer LANGUAGE sql AS 'SELECT 1';
CREATE PROCEDURE stale_dev_procedure() LANGUAGE sql AS 'SELECT 1';
CREATE AGGREGATE stale_dev_aggregate(integer) (
	SFUNC = int4pl,
	STYPE = integer,
	INITCOND = '0'
);
CREATE SEQUENCE stale_dev_sequence;
CREATE DOMAIN stale_dev_domain AS text;
CREATE TYPE stale_dev_composite AS (value integer);
CREATE TYPE z_stale_dev_range AS RANGE (
	subtype = integer,
	multirange_type_name = a_stale_dev_multirange
);
`)
	c.Assert(err, qt.IsNil)
	t.Run("database realm cleans dependencies across schemas", func(t *testing.T) {
		c := qt.New(t)
		_, err := devDB.ExecContext(ctx, `
CREATE TABLE public.stale_dependency_parent (id integer PRIMARY KEY);
CREATE SCHEMA audit;
CREATE VIEW audit.external_parent_view AS
	SELECT id FROM public.stale_dependency_parent;
CREATE TABLE audit.external_child (
	id integer PRIMARY KEY,
	parent_id integer REFERENCES public.stale_dependency_parent(id)
);
`)
		c.Assert(err, qt.IsNil)
		defer func() {
			_, cleanupErr := devDB.ExecContext(
				context.Background(),
				"DROP SCHEMA IF EXISTS audit CASCADE; DROP TABLE IF EXISTS public.stale_dependency_parent",
			)
			c.Check(cleanupErr, qt.IsNil)
		}()
		dir := t.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		schemaPath := filepath.Join(dir, "schema.sql")
		c.Assert(os.WriteFile(
			schemaPath,
			[]byte("CREATE TABLE desired_items (id BIGSERIAL PRIMARY KEY);\n"),
			0o600,
		), qt.IsNil)

		output, err := runPtah(ctx, dir, binaryPath,
			"migrate", "diff",
			"--to", "file://"+schemaPath,
			"--dev-url", testDBURL,
			"--dir", "file://"+migrationsDir,
			"database_realm_dependencies")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		var externalObjectCount int
		err = devDB.QueryRowContext(ctx, `
SELECT
	(SELECT COUNT(*) FROM pg_class c
	 JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE n.nspname = 'audit'
	   AND c.relname IN ('external_parent_view', 'external_child')) +
	(SELECT COUNT(*) FROM pg_class c
	 JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE n.nspname = 'public'
	   AND c.relname = 'stale_dependency_parent')
`).Scan(&externalObjectCount)
		c.Assert(err, qt.IsNil)
		c.Assert(externalObjectCount, qt.Equals, 0)
		var auditSchemaCount int
		err = devDB.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM pg_namespace
WHERE nspname = 'audit'
`).Scan(&auditSchemaCount)
		c.Assert(err, qt.IsNil)
		c.Assert(auditSchemaCount, qt.Equals, 0)
		migrationSQL := readFirstMatchingFile(
			c,
			migrationsDir,
			"*_database_realm_dependencies.sql",
		)
		c.Assert(migrationSQL, qt.Contains, "CREATE TABLE")
		c.Assert(migrationSQL, qt.Contains, "desired_items")
	})
	desiredDBName := testDBName + "_desired"
	createE2EDatabase(c, ctx, adminDB, desiredDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, desiredDBName)

	desiredDBURL := replaceDatabaseName(c, dbURL, desiredDBName)
	desiredDB, err := sql.Open("pgx", desiredDBURL)
	c.Assert(err, qt.IsNil)
	defer desiredDB.Close()
	_, err = desiredDB.ExecContext(ctx, `
CREATE SEQUENCE shared_ids;
CREATE TABLE desired_database_items (
	id BIGSERIAL PRIMARY KEY,
	name TEXT NOT NULL
);
CREATE TABLE desired_shared_sequence_items (
	id BIGINT PRIMARY KEY DEFAULT nextval('shared_ids'),
	name TEXT NOT NULL
);
`)
	c.Assert(err, qt.IsNil)

	t.Run("env database desired state generates migration without mutating source", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`variable "desired_url" {}

env "dev" {
  url = var.desired_url
  dev = "`+testDBURL+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

		output, err := runPtah(ctx, dir, binaryPath,
			"migrate", "diff",
			"--config", "file://"+filepath.Join(dir, "atlas.hcl"),
			"--env", "dev",
			"--var", "desired_url="+desiredDBURL,
			"--to", "env://url",
			"--schema", "public",
			"add_database_items")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		migrationSQL := readFirstMatchingFile(c, migrationsDir, "*_add_database_items.sql")
		c.Assert(migrationSQL, qt.Contains, "CREATE TABLE")
		c.Assert(migrationSQL, qt.Contains, "desired_database_items")
		c.Assert(migrationSQL, qt.Contains, "BIGSERIAL")
		c.Assert(migrationSQL, qt.Contains, "CREATE SEQUENCE")
		c.Assert(migrationSQL, qt.Contains, "shared_ids")
		c.Assert(migrationSQL, qt.Contains, "nextval")
		var sourceTableCount int
		queryErr := desiredDB.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'desired_database_items'").
			Scan(&sourceTableCount)
		c.Assert(queryErr, qt.IsNil)
		c.Assert(sourceTableCount, qt.Equals, 1)
		c.Assert(e2eUserTableCount(c, ctx, testDBURL), qt.Equals, 0)
		c.Assert(e2eStaleObjectCount(c, ctx, testDBURL), qt.Equals, 0)

		applyDBName := testDBName + "_env_apply"
		createE2EDatabase(c, ctx, adminDB, applyDBName)
		defer dropE2EDatabase(c, context.Background(), adminDB, applyDBName)
		applyDBURL := replaceDatabaseName(c, dbURL, applyDBName)
		output, err = runPtah(ctx, t.TempDir(), binaryPath,
			"migrate", "apply",
			"--url", applyDBURL,
			"--dir", "file://"+migrationsDir)
		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		c.Assert(e2eTableCount(c, ctx, applyDBURL, "desired_database_items"), qt.Equals, 1)
		c.Assert(e2eTableCount(c, ctx, applyDBURL, "desired_shared_sequence_items"), qt.Equals, 1)

		applyDB, err := sql.Open("pgx", applyDBURL)
		c.Assert(err, qt.IsNil)
		defer applyDB.Close()

		var generatedID int64
		err = applyDB.QueryRowContext(ctx,
			"INSERT INTO desired_database_items (name) VALUES ($1) RETURNING id",
			"generated",
		).Scan(&generatedID)
		c.Assert(err, qt.IsNil)
		c.Assert(generatedID > 0, qt.IsTrue)

		var sharedGeneratedID int64
		err = applyDB.QueryRowContext(ctx,
			"INSERT INTO desired_shared_sequence_items (name) VALUES ($1) RETURNING id",
			"shared",
		).Scan(&sharedGeneratedID)
		c.Assert(err, qt.IsNil)
		c.Assert(sharedGeneratedID > 0, qt.IsTrue)

		var sharedSequenceIsNotSerial bool
		err = applyDB.QueryRowContext(ctx,
			"SELECT pg_get_serial_sequence('public.desired_shared_sequence_items', 'id') IS NULL",
		).Scan(&sharedSequenceIsNotSerial)
		c.Assert(err, qt.IsNil)
		c.Assert(sharedSequenceIsNotSerial, qt.IsTrue)
	})

	t.Run("concurrent index policy splits files and tags txmode none", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"),
			[]byte("CREATE TABLE users (id SERIAL PRIMARY KEY);\n"), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(`CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email TEXT
);
CREATE INDEX idx_users_email ON users (email);
`), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "dev" {
  dev = "`+testDBURL+`"
  schema {
    src = "file://schema.sql"
  }
  migration {
    dir = "file://migrations"
  }
  diff {
    concurrent_index {
      create = true
    }
  }
}
`), 0o600), qt.IsNil)

		// migrate diff refuses a directory nothing has verified (#1086), the
		// same as the community binary, so the seeded 1_init.sql has to carry
		// an atlas.sum before the diff runs. Hashing here rather than writing
		// the sum by hand keeps the fixture honest: the gate below is checking
		// a sum this binary produced.
		hashOutput, err := runPtah(ctx, dir, binaryPath,
			"migrate", "hash", "--dir", "file://"+migrationsDir)
		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", hashOutput))

		output, err := runPtah(ctx, dir, binaryPath, "migrate", "diff", "--env", "dev", "add_email_index")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		transactionalSQL := readFirstMatchingFile(c, migrationsDir, "*_add_email_index_transactional.sql")
		c.Assert(transactionalSQL, qt.Contains, "ADD COLUMN")
		c.Assert(transactionalSQL, qt.Not(qt.Contains), "atlas:txmode")
		c.Assert(transactionalSQL, qt.Not(qt.Contains), "CONCURRENTLY")
		concurrentSQL := readFirstMatchingFile(c, migrationsDir, "*_add_email_index_concurrent_indexes.sql")
		c.Assert(strings.HasPrefix(concurrentSQL, "-- atlas:txmode none\n\n"), qt.IsTrue,
			qt.Commentf("concurrent file:\n%s", concurrentSQL))
		c.Assert(concurrentSQL, qt.Contains, "CREATE INDEX CONCURRENTLY")
		sumContent, readErr := os.ReadFile(filepath.Join(migrationsDir, "atlas.sum"))
		c.Assert(readErr, qt.IsNil)
		c.Assert(string(sumContent), qt.Contains, "_add_email_index_transactional.sql")
		c.Assert(string(sumContent), qt.Contains, "_add_email_index_concurrent_indexes.sql")

		// Round trip: rerunning the diff replays the freshly generated files
		// on the dev database. The txmode-none file executes CREATE INDEX
		// CONCURRENTLY, which PostgreSQL rejects inside a transaction block,
		// so a synced result proves the directive drove non-transactional
		// execution end to end.
		output, err = runPtah(ctx, dir, binaryPath, "migrate", "diff", "--env", "dev", "noop")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		c.Assert(output, qt.Contains, "The migration directory is synced with the desired state")
		c.Assert(e2eUserTableCount(c, ctx, testDBURL), qt.Equals, 0)

		applyDBName := testDBName + "_concurrent_apply"
		createE2EDatabase(c, ctx, adminDB, applyDBName)
		defer dropE2EDatabase(c, context.Background(), adminDB, applyDBName)
		applyDBURL := replaceDatabaseName(c, dbURL, applyDBName)
		output, err = runPtah(ctx, dir, binaryPath,
			"migrate", "apply",
			"--url", applyDBURL,
			"--dir", "file://"+migrationsDir)
		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		c.Assert(e2eIndexIsValid(c, ctx, applyDBURL, "idx_users_email"), qt.IsTrue)
	})

	t.Run("qualifier prefixes generated statements and artifacts apply cleanly", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(`CREATE TABLE items (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);
`), 0o600), qt.IsNil)

		output, err := runPtah(ctx, dir, binaryPath,
			"migrate", "diff",
			"--to", "file://"+filepath.Join(dir, "schema.sql"),
			"--dev-url", testDBURL,
			"--dir", "file://"+migrationsDir,
			"--qualifier", "tenant",
			"add_items")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		migrationSQL := readFirstMatchingFile(c, migrationsDir, "*_add_items.sql")
		c.Assert(migrationSQL, qt.Contains, `CREATE TABLE "tenant"."items"`)

		// The generated artifact applies cleanly to a database that has the
		// qualifier schema, creating the table inside that schema only.
		targetDB, openErr := sql.Open("pgx", testDBURL)
		c.Assert(openErr, qt.IsNil)
		defer targetDB.Close()
		_, execErr := targetDB.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS tenant")
		c.Assert(execErr, qt.IsNil)
		_, execErr = targetDB.ExecContext(ctx, migrationSQL)
		c.Assert(execErr, qt.IsNil)
		var count int
		queryErr := targetDB.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'tenant' AND table_name = 'items'").Scan(&count)
		c.Assert(queryErr, qt.IsNil)
		c.Assert(count, qt.Equals, 1)
	})

	t.Run("invalid qualifier fails without touching the migration directory", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"),
			[]byte("CREATE TABLE items (id SERIAL PRIMARY KEY);\n"), 0o600), qt.IsNil)

		output, err := runPtah(ctx, dir, binaryPath,
			"migrate", "diff",
			"--to", "file://"+filepath.Join(dir, "schema.sql"),
			"--dev-url", testDBURL,
			"--dir", "file://"+migrationsDir,
			"--qualifier", "bad.name",
			"broken")

		c.Assert(err, qt.IsNotNil)
		c.Assert(output, qt.Contains, `invalid --qualifier "bad.name"`)
		entries, readErr := os.ReadDir(migrationsDir)
		c.Assert(readErr, qt.IsNil)
		c.Assert(entries, qt.HasLen, 0)
	})
}

func e2eUserTableCount(c *qt.C, ctx context.Context, dbURL string) int {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var count int
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func e2eTableCount(c *qt.C, ctx context.Context, dbURL, tableName string) int {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var count int
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = $1`, tableName).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func e2eStaleObjectCount(c *qt.C, ctx context.Context, dbURL string) int {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var count int
	err = db.QueryRowContext(ctx, `
SELECT
	(SELECT COUNT(*) FROM pg_class WHERE relname IN (
		'stale_dev_view',
		'stale_dev_materialized_view',
		'stale_dev_sequence'
	)) +
	(SELECT COUNT(*) FROM pg_proc WHERE proname IN (
		'stale_dev_function',
		'stale_dev_procedure',
		'stale_dev_aggregate'
	)) +
	(SELECT COUNT(*) FROM pg_type WHERE typname IN (
		'stale_dev_domain',
		'stale_dev_composite',
		'z_stale_dev_range',
		'a_stale_dev_multirange'
	))`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func e2eIndexIsValid(c *qt.C, ctx context.Context, dbURL, indexName string) bool {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var valid, ready bool
	err = db.QueryRowContext(ctx, `
SELECT i.indisvalid, i.indisready
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
WHERE ic.relname = $1`, indexName).Scan(&valid, &ready)
	c.Assert(err, qt.IsNil)
	return valid && ready
}
