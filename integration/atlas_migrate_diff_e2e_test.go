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
)

// TestAtlasMigrateDiffConcurrentIndexAndQualifierE2E runs the real ptah-compat
// binary against a disposable PostgreSQL database and verifies the
// Atlas-compatible migrate diff behavior added for issue #815:
// concurrent-index migration-file metadata (`-- atlas:txmode none`), the
// transactional/concurrent safety split, atomic atlas.sum updates, and
// --qualifier artifact naming.
func TestAtlasMigrateDiffConcurrentIndexAndQualifierE2E(t *testing.T) {
	dbURL := requirePostgresE2EDatabaseURL(t)

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
	desiredDBName := testDBName + "_desired"
	createE2EDatabase(c, ctx, adminDB, desiredDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, desiredDBName)

	desiredDBURL := replaceDatabaseName(c, dbURL, desiredDBName)
	desiredDB, err := sql.Open("pgx", desiredDBURL)
	c.Assert(err, qt.IsNil)
	defer desiredDB.Close()
	_, err = desiredDB.ExecContext(ctx, "CREATE TABLE desired_database_items (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL)")
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
		var sourceTableCount int
		queryErr := desiredDB.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'desired_database_items'").
			Scan(&sourceTableCount)
		c.Assert(queryErr, qt.IsNil)
		c.Assert(sourceTableCount, qt.Equals, 1)
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
		c.Assert(e2eIndexIsValid(c, ctx, testDBURL, "idx_users_email"), qt.IsTrue)
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

		c.Assert(err, qt.Not(qt.IsNil))
		c.Assert(output, qt.Contains, `invalid --qualifier "bad.name"`)
		entries, readErr := os.ReadDir(migrationsDir)
		c.Assert(readErr, qt.IsNil)
		c.Assert(entries, qt.HasLen, 0)
	})
}

// e2eIndexIsValid reports whether the named index exists, is valid, and is
// ready on the target database (pg_index.indisvalid/indisready both hold for
// successfully completed concurrent builds).
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
