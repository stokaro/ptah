//go:build integration

package atlas_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

func TestSchemaCleanScopeRefusesUnselectedPostgresDependents(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	schemaName := fmt.Sprintf("ptah_clean_dependency_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	admin, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schemaName)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	scopedURL := parsed.String()
	conn, err := dbschema.ConnectToDatabase(t.Context(), scopedURL)
	c.Assert(err, qt.IsNil)
	for _, statement := range []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY)",
		"CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))",
		"CREATE VIEW active_users AS SELECT id FROM users",
	} {
		_, err = conn.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	dbschema.CloseAndWarn(conn)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	stdout, stderr, code := runAtlasBinary(
		compat,
		nil,
		"schema", "clean",
		"--url", scopedURL,
		"--include", "users[type=table]",
		"--auto-approve",
	)

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, `DROP TABLE IF EXISTS "users" RESTRICT`)
	c.Assert(stdout, qt.Not(qt.Contains), "CASCADE")
	c.Assert(stderr, qt.Contains, "would leave dependents behind")
	c.Assert(postgresScopedDependencyObjectCount(t, scopedURL), qt.Equals, 4)
}

func TestSchemaCleanScopeOrdersSelectedPostgresViewDependencies(t *testing.T) {
	c := qt.New(t)
	scopedURL := postgresCleanupDependencySchema(t, "view_order")
	postgresCleanupDependencyExec(t, scopedURL,
		"CREATE VIEW a_base AS SELECT 1 AS id",
		"CREATE VIEW z_child AS SELECT id FROM a_base",
	)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	stdout, stderr, code := runAtlasBinary(
		compat,
		nil,
		"schema", "clean",
		"--url", scopedURL,
		"--include", "*[type=view]",
		"--auto-approve",
	)

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, `DROP VIEW IF EXISTS "a_base" RESTRICT`)
	c.Assert(stdout, qt.Contains, `DROP VIEW IF EXISTS "z_child" RESTRICT`)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(postgresCleanupDependencyCount(t, scopedURL), qt.Equals, 0)
}

func TestSchemaCleanScopeRollsBackPostgresPlanWhenRestrictRefuses(t *testing.T) {
	c := qt.New(t)
	scopedURL := postgresCleanupDependencySchema(t, "atomic_restrict")
	postgresCleanupDependencyExec(t, scopedURL,
		"CREATE TABLE a_parent (id INTEGER PRIMARY KEY)",
		"CREATE TABLE b_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES a_parent(id))",
		"CREATE TABLE z_blocked (id INTEGER PRIMARY KEY)",
		"CREATE VIEW hidden_blocker AS SELECT id FROM z_blocked",
	)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	stdout, stderr, code := runAtlasBinary(
		compat,
		nil,
		"schema", "clean",
		"--url", scopedURL,
		"--include", "*[type=table]",
		"--auto-approve",
	)

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, `DROP CONSTRAINT "b_child_parent_id_fkey"`)
	c.Assert(stdout, qt.Contains, `DROP TABLE IF EXISTS "z_blocked" RESTRICT`)
	c.Assert(stderr, qt.Contains, "would leave dependents behind")
	c.Assert(postgresCleanupDependencyCount(t, scopedURL), qt.Equals, 5)
}

func postgresCleanupDependencySchema(t *testing.T, suffix string) string {
	t.Helper()
	c := qt.New(t)
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	schemaName := fmt.Sprintf(
		"ptah_clean_%s_%d_%d",
		suffix,
		os.Getpid(),
		time.Now().UnixNano()%1_000_000,
	)
	admin, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schemaName)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func postgresCleanupDependencyExec(t *testing.T, dbURL string, statements ...string) {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err = conn.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
}

func postgresCleanupDependencyCount(t *testing.T, dbURL string) int {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(t.Context(), `
		SELECT
			(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm')) +
			(SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
			 WHERE n.nspname = current_schema() AND c.contype = 'f')`,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresScopedDependencyObjectCount(t *testing.T, dbURL string) int {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(t.Context(), `
		SELECT
			(SELECT count(*) FROM information_schema.tables
			 WHERE table_schema = current_schema() AND table_name IN ('users', 'posts')) +
			(SELECT count(*) FROM information_schema.views
			 WHERE table_schema = current_schema() AND table_name = 'active_users') +
			(SELECT count(*) FROM information_schema.table_constraints
			 WHERE constraint_schema = current_schema() AND constraint_name = 'posts_user_id_fkey')`,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// TestSchemaCleanScopeRollsBackWhenAForeignKeyBlocksTheDrop keeps the savepoint
// and rollback machinery under test after stokaro/ptah#1704.
//
// The dependents pre-check reads pg_rewrite, which records the definitions of
// views and materialized views. A FOREIGN KEY is not recorded there, so a drop
// blocked by one still reaches the server, still answers 2BP01, and still rolls
// the whole transaction back -- the path the sibling rollback test exercised
// before its own case started being refused early.
//
// Without this, the pre-check would have quietly retired the coverage for the
// machinery it sits in front of.
func TestSchemaCleanScopeRollsBackWhenAForeignKeyBlocksTheDrop(t *testing.T) {
	c := qt.New(t)
	scopedURL := postgresCleanupDependencySchema(t, "fk_rollback")
	postgresCleanupDependencyExec(t, scopedURL,
		"CREATE TABLE a_parent (id INTEGER PRIMARY KEY)",
		"CREATE TABLE b_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES a_parent(id))",
	)

	// Captured before the run rather than written as a literal: what this
	// asserts is that NOTHING changed, and a hard-coded number states the
	// fixture's shape instead, which is a second thing to keep in step.
	before := postgresCleanupDependencyCount(t, scopedURL)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	stdout, stderr, code := runAtlasBinary(
		compat,
		nil,
		"schema", "clean",
		"--url", scopedURL,
		"--include", "a_parent[type=table]",
		"--auto-approve",
	)

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, `DROP TABLE IF EXISTS "a_parent" RESTRICT`)
	// The server's own sentence, not the pre-check's: this dependency is
	// invisible to pg_rewrite, which is the point of the case.
	c.Assert(stderr, qt.Contains, "other objects depend on it")
	c.Assert(stderr, qt.Not(qt.Contains), "would leave dependents behind")
	// Everything survives: the transaction rolled back.
	c.Assert(postgresCleanupDependencyCount(t, scopedURL), qt.Equals, before)
}
