//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/internal/dbschema/postgres"
)

func TestWriterDropAllTables_LiveRejectsExternalPolicyDependency(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", requirePostgresWriterLiveURL(t))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	c.Assert(db.PingContext(ctx), qt.IsNil)

	suffix := time.Now().UnixNano()
	managedSchema := fmt.Sprintf("ptah_cleanup_managed_%d", suffix)
	externalSchema := fmt.Sprintf("ptah_cleanup_external_%d", suffix)
	managedIdent := pgx.Identifier{managedSchema}.Sanitize()
	externalIdent := pgx.Identifier{externalSchema}.Sanitize()

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE SCHEMA %[1]s;
		CREATE SCHEMA %[2]s;
		CREATE FUNCTION %[1]s.is_allowed()
			RETURNS boolean
			LANGUAGE sql
			IMMUTABLE
			AS 'SELECT true';
		CREATE TABLE %[1]s.stale_items (id bigint PRIMARY KEY);
		CREATE TABLE %[2]s.audit_items (id bigint PRIMARY KEY);
		ALTER TABLE %[2]s.audit_items ENABLE ROW LEVEL SECURITY;
		CREATE POLICY audit_items_policy
			ON %[2]s.audit_items
			USING (%[1]s.is_allowed());
	`, managedIdent, externalIdent))
	c.Assert(err, qt.IsNil)
	defer func() {
		_, cleanupErr := db.ExecContext(
			context.Background(),
			fmt.Sprintf("DROP SCHEMA %s CASCADE; DROP SCHEMA %s CASCADE", externalIdent, managedIdent),
		)
		c.Check(cleanupErr, qt.IsNil)
	}()

	writer := postgres.NewPostgreSQLWriter(db, managedSchema)
	err = writer.DropAllTables(ctx)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, fmt.Sprintf(`refusing to clean schema %q`, managedSchema))
	c.Assert(err.Error(), qt.Contains, "because other objects depend on it")
	c.Assert(postgresWriterLiveObjectCount(c, ctx, db, managedSchema, "stale_items"), qt.Equals, 1)
	c.Assert(postgresWriterLivePolicyCount(c, ctx, db, externalSchema, "audit_items_policy"), qt.Equals, 1)
}

func TestWriterDropAllTables_LiveResolvesInternalDependencies(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", requirePostgresWriterLiveURL(t))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	c.Assert(db.PingContext(ctx), qt.IsNil)

	schema := fmt.Sprintf("ptah_cleanup_internal_%d", time.Now().UnixNano())
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE SCHEMA %[1]s;
		CREATE TABLE %[1]s.parents (
			id bigint PRIMARY KEY,
			child_id bigint
		);
		CREATE TABLE %[1]s.children (
			id bigint PRIMARY KEY,
			parent_id bigint REFERENCES %[1]s.parents(id)
		);
		ALTER TABLE %[1]s.parents
			ADD CONSTRAINT parents_child_fkey
			FOREIGN KEY (child_id) REFERENCES %[1]s.children(id);
		CREATE VIEW %[1]s.a_base_view AS
			SELECT id FROM %[1]s.parents;
		CREATE VIEW %[1]s.z_dependent_view AS
			SELECT id FROM %[1]s.a_base_view;
	`, schemaIdent))
	c.Assert(err, qt.IsNil)
	defer func() {
		_, cleanupErr := db.ExecContext(
			context.Background(),
			fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaIdent),
		)
		c.Check(cleanupErr, qt.IsNil)
	}()

	writer := postgres.NewPostgreSQLWriter(db, schema)
	err = writer.DropAllTables(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveRelationCount(c, ctx, db, schema), qt.Equals, 0)
}

func requirePostgresWriterLiveURL(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"POSTGRES_TEST_DSN", "POSTGRES_URL", "TEST_DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	t.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, or TEST_DATABASE_URL is not set")
	return ""
}

func postgresWriterLiveObjectCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	schema string,
	name string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relname = $2
	`, schema, name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresWriterLivePolicyCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	schema string,
	name string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_policy p
		JOIN pg_class c ON c.oid = p.polrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND p.polname = $2
	`, schema, name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresWriterLiveRelationCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	schema string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
	`, schema).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}
