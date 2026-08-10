//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbschema/postgres"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

type postgresWriterFamilyLiveCase struct {
	name   string
	urlEnv string
}

type postgresWriterLiveDatabase struct {
	db      *sql.DB
	name    string
	cleanup func()
}

type postgresWriterLiveSchemaMetadata struct {
	Owner      string
	Comment    sql.NullString
	Privileges []string
}

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

func TestWriterDropAllTables_LivePostgresFamilyResolvesReverseViewChain(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	tests := []postgresWriterFamilyLiveCase{
		{name: "cockroachdb", urlEnv: "COCKROACHDB_URL"},
		{name: "yugabytedb", urlEnv: "YUGABYTEDB_URL"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db, err := sql.Open("pgx", requirePostgresWriterFamilyLiveURL(c, test.urlEnv))
			c.Assert(err, qt.IsNil)
			defer db.Close()
			c.Assert(db.PingContext(ctx), qt.IsNil)

			schema := fmt.Sprintf("ptah_cleanup_chain_%d", time.Now().UnixNano())
			schemaIdent := pgx.Identifier{schema}.Sanitize()
			_, err = db.ExecContext(ctx, fmt.Sprintf(`
				CREATE SCHEMA %[1]s;
				CREATE TABLE %[1]s.stale_parent (id bigint PRIMARY KEY);
				CREATE VIEW %[1]s.a_base_view AS
					SELECT id FROM %[1]s.stale_parent;
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
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, schema), qt.Equals, 3)

			writer := postgres.NewPostgreSQLWriter(db, schema)
			err = writer.DropAllTables(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, schema), qt.Equals, 0)
		})
	}
}

func TestWriterDropDatabaseRealm_LivePostgresFamilyCleansCrossSchemaGraph(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	tests := []postgresWriterFamilyLiveCase{
		{name: "postgres", urlEnv: "POSTGRES_URL"},
		{name: "cockroachdb", urlEnv: "COCKROACHDB_URL"},
		{name: "yugabytedb", urlEnv: "YUGABYTEDB_URL"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			liveDatabase := newPostgresWriterLiveDatabase(
				c,
				ctx,
				requirePostgresWriterFamilyLiveURL(c, test.urlEnv),
			)
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			rootMetadata := loadPostgresWriterLiveSchemaMetadata(c, ctx, db, "public")
			systemExtensions := postgresWriterLiveExtensionNames(c, ctx, db)

			suffix := time.Now().UnixNano()
			parentSchema := fmt.Sprintf("ptah_realm_parent_%d", suffix)
			childSchema := fmt.Sprintf("ptah_realm_child_%d", suffix)
			ybPrefixedSchema := fmt.Sprintf("yb_ptah_realm_%d", suffix)
			parentIdent := pgx.Identifier{parentSchema}.Sanitize()
			childIdent := pgx.Identifier{childSchema}.Sanitize()
			ybPrefixedIdent := pgx.Identifier{ybPrefixedSchema}.Sanitize()
			_, err := db.ExecContext(ctx, fmt.Sprintf(`
				CREATE SCHEMA %[1]s;
				CREATE SCHEMA %[2]s;
				CREATE SCHEMA %[3]s;
				CREATE TYPE public.ptah_root_status AS ENUM ('ready');
				CREATE SEQUENCE public.ptah_root_sequence;
				CREATE FUNCTION public.ptah_root_answer()
					RETURNS integer
					LANGUAGE sql
					AS 'SELECT 42';
				CREATE TABLE public.ptah_root_items (id bigint PRIMARY KEY);
				CREATE TABLE %[1]s.parents (id bigint PRIMARY KEY);
				CREATE TABLE %[2]s.children (
					id bigint PRIMARY KEY,
					parent_id bigint REFERENCES %[1]s.parents(id)
				);
				CREATE VIEW %[2]s.parent_ids AS
					SELECT id FROM %[1]s.parents;
				CREATE TABLE %[3]s.prefixed_items (id bigint PRIMARY KEY);
			`, parentIdent, childIdent, ybPrefixedIdent))
			c.Assert(err, qt.IsNil)

			writer := postgres.NewPostgreSQLWriter(db, "public")
			err = writer.DropDatabaseRealm(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(writer.DropDatabaseRealm(ctx), qt.IsNil)
			c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, parentSchema), qt.Equals, 0)
			c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, childSchema), qt.Equals, 0)
			c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, ybPrefixedSchema), qt.Equals, 0)
			c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "public"), qt.Equals, 1)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
			c.Assert(
				postgresWriterLiveNamedTypeCount(c, ctx, db, "public", "ptah_root_status"),
				qt.Equals,
				0,
			)
			c.Assert(
				postgresWriterLiveRoutineCount(c, ctx, db, "public", "ptah_root_answer"),
				qt.Equals,
				0,
			)
			c.Assert(
				loadPostgresWriterLiveSchemaMetadata(c, ctx, db, "public"),
				qt.DeepEquals,
				rootMetadata,
			)
			c.Assert(postgresWriterLiveExtensionNames(c, ctx, db), qt.DeepEquals, systemExtensions)
			c.Assert(postgresWriterLiveCurrentDatabase(c, ctx, db), qt.Equals, liveDatabase.name)
		})
	}
}

func TestWriterDropDatabaseRealm_LivePostgresCleansToastTable(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	liveDatabase := newPostgresWriterLiveDatabase(
		c,
		ctx,
		requirePostgresWriterFamilyLiveURL(c, "POSTGRES_URL"),
	)
	defer liveDatabase.cleanup()
	db := liveDatabase.db

	_, err := db.ExecContext(ctx, `
		CREATE TABLE public.ptah_toast_items (
			id bigint PRIMARY KEY,
			payload text NOT NULL
		);
		INSERT INTO public.ptah_toast_items (id, payload)
		SELECT 1, string_agg(md5(value::text), '')
		FROM generate_series(1, 20000) AS value;
	`)
	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveToastOID(c, ctx, db, "public", "ptah_toast_items"), qt.Not(qt.Equals), 0)
	c.Assert(postgresWriterLiveStoredColumnSize(c, ctx, db), qt.Not(qt.Equals), 0)

	writer := postgres.NewPostgreSQLWriter(db, "public")
	err = writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
	c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "public"), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_LivePostgresCleansLargeObjects(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	liveDatabase := newPostgresWriterLiveDatabase(
		c,
		ctx,
		requirePostgresWriterFamilyLiveURL(c, "POSTGRES_URL"),
	)
	defer liveDatabase.cleanup()
	db := liveDatabase.db

	var oid uint32
	err := db.QueryRowContext(ctx, "SELECT lo_create(0)").Scan(&oid)
	c.Assert(err, qt.IsNil)
	c.Assert(oid, qt.Not(qt.Equals), uint32(0))

	writer := postgres.NewPostgreSQLWriter(db, "public")
	err = writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.IsNil)
	var objectCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_largeobject_metadata").Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_LivePostgresRejectsPublication(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	liveDatabase := newPostgresWriterLiveDatabase(
		c,
		ctx,
		requirePostgresWriterFamilyLiveURL(c, "POSTGRES_URL"),
	)
	defer liveDatabase.cleanup()
	db := liveDatabase.db

	_, err := db.ExecContext(ctx, `
		CREATE TABLE public.ptah_published_events (id bigint PRIMARY KEY);
		CREATE PUBLICATION ptah_events_publication
			FOR TABLE public.ptah_published_events;
	`)
	c.Assert(err, qt.IsNil)

	writer := postgres.NewPostgreSQLWriter(db, "public")
	err = writer.DropDatabaseRealm(ctx)

	c.Assert(
		err,
		qt.ErrorMatches,
		`refusing to clean PostgreSQL database realm with unsupported database-scoped `+
			`publication "ptah_events_publication"`,
	)
	c.Assert(
		postgresWriterLiveObjectCount(c, ctx, db, "public", "ptah_published_events"),
		qt.Equals,
		1,
	)
	var publicationCount int
	err = db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM pg_publication WHERE pubname = 'ptah_events_publication'",
	).Scan(&publicationCount)
	c.Assert(err, qt.IsNil)
	c.Assert(publicationCount, qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_LivePostgresRollsBackOnTemporaryPolicyDependency(
	t *testing.T,
) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	liveDatabase := newPostgresWriterLiveDatabase(
		c,
		ctx,
		requirePostgresWriterFamilyLiveURL(c, "POSTGRES_URL"),
	)
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	conn, err := db.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, `
		CREATE FUNCTION public.ptah_policy_guard()
			RETURNS boolean
			LANGUAGE sql
			IMMUTABLE
			AS 'SELECT true';
		CREATE TABLE public.ptah_realm_items (id bigint PRIMARY KEY);
		CREATE TEMP TABLE ptah_preserved_items (id bigint PRIMARY KEY);
		ALTER TABLE pg_temp.ptah_preserved_items ENABLE ROW LEVEL SECURITY;
		CREATE POLICY ptah_preserved_policy
			ON pg_temp.ptah_preserved_items
			USING (public.ptah_policy_guard());
	`)
	c.Assert(err, qt.IsNil)

	writer := postgres.NewPostgreSQLWriterForRunner(sqlrunner.NewConn(ctx, conn), "public")
	err = writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "because other objects depend on it")
	c.Assert(postgresWriterLiveObjectCount(c, ctx, db, "public", "ptah_realm_items"), qt.Equals, 1)
	c.Assert(
		postgresWriterLiveRoutineCount(c, ctx, db, "public", "ptah_policy_guard"),
		qt.Equals,
		1,
	)
	c.Assert(postgresWriterLiveTemporaryPolicyCount(c, ctx, conn), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_LiveRejectsProtectedDatabase(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name     string
		urlEnv   string
		database string
	}{
		{
			name:     "postgres",
			urlEnv:   "POSTGRES_URL",
			database: "postgres",
		},
		{
			name:     "cockroachdb",
			urlEnv:   "COCKROACHDB_URL",
			database: "defaultdb",
		},
		{
			name:     "yugabytedb",
			urlEnv:   "YUGABYTEDB_URL",
			database: "yugabyte",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db := openPostgresWriterProtectedLiveDatabase(
				c,
				requirePostgresWriterFamilyLiveURL(c, test.urlEnv),
				test.database,
			)
			writer := postgres.NewPostgreSQLWriter(db, "public")

			err := writer.DropDatabaseRealm(c.Context())

			c.Assert(
				err,
				qt.ErrorMatches,
				`refusing to clean protected PostgreSQL-family database "`+test.database+`"`,
			)
		})
	}
}

func TestWriterDropDatabaseRealm_LivePostgresRestoresRootMetadata(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	liveDatabase := newPostgresWriterLiveDatabase(
		c,
		ctx,
		requirePostgresWriterFamilyLiveURL(c, "POSTGRES_URL"),
	)
	defer liveDatabase.cleanup()
	db := liveDatabase.db

	_, err := db.ExecContext(ctx, `
		COMMENT ON SCHEMA public IS 'Ptah cleanup root metadata';
		REVOKE ALL PRIVILEGES ON SCHEMA public FROM PUBLIC;
		GRANT USAGE ON SCHEMA public TO PUBLIC;
		CREATE COLLATION public.ptah_case_sensitive FROM "C";
		CREATE EXTENSION hstore WITH SCHEMA public;
		ALTER DEFAULT PRIVILEGES IN SCHEMA public
			GRANT SELECT ON TABLES TO PUBLIC;
		CREATE TABLE public.stale_items (id bigint PRIMARY KEY);
	`)
	c.Assert(err, qt.IsNil)
	beforeOID := postgresWriterLiveSchemaOID(c, ctx, db, "public")
	beforeMetadata := loadPostgresWriterLiveSchemaMetadata(c, ctx, db, "public")

	writer := postgres.NewPostgreSQLWriter(db, "public")
	err = writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveSchemaOID(c, ctx, db, "public"), qt.Not(qt.Equals), beforeOID)
	c.Assert(loadPostgresWriterLiveSchemaMetadata(c, ctx, db, "public"), qt.DeepEquals, beforeMetadata)
	c.Assert(postgresWriterLiveCollationCount(c, ctx, db, "public"), qt.Equals, 0)
	c.Assert(postgresWriterLiveExtensionCount(c, ctx, db, "hstore"), qt.Equals, 0)
	c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
	c.Assert(postgresWriterLiveCurrentDatabase(c, ctx, db), qt.Equals, liveDatabase.name)
}

func TestWriterDropDatabaseRealm_LivePostgresCreatesAbsentRoot(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	liveDatabase := newPostgresWriterLiveDatabase(
		c,
		ctx,
		requirePostgresWriterFamilyLiveURL(c, "POSTGRES_URL"),
	)
	defer liveDatabase.cleanup()

	writer := postgres.NewPostgreSQLWriter(liveDatabase.db, "shadow")
	err := writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(writer.DropDatabaseRealm(ctx), qt.IsNil)
	c.Assert(postgresWriterLiveSchemaCount(c, ctx, liveDatabase.db, "public"), qt.Equals, 1)
	c.Assert(postgresWriterLiveSchemaCount(c, ctx, liveDatabase.db, "shadow"), qt.Equals, 1)
	c.Assert(
		postgresWriterLiveCurrentDatabase(c, ctx, liveDatabase.db),
		qt.Equals,
		liveDatabase.name,
	)
}

func TestWriterDropAllTables_LiveRejectsCrossSchemaPartitionEdges(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", requirePostgresWriterLiveURL(t))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	c.Assert(db.PingContext(ctx), qt.IsNil)

	tests := []struct {
		name                string
		setup               string
		managedRelationName string
		externalRelation    string
	}{
		{
			name: "external child",
			setup: `
				CREATE TABLE %[1]s.events (
					id bigint,
					occurred_at date
				) PARTITION BY RANGE (occurred_at);
				CREATE TABLE %[2]s.events_2025
					PARTITION OF %[1]s.events
					FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
			`,
			managedRelationName: "events",
			externalRelation:    "events_2025",
		},
		{
			name: "external parent",
			setup: `
				CREATE TABLE %[2]s.events (
					id bigint,
					occurred_at date
				) PARTITION BY RANGE (occurred_at);
				CREATE TABLE %[1]s.events_2025
					PARTITION OF %[2]s.events
					FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
			`,
			managedRelationName: "events_2025",
			externalRelation:    "events",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			suffix := time.Now().UnixNano()
			managedSchema := fmt.Sprintf("ptah_partition_managed_%d", suffix)
			externalSchema := fmt.Sprintf("ptah_partition_external_%d", suffix)
			managedIdent := pgx.Identifier{managedSchema}.Sanitize()
			externalIdent := pgx.Identifier{externalSchema}.Sanitize()

			_, err := db.ExecContext(ctx, fmt.Sprintf(`
				CREATE SCHEMA %[1]s;
				CREATE SCHEMA %[2]s;
				%[3]s
			`, managedIdent, externalIdent, fmt.Sprintf(test.setup, managedIdent, externalIdent)))
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
			c.Assert(err.Error(), qt.Contains, "across the schema boundary")
			c.Assert(
				postgresWriterLiveObjectCount(c, ctx, db, managedSchema, test.managedRelationName),
				qt.Equals,
				1,
			)
			c.Assert(
				postgresWriterLiveObjectCount(c, ctx, db, externalSchema, test.externalRelation),
				qt.Equals,
				1,
			)
		})
	}
}

func newPostgresWriterLiveDatabase(
	c *qt.C,
	ctx context.Context,
	rawURL string,
) postgresWriterLiveDatabase {
	c.Helper()
	admin, err := sql.Open("pgx", rawURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_writer_%d", time.Now().UnixNano())
	nameIdent := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+nameIdent)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	db, err := sql.Open("pgx", parsed.String())
	c.Assert(err, qt.IsNil)
	c.Assert(db.PingContext(ctx), qt.IsNil)

	return postgresWriterLiveDatabase{
		db:   db,
		name: name,
		cleanup: func() {
			c.Check(db.Close(), qt.IsNil)
			_, dropErr := admin.ExecContext(
				context.Background(),
				"DROP DATABASE IF EXISTS "+nameIdent,
			)
			c.Check(dropErr, qt.IsNil)
			c.Check(admin.Close(), qt.IsNil)
		},
	}
}

func requirePostgresWriterFamilyLiveURL(c *qt.C, name string) string {
	c.Helper()
	rawURL := os.Getenv(name)
	if rawURL == "" {
		c.Skipf("%s is not set", name)
	}
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Scheme = "postgres"
	return parsed.String()
}

func openPostgresWriterProtectedLiveDatabase(c *qt.C, rawURL, database string) *sql.DB {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.RawPath = ""
	db, err := sql.Open("pgx", parsed.String())
	c.Assert(err, qt.IsNil)
	c.Assert(db.PingContext(c.Context()), qt.IsNil)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})
	return db
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

func postgresWriterLiveCurrentDatabase(c *qt.C, ctx context.Context, db *sql.DB) string {
	c.Helper()
	var name string
	err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&name)
	c.Assert(err, qt.IsNil)
	return name
}

func postgresWriterLiveSchemaOID(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	schema string,
) int64 {
	c.Helper()
	var oid int64
	err := db.QueryRowContext(
		ctx,
		"SELECT oid FROM pg_namespace WHERE nspname = $1",
		schema,
	).Scan(&oid)
	c.Assert(err, qt.IsNil)
	return oid
}

func loadPostgresWriterLiveSchemaMetadata(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	schema string,
) postgresWriterLiveSchemaMetadata {
	c.Helper()
	var metadata postgresWriterLiveSchemaMetadata
	err := db.QueryRowContext(ctx, `
		SELECT
			pg_get_userbyid(n.nspowner),
			obj_description(n.oid, 'pg_namespace')
		FROM pg_namespace n
		WHERE n.nspname = $1
	`, schema).Scan(&metadata.Owner, &metadata.Comment)
	c.Assert(err, qt.IsNil)

	rows, err := db.QueryContext(ctx, `
		SELECT
			CASE acl.grantee
				WHEN 0 THEN 'PUBLIC'
				ELSE pg_get_userbyid(acl.grantee)
			END,
			acl.privilege_type,
			acl.is_grantable
		FROM pg_namespace n
		CROSS JOIN LATERAL aclexplode(
			COALESCE(n.nspacl, acldefault('n', n.nspowner))
		) acl
		WHERE n.nspname = $1
		ORDER BY 1, 2, 3
	`, schema)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	for rows.Next() {
		var grantee string
		var privilege string
		var grantOption bool
		c.Assert(rows.Scan(&grantee, &privilege, &grantOption), qt.IsNil)
		metadata.Privileges = append(
			metadata.Privileges,
			fmt.Sprintf("%s:%s:%t", grantee, privilege, grantOption),
		)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return metadata
}

func postgresWriterLiveCollationCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	schema string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_collation c
		JOIN pg_namespace n ON n.oid = c.collnamespace
		WHERE n.nspname = $1
	`, schema).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresWriterLiveExtensionCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	name string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM pg_extension WHERE extname = $1",
		name,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresWriterLiveExtensionNames(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, "SELECT extname FROM pg_extension ORDER BY extname")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
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

func postgresWriterLiveNamedTypeCount(
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
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = $1
		  AND t.typname = $2
	`, schema, name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresWriterLiveRoutineCount(
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
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = $1
		  AND p.proname = $2
	`, schema, name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresWriterLiveToastOID(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	schema string,
	table string,
) int64 {
	c.Helper()
	var oid int64
	err := db.QueryRowContext(ctx, `
		SELECT c.reltoastrelid
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relname = $2
	`, schema, table).Scan(&oid)
	c.Assert(err, qt.IsNil)
	return oid
}

func postgresWriterLiveStoredColumnSize(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var size int
	err := db.QueryRowContext(
		ctx,
		"SELECT pg_column_size(payload) FROM public.ptah_toast_items WHERE id = 1",
	).Scan(&size)
	c.Assert(err, qt.IsNil)
	return size
}

func postgresWriterLiveTemporaryPolicyCount(
	c *qt.C,
	ctx context.Context,
	conn *sql.Conn,
) int {
	c.Helper()
	var count int
	err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_policy p
		JOIN pg_class c ON c.oid = p.polrelid
		WHERE c.relnamespace = pg_my_temp_schema()
		  AND p.polname = 'ptah_preserved_policy'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresWriterLiveSchemaCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	schema string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM pg_namespace WHERE nspname = $1",
		schema,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}
