//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// A dev database an atlas.hcl docker block provisions starts from the state
// the image and the block's baseline leave, and every reset returns it there
// (stokaro/ptah#4056). These tests record a Supabase-shaped starting point,
// change it the way a migration directory does, reset, and read the catalog
// back with queries of their own rather than with the snapshot the reset
// compares by.

// startingPointLiveSQL is the starting point. %[1]s is a role the test owns.
const startingPointLiveSQL = `
	CREATE SCHEMA auth;
	CREATE TABLE auth.users (id bigint PRIMARY KEY, email text);
	CREATE VIEW auth.active AS SELECT id FROM auth.users;
	CREATE TABLE auth.audit (id bigint, note text);
	CREATE FUNCTION auth.uid() RETURNS bigint LANGUAGE sql AS 'SELECT 1::bigint';
	CREATE FUNCTION auth.touch() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
	CREATE TRIGGER touch BEFORE UPDATE ON auth.users FOR EACH ROW EXECUTE FUNCTION auth.touch();
	ALTER TABLE auth.users ENABLE ROW LEVEL SECURITY;
	CREATE POLICY own ON auth.users USING (true);
	GRANT USAGE ON SCHEMA auth TO %[1]s;
	GRANT SELECT ON auth.users TO %[1]s;
	GRANT UPDATE (email) ON auth.users TO %[1]s;
	ALTER DEFAULT PRIVILEGES IN SCHEMA auth GRANT SELECT ON TABLES TO %[1]s;
`

// startingPointLiveRun is what a run adds to the starting point and changes in
// it: objects in a new schema, in public and in auth, on auth.users itself,
// the privileges the starting point set, and settings of its objects. The
// primary key on auth.audit makes its column NOT NULL, which PostgreSQL 18
// records as a constraint the server refuses to drop while the key holds it.
const startingPointLiveRun = `
	ALTER TABLE auth.audit ADD PRIMARY KEY (id);
	ALTER TABLE auth.users ALTER COLUMN email SET NOT NULL;
	ALTER TABLE auth.users ALTER COLUMN email SET DEFAULT 'nobody';
	ALTER TABLE auth.users DISABLE ROW LEVEL SECURITY;
	ALTER TABLE auth.users DISABLE TRIGGER touch;
	COMMENT ON TABLE auth.users IS 'run';
	COMMENT ON COLUMN auth.users.email IS 'run';
	CREATE OR REPLACE FUNCTION auth.uid() RETURNS bigint LANGUAGE sql AS 'SELECT 2::bigint';
	ALTER FUNCTION auth.uid() OWNER TO %[1]s;
	CREATE OR REPLACE VIEW auth.active AS SELECT id FROM auth.users WHERE id > 0;
	CREATE SCHEMA app;
	CREATE TABLE app.notes (id integer);
	CREATE TABLE auth.sessions (id integer);
	CREATE TABLE public.profiles (id bigint PRIMARY KEY REFERENCES auth.users (id));
	CREATE FUNCTION public.on_signup() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
	CREATE TRIGGER on_signup AFTER INSERT ON auth.users FOR EACH ROW EXECUTE FUNCTION public.on_signup();
	CREATE POLICY run_policy ON auth.users USING (false);
	ALTER TABLE auth.users ADD COLUMN nickname text;
	ALTER TABLE auth.users ADD CONSTRAINT email_set CHECK (email <> '');
	CREATE INDEX users_email ON auth.users (email);
	REVOKE SELECT ON auth.users FROM %[1]s;
	GRANT INSERT ON auth.users TO %[1]s;
	REVOKE UPDATE (email) ON auth.users FROM %[1]s;
	ALTER DEFAULT PRIVILEGES IN SCHEMA auth REVOKE SELECT ON TABLES FROM %[1]s;
	ALTER DEFAULT PRIVILEGES IN SCHEMA auth GRANT DELETE ON TABLES TO %[1]s;
`

// startingPointLiveCatalog is what the tests read back: the shape of the
// starting point and the exact access lists on it.
type startingPointLiveCatalog struct {
	Schemas       string
	AuthRelations string
	PublicObjects string
	UserColumns   string
	UserTriggers  string
	UserPolicies  string
	UserChecks    string
	UserACL       string
	EmailACL      string
	SchemaACL     string
	DefaultACL    string
	UserSettings  string
	AuditSettings string
	UID           string
	ActiveView    string
}

const startingPointLiveCatalogQuery = `
	SELECT
		(SELECT string_agg(nspname, ',' ORDER BY nspname) FROM pg_namespace
			WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema'),
		(SELECT string_agg(relname, ',' ORDER BY relname) FROM pg_class WHERE relnamespace = 'auth'::regnamespace),
		(SELECT COALESCE(string_agg(name, ',' ORDER BY name), '') FROM (
			SELECT relname AS name FROM pg_class WHERE relnamespace = 'public'::regnamespace
			UNION ALL SELECT proname FROM pg_proc WHERE pronamespace = 'public'::regnamespace) public_objects),
		(SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute
			WHERE attrelid = 'auth.users'::regclass AND attnum > 0 AND NOT attisdropped),
		(SELECT string_agg(tgname || ':' || tgenabled::text, ',' ORDER BY tgname) FROM pg_trigger
			WHERE tgrelid = 'auth.users'::regclass AND NOT tgisinternal),
		(SELECT string_agg(polname, ',' ORDER BY polname) FROM pg_policy WHERE polrelid = 'auth.users'::regclass),
		(SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint WHERE conrelid = 'auth.users'::regclass),
		(SELECT relacl::text FROM pg_class WHERE oid = 'auth.users'::regclass),
		(SELECT COALESCE(attacl::text, '') FROM pg_attribute WHERE attrelid = 'auth.users'::regclass AND attname = 'email'),
		(SELECT nspacl::text FROM pg_namespace WHERE nspname = 'auth'),
		(SELECT defaclacl::text FROM pg_default_acl WHERE defaclnamespace = 'auth'::regnamespace),
		(SELECT concat_ws(' ', c.relrowsecurity, obj_description(c.oid, 'pg_class'), a.attnotnull,
			pg_get_expr(d.adbin, d.adrelid), col_description(c.oid, a.attnum))
			FROM pg_class c
			JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'email'
			LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
			WHERE c.oid = 'auth.users'::regclass),
		(SELECT concat_ws(' ', a.attnotnull, (SELECT string_agg(conname, ',') FROM pg_constraint
				WHERE conrelid = 'auth.audit'::regclass AND contype <> 'n'))
			FROM pg_attribute a WHERE a.attrelid = 'auth.audit'::regclass AND a.attname = 'id'),
		(SELECT prosrc || ' ' || pg_get_userbyid(proowner) FROM pg_proc WHERE oid = 'auth.uid()'::regprocedure),
		pg_get_viewdef('auth.active'::regclass)
`

func readStartingPointLiveCatalog(c *qt.C, ctx context.Context, db *sql.DB) startingPointLiveCatalog {
	c.Helper()
	var got startingPointLiveCatalog
	err := db.QueryRowContext(ctx, startingPointLiveCatalogQuery).Scan(
		&got.Schemas, &got.AuthRelations, &got.PublicObjects, &got.UserColumns, &got.UserTriggers,
		&got.UserPolicies, &got.UserChecks, &got.UserACL, &got.EmailACL, &got.SchemaACL, &got.DefaultACL,
		&got.UserSettings, &got.AuditSettings, &got.UID, &got.ActiveView,
	)
	c.Assert(err, qt.IsNil)
	return got
}

// startingPointLiveFixture is a database at its starting point, with the
// record a claim takes of it.
type startingPointLiveFixture struct {
	db     *sql.DB
	role   string
	writer *postgres.PostgreSQLWriter
	kept   dbreset.Kept
	start  startingPointLiveCatalog
}

func newStartingPointLiveFixture(c *qt.C, ctx context.Context) startingPointLiveFixture {
	c.Helper()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	db := liveDatabase.db
	role := pgx.Identifier{fmt.Sprintf("ptah_start_%d", time.Now().UnixNano())}.Sanitize()
	_, err := db.ExecContext(ctx, "CREATE ROLE "+role)
	c.Assert(err, qt.IsNil)
	// Registered after the database's cleanup is, so it runs first: a role
	// that holds privileges in a database cannot be dropped.
	c.Cleanup(func() {
		liveDatabase.cleanup()
		admin, openErr := sql.Open("pgx", requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
		c.Assert(openErr, qt.IsNil)
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	_, err = db.ExecContext(ctx, fmt.Sprintf(startingPointLiveSQL, role))
	c.Assert(err, qt.IsNil)
	writer := postgres.NewPostgreSQLWriter(db, "public")
	start, err := writer.CaptureStartingPoint(ctx)
	c.Assert(err, qt.IsNil)
	return startingPointLiveFixture{
		db:     db,
		role:   role,
		writer: writer,
		kept:   dbreset.Kept{StartingPoint: start},
		start:  readStartingPointLiveCatalog(c, ctx, db),
	}
}

// TestWriterReturnsADockerBlockDevDatabaseToItsStartingPoint_LivePostgres
// pins the reset both cleanups run for a dev database with a starting point:
// a realm reset, which a migration replay runs, and a schema reset. Each
// removes what the run added in every schema, the trigger, policy, column,
// constraint and index on auth.users included, restores each setting the run
// changed in place, and puts back each privilege the run changed, so the
// catalog reads as it did before the run.
func TestWriterReturnsADockerBlockDevDatabaseToItsStartingPoint_LivePostgres(t *testing.T) {
	for _, entry := range []struct {
		name  string
		reset func(*postgres.PostgreSQLWriter, context.Context, dbreset.Kept) error
	}{
		{name: "realm", reset: (*postgres.PostgreSQLWriter).DropDatabaseRealmKeeping},
		{name: "schema", reset: (*postgres.PostgreSQLWriter).DropAllTablesKeeping},
	} {
		t.Run(entry.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			fixture := newStartingPointLiveFixture(c, ctx)
			_, err := fixture.db.ExecContext(ctx, fmt.Sprintf(startingPointLiveRun, fixture.role))
			c.Assert(err, qt.IsNil)
			c.Assert(readStartingPointLiveCatalog(c, ctx, fixture.db), qt.Not(qt.DeepEquals), fixture.start)

			err = entry.reset(fixture.writer, ctx, fixture.kept)

			c.Assert(err, qt.IsNil)
			c.Assert(readStartingPointLiveCatalog(c, ctx, fixture.db), qt.DeepEquals, fixture.start)
			c.Assert(fixture.start.UserTriggers, qt.Equals, "touch:O")
			c.Assert(fixture.start.UserColumns, qt.Equals, "id,email")
			c.Assert(fixture.start.UserSettings, qt.Equals, "t f")
			c.Assert(fixture.start.AuditSettings, qt.Equals, "f")
		})
	}
}

// TestWriterRefusesAStartingPointItCannotReturnTo_LivePostgres pins that a reset
// refuses, and leaves the database as the run left it, when no statement can
// return part of the starting point: an object dropped by the run, or by the
// CASCADE that removes what the run made it depend on, and a setting the
// reset cannot change back. Committed, the next use would start from a
// different state without saying so.
func TestWriterRefusesAStartingPointItCannotReturnTo_LivePostgres(t *testing.T) {
	for _, entry := range []struct {
		name    string
		run     string
		wantErr string
	}{
		{
			name:    "dropped by the run",
			run:     `CREATE SCHEMA app; DROP FUNCTION auth.uid();`,
			wantErr: `(?s).*the run dropped part of the dev database's starting point, which the reset cannot create again: function auth\.uid\(\)`,
		},
		{
			name:    "a column type changed",
			run:     `CREATE SCHEMA app; ALTER TABLE auth.users ALTER COLUMN email TYPE varchar(64);`,
			wantErr: `(?s).*the run changed part of the dev database's starting point, which the reset cannot change back: column auth\.users\.email type`,
		},
		{
			name: "dropped by the reset's cascade",
			run: `CREATE SCHEMA app;
				CREATE FUNCTION public.one() RETURNS integer LANGUAGE sql AS 'SELECT 1';
				CREATE OR REPLACE VIEW auth.active AS SELECT id, public.one() AS one FROM auth.users;`,
			wantErr: `(?s).*the run dropped part of the dev database's starting point, which the reset cannot create again: view auth\.active`,
		},
	} {
		t.Run(entry.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			fixture := newStartingPointLiveFixture(c, ctx)
			_, err := fixture.db.ExecContext(ctx, entry.run)
			c.Assert(err, qt.IsNil)

			err = fixture.writer.DropDatabaseRealmKeeping(ctx, fixture.kept)

			c.Assert(err, qt.ErrorMatches, entry.wantErr)
			c.Assert(postgresWriterLiveSchemasLike(c, ctx, fixture.db, "app"), qt.DeepEquals, []string{"app"})
		})
	}
}
