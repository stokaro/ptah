//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// A realm cleanup drops every user extension, and DROP EXTENSION takes the
// extension's members with it: its schemas, and the tables and constraints in
// them. Those members are therefore not the cleanup's to drop one by one. When
// they were, a dev database holding timescaledb failed every replay, because
// the constraint drops queued for the extension's catalog ran after the
// extension drop had removed their schema (stokaro/ptah#3540).
//
// The first test builds those shapes on any PostgreSQL with ALTER EXTENSION
// ... ADD, which records the same pg_depend row CREATE EXTENSION records for
// the objects it creates. The last runs the extension that found it.

// postgresWriterLiveSchemasLike returns the schemas whose name matches pattern.
func postgresWriterLiveSchemasLike(c *qt.C, ctx context.Context, db *sql.DB, pattern string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, "SELECT nspname FROM pg_namespace WHERE nspname LIKE $1 ORDER BY nspname", pattern)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

func TestWriterDropDatabaseRealm_LivePostgresLeavesExtensionMembersToTheExtension(t *testing.T) {
	tests := []struct {
		name  string
		setup string
	}{
		{
			// TimescaleDB's shape: the extension's catalog in schemas of its own.
			name: "a schema the extension owns",
			setup: `
				CREATE EXTENSION hstore;
				CREATE SCHEMA owned_by_extension;
				CREATE TABLE owned_by_extension.parent (id integer PRIMARY KEY);
				CREATE TABLE owned_by_extension.child (parent_id integer REFERENCES owned_by_extension.parent (id));
				ALTER EXTENSION hstore ADD SCHEMA owned_by_extension;
				ALTER EXTENSION hstore ADD TABLE owned_by_extension.parent;
				ALTER EXTENSION hstore ADD TABLE owned_by_extension.child;
				CREATE TABLE public.user_table (id integer PRIMARY KEY);`,
		},
		{
			// A default grant in an extension's schema goes with the schema, and
			// the statement that would revoke it names the schema. Queued, it
			// runs after the extension drop and fails on the missing schema,
			// which IF EXISTS cannot cover for ALTER DEFAULT PRIVILEGES.
			name: "a default grant in a schema the extension owns",
			setup: `
				CREATE EXTENSION hstore;
				CREATE SCHEMA owned_by_extension;
				ALTER EXTENSION hstore ADD SCHEMA owned_by_extension;
				ALTER DEFAULT PRIVILEGES IN SCHEMA owned_by_extension GRANT SELECT ON TABLES TO PUBLIC;
				CREATE TABLE public.user_table (id integer PRIMARY KEY);`,
		},
		{
			// The same member tables in public. A table drop names the table
			// with IF EXISTS and survives the extension drop; a constraint
			// drop is an ALTER TABLE on a table that is gone.
			name: "member tables in public joined by a foreign key",
			setup: `
				CREATE EXTENSION hstore;
				CREATE TABLE public.member_parent (id integer PRIMARY KEY);
				CREATE TABLE public.member_child (parent_id integer REFERENCES public.member_parent (id));
				ALTER EXTENSION hstore ADD TABLE public.member_parent;
				ALTER EXTENSION hstore ADD TABLE public.member_child;
				CREATE TABLE public.user_table (id integer PRIMARY KEY);`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			_, err := db.ExecContext(ctx, test.setup)
			c.Assert(err, qt.IsNil)

			err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealm(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(postgresWriterLiveExtensionNames(c, ctx, db), qt.DeepEquals, []string{"plpgsql"})
			c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "owned_by_extension"), qt.Equals, 0)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
			c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "public"), qt.Equals, 1)
		})
	}
}

// TestWriterDropDatabaseRealm_LivePostgresCleansAUserSchemaBesideAnExtension is
// the control for the test above: leaving an extension's schema to the
// extension must not leave a schema the user made.
func TestWriterDropDatabaseRealm_LivePostgresCleansAUserSchemaBesideAnExtension(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE EXTENSION hstore;
		CREATE SCHEMA user_schema;
		CREATE TABLE user_schema.parent (id integer PRIMARY KEY);
		CREATE TABLE user_schema.child (parent_id integer REFERENCES user_schema.parent (id), attrs hstore);
	`)
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealm(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveExtensionNames(c, ctx, db), qt.DeepEquals, []string{"plpgsql"})
	c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "user_schema"), qt.Equals, 0)
}

// TestWriterDropDatabaseRealm_LiveTimescaleDB runs the cleanup against the
// extension that found stokaro/ptah#3540. The extension alone is the dev
// database a replay starts from, and the failing shape: its drop succeeds
// first and removes the schemas the queued catalog drops then name. With a
// hypertable holding a row the extension drop waits on the user's objects, and
// a chunk stands in an extension schema without being an extension member.
func TestWriterDropDatabaseRealm_LiveTimescaleDB(t *testing.T) {
	tests := []struct {
		name  string
		setup string
	}{
		{
			name:  "the extension alone",
			setup: "CREATE EXTENSION IF NOT EXISTS timescaledb",
		},
		{
			name: "a hypertable holding a row",
			setup: `
				CREATE EXTENSION IF NOT EXISTS timescaledb;
				CREATE TABLE public.metrics (at timestamptz NOT NULL, value integer);
				SELECT create_hypertable('public.metrics', 'at');
				INSERT INTO public.metrics VALUES (now(), 1);`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.TimescaleDB))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			_, err := db.ExecContext(ctx, test.setup)
			c.Assert(err, qt.IsNil)

			err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealm(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(postgresWriterLiveExtensionNames(c, ctx, db), qt.DeepEquals, []string{"plpgsql"})
			c.Assert(postgresWriterLiveSchemasLike(c, ctx, db, "%timescaledb%"), qt.HasLen, 0)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
		})
	}
}

// postgresWriterLiveRelationsLike counts the relations whose name matches
// pattern, in any schema.
func postgresWriterLiveRelationsLike(c *qt.C, ctx context.Context, db *sql.DB, pattern string) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pg_class WHERE relname LIKE $1 AND relkind IN ('r', 'p')", pattern,
	).Scan(&count), qt.IsNil)
	return count
}

// TestWriterDropDatabaseRealmKeeping_LivePostgres keeps one extension and not
// another. The kept one stays installed with everything it owns, including a
// member table in the schema the cleanup empties in place; the other goes, and
// so does every object the user made (stokaro/ptah#3542).
func TestWriterDropDatabaseRealmKeeping_LivePostgres(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE EXTENSION hstore;
		CREATE EXTENSION pg_trgm;
		CREATE TABLE public.member_table (id integer PRIMARY KEY);
		ALTER EXTENSION hstore ADD TABLE public.member_table;
		CREATE TABLE public.user_table (id integer PRIMARY KEY, attrs hstore);
		CREATE INDEX user_table_attrs ON public.user_table USING gist (attrs);
		CREATE SCHEMA user_schema;
		CREATE TABLE user_schema.t (id integer);
	`)
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealmKeeping(ctx, dbreset.Kept{Extensions: []string{"hstore"}})

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveExtensionNames(c, ctx, db), qt.DeepEquals, []string{"hstore", "plpgsql"})
	c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "user_schema"), qt.Equals, 0)
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "user_table"), qt.Equals, 0)
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "member_table"), qt.Equals, 1)
	c.Assert(postgresWriterLiveRoutineCount(c, ctx, db, "public", "hstore_in"), qt.Equals, 1)
	c.Assert(postgresWriterLiveRoutineCount(c, ctx, db, "public", "similarity"), qt.Equals, 0)
}

// TestWriterDropDatabaseRealmKeeping_LiveTimescaleDB keeps timescaledb while a
// hypertable holds a row: the extension and its schemas stay, and the
// hypertable goes with its chunk.
func TestWriterDropDatabaseRealmKeeping_LiveTimescaleDB(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.TimescaleDB))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE EXTENSION IF NOT EXISTS timescaledb;
		CREATE TABLE public.metrics (at timestamptz NOT NULL, value integer);
		SELECT create_hypertable('public.metrics', 'at');
		INSERT INTO public.metrics VALUES (now(), 1);
	`)
	c.Assert(err, qt.IsNil)
	extensionSchemas := postgresWriterLiveSchemasLike(c, ctx, db, "%timescaledb%")

	err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealmKeeping(ctx, dbreset.Kept{Extensions: []string{"timescaledb"}})

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveExtensionNames(c, ctx, db), qt.DeepEquals, []string{"plpgsql", "timescaledb"})
	c.Assert(postgresWriterLiveSchemasLike(c, ctx, db, "%timescaledb%"), qt.DeepEquals, extensionSchemas)
	c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "\\_hyper\\_%"), qt.Equals, 0)
}

// TestWriterDropAllTablesKeeping_LivePostgres empties the public schema while
// hstore, installed in it, is kept: the extension and a table it owns stay,
// and the user's objects in the schema go (stokaro/ptah#3810).
func TestWriterDropAllTablesKeeping_LivePostgres(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE EXTENSION hstore SCHEMA public;
		CREATE TABLE public.member_table (id integer PRIMARY KEY);
		ALTER EXTENSION hstore ADD TABLE public.member_table;
		CREATE TABLE public.user_table (id integer PRIMARY KEY, attrs hstore);
		CREATE VIEW public.user_view AS SELECT id FROM public.user_table;
	`)
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropAllTablesKeeping(ctx, dbreset.Kept{Extensions: []string{"hstore"}})

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveExtensionNames(c, ctx, db), qt.DeepEquals, []string{"hstore", "plpgsql"})
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "member_table"), qt.Equals, 1)
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "user_table"), qt.Equals, 0)
	c.Assert(postgresWriterLiveRoutineCount(c, ctx, db, "public", "hstore_in"), qt.Equals, 1)
}

// TestWriterDropAllTablesKeeping_LivePostgresRefusesAnExtensionItWasNotAsked
// is the control: an extension in the schema the caller did not name is still
// refused, because dropping it removes its members wherever they are.
func TestWriterDropAllTablesKeeping_LivePostgresRefusesAnExtensionItWasNotAsked(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE EXTENSION hstore SCHEMA public;
		CREATE EXTENSION pg_trgm SCHEMA public;
	`)
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropAllTablesKeeping(ctx, dbreset.Kept{Extensions: []string{"hstore"}})

	c.Assert(err, qt.ErrorMatches, `refusing to clean schema "public": extension "pg_trgm" is owned by it; .*`)
	c.Assert(postgresWriterLiveExtensionNames(c, ctx, db), qt.DeepEquals, []string{"hstore", "pg_trgm", "plpgsql"})
}

// TestWriterDropDatabaseRealmKeeping_LiveKeepsSchemas empties the realm of
// a database whose schema kept_schema is named: it stays with its table and
// row, while a user schema not named goes and public is emptied, even though
// the list names it too (stokaro/ptah#3808).
func TestWriterDropDatabaseRealmKeeping_LiveKeepsSchemas(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE SCHEMA kept_schema;
		CREATE TABLE kept_schema.kept (id integer PRIMARY KEY);
		INSERT INTO kept_schema.kept VALUES (1);
		CREATE SCHEMA user_schema;
		CREATE TABLE user_schema.t (id integer);
		CREATE TABLE public.user_table (id integer PRIMARY KEY);
	`)
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealmKeeping(ctx, dbreset.Kept{
		Schemas: []string{"kept_schema", "public"}, Server: dbreset.NamedServer,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "kept_schema"), qt.Equals, 1)
	c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "user_schema"), qt.Equals, 0)
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "user_table"), qt.Equals, 0)
	var rows int
	c.Assert(db.QueryRowContext(ctx, "SELECT count(*) FROM kept_schema.kept").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}

// TestWriterDropDatabaseRealmKeeping_LiveLeavesPublicBesideAnotherRoot
// cleans a realm whose root is app while the caller keeps public, which holds
// a table with a row. public is kept like any other schema: the cleanup
// neither drops it nor empties it in place. CockroachDB is the engine that
// empties public in place rather than dropping it, so it is the row that
// keeps that arm honest.
func TestWriterDropDatabaseRealmKeeping_LiveLeavesPublicBesideAnotherRoot(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
	}

	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, engine.engine))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			for _, statement := range []string{
				"CREATE TABLE public.user_table (id integer PRIMARY KEY)",
				"INSERT INTO public.user_table VALUES (1)",
				"CREATE SCHEMA app",
				"CREATE TABLE app.t (id integer PRIMARY KEY)",
			} {
				_, err := db.ExecContext(ctx, statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}

			err := postgres.NewPostgreSQLWriter(db, "app").DropDatabaseRealmKeeping(ctx, dbreset.Kept{
				Schemas: []string{"public"}, Server: dbreset.NamedServer,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "app"), qt.Equals, 1)
			c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "t"), qt.Equals, 0)
			var rows int
			c.Assert(db.QueryRowContext(ctx, "SELECT count(*) FROM public.user_table").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 1)
		})
	}
}

// TestWriterDropDatabaseRealm_LivePostgresNamesWhatKeepsASchema cleans a realm
// whose public schema holds a text search configuration and a dictionary,
// which the cleanup does not list. The schema drop is RESTRICT, so it fails
// and the cleanup rolls back; the error names both objects in the server's
// words, where the server's own error says only that other objects depend on
// the schema (stokaro/ptah#3851).
func TestWriterDropDatabaseRealm_LivePostgresNamesWhatKeepsASchema(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE TEXT SEARCH CONFIGURATION public.keep_ts (COPY = simple);
		CREATE TEXT SEARCH DICTIONARY public.keep_td (TEMPLATE = simple);
	`)
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealm(ctx)

	c.Assert(err, qt.ErrorMatches, `(?s)failed to drop user schema "public" from PostgreSQL database realm `+
		`\(text search configuration keep_ts depends on schema public; `+
		`text search dictionary keep_td depends on schema public\): .*cannot drop schema public.*`)
	var remaining int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM pg_ts_config WHERE cfgname = 'keep_ts')
		     + (SELECT count(*) FROM pg_ts_dict WHERE dictname = 'keep_td')`).Scan(&remaining), qt.IsNil)
	c.Assert(remaining, qt.Equals, 2)
}
