package postgres_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/aclitem"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

func TestWriterDropAllTables_CanceledContext(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, emptyPostgresCleanupQuery)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := writer.DropAllTables(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db.QueryCount(), qt.Equals, 0)
	c.Assert(db.BeginCount(), qt.Equals, 0)
}

func TestWriterDropAllTables_CommitsAllCatalogObjects(t *testing.T) {
	c := qt.New(t)
	var catalogQueries []string
	var catalogArgs [][]driver.NamedValue
	var execQueries []string
	var events []string
	db := dbtest.OpenWithExec(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		catalogQueries = append(catalogQueries, query)
		catalogArgs = append(catalogArgs, args)
		events = append(events, "query: "+query)
		return postgresCleanupQuery(query, args)
	}, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		events = append(events, "exec: "+query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(catalogQueries, qt.HasLen, 5)
	c.Assert(catalogQueries[0], qt.Equals, "SELECT version()")
	c.Assert(catalogQueries[1], qt.Contains, "SELECT COUNT(*)")
	c.Assert(catalogQueries[1], qt.Contains, "FROM pg_extension")
	c.Assert(catalogQueries[2], qt.Contains, "cleanup_objects")
	c.Assert(catalogQueries[2], qt.Contains, "view_dependencies")
	c.Assert(catalogQueries[2], qt.Contains, "ORDER BY priority, dependency_depth DESC")
	c.Assert(catalogQueries[2], qt.Contains, "FROM pg_constraint")
	c.Assert(catalogQueries[2], qt.Contains, "con.contype = 'f'")
	c.Assert(catalogQueries[2], qt.Contains, "p.prokind = 'f' OR p.prokind = 'p'")
	c.Assert(catalogQueries[2], qt.Contains, "d.deptype = 'i'")
	c.Assert(catalogQueries[2], qt.Contains, "t.typtype IN ('e', 'd', 'r')")
	c.Assert(catalogQueries[2], qt.Contains, "RESTRICT")
	c.Assert(catalogQueries[2], qt.Not(qt.Contains), "CASCADE")
	c.Assert(catalogQueries[2], qt.Not(qt.Contains), "dependent_objects")
	c.Assert(catalogQueries[2], qt.Not(qt.Contains), "d.deptype = 'e'")
	c.Assert(catalogQueries[2], qt.Not(qt.Contains), "pg_default_acl")
	c.Assert(catalogQueries[3], qt.Contains, "FROM pg_default_acl")
	c.Assert(catalogQueries[3], qt.Contains, "array_to_json(d.defaclacl)")
	c.Assert(catalogQueries[3], qt.Not(qt.Contains), "aclexplode")
	c.Assert(catalogQueries[4], qt.Contains, "FROM pg_inherits")
	c.Assert(catalogQueries[4], qt.Contains, "child.relispartition")
	c.Assert(catalogArgs[0], qt.HasLen, 0)
	c.Assert(catalogArgs[1], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[2], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[3], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[4], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(execQueries[0], qt.Equals,
		`LOCK TABLE "public"."remote_users", "public"."users" IN ACCESS EXCLUSIVE MODE`)
	c.Assert(events[0], qt.Contains, "query: SELECT version()")
	c.Assert(events[1], qt.Contains, "FROM pg_extension")
	c.Assert(events[2], qt.Contains, "cleanup_objects")
	c.Assert(events[3], qt.Contains, "FROM pg_default_acl")
	c.Assert(events[4], qt.Contains, "exec: LOCK TABLE")
	c.Assert(events[5], qt.Contains, "FROM pg_inherits")
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 5)
	c.Assert(db.ExecCount(), qt.Equals, 11)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropAllTables_RollsBackOnFailure(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, postgresCleanupQuery, failPostgresTypeDrop)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`refusing to clean schema "public": failed to drop type status: SQL execution failed: boom\nSQL: DROP TYPE IF EXISTS "public"."status" RESTRICT`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 5)
	c.Assert(db.ExecCount(), qt.Equals, 14)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropAllTables_RejectsSchemaScopedExtensions(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, postgresSchemaScopedExtensionQuery, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`refusing to clean schema "public": extension "hstore" is owned by it; schema-scoped cleanup cannot prove that every extension member is confined to the schema`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 2)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropAllTables_RejectsExternalPolicyDependency(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, postgresPolicyCleanupQuery, failPostgresPolicyDependency)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`refusing to clean schema "public": failed to drop function is_allowed: SQL execution failed: cannot drop function public\.is_allowed\(\) because policy audit_policy depends on it\nSQL: DROP FUNCTION IF EXISTS "public"."is_allowed"\(\) RESTRICT`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 5)
	c.Assert(db.ExecCount(), qt.Equals, 2)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropAllTables_ResolvesInternalDependencies(t *testing.T) {
	c := qt.New(t)
	execHandler := new(postgresInternalDependencyExec)
	db := dbtest.OpenWithExec(t, postgresInternalDependencyQuery, execHandler.execute)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 5)
	c.Assert(db.ExecCount(), qt.Equals, 5)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

// TestWriterDropAllTables_CockroachRetriesEachDropAlone drops on CockroachDB a
// schema whose table a view reads, listed table first. Each drop runs in a
// transaction of its own, so the refused table drop is sent again after the
// view, with no savepoint around it.
func TestWriterDropAllTables_CockroachRetriesEachDropAlone(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	execHandler := new(postgresInternalDependencyExec)
	db := dbtest.OpenWithExec(t, cockroachInternalDependencyQuery, func(
		query string,
		args []driver.NamedValue,
	) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return execHandler.execute(query, args)
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries, qt.DeepEquals, []string{
		`DROP TABLE IF EXISTS "public"."users" RESTRICT`,
		`DROP VIEW IF EXISTS "public"."active_users" RESTRICT`,
		`DROP TABLE IF EXISTS "public"."users" RESTRICT`,
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_RecreatesRootSchemaWithMetadata(t *testing.T) {
	c := qt.New(t)
	var catalogQueries []string
	var catalogArgs [][]driver.NamedValue
	var execQueries []string
	queryHandler := newPostgresRealmMetadataQuery()
	db := dbtest.OpenWithExec(t, func(
		query string,
		args []driver.NamedValue,
	) (dbtest.QueryResult, error) {
		catalogQueries = append(catalogQueries, query)
		catalogArgs = append(catalogArgs, args)
		return queryHandler.query(query, args)
	}, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(catalogQueries, qt.HasLen, 14)
	c.Assert(catalogQueries[0], qt.Equals, "SELECT version()")
	c.Assert(catalogQueries[1], qt.Equals, "SELECT current_database()")
	c.Assert(catalogQueries[2], qt.Contains, "database_scoped_artifacts")
	c.Assert(catalogQueries[2], qt.Contains, "FROM pg_publication")
	c.Assert(catalogQueries[2], qt.Contains, "FROM pg_subscription")
	c.Assert(catalogQueries[2], qt.Contains, "subscription.subdbid =")
	c.Assert(catalogQueries[2], qt.Contains, "FROM pg_replication_slots")
	c.Assert(catalogQueries[2], qt.Contains, "slot.slot_type = 'logical'")
	c.Assert(catalogQueries[2], qt.Contains, "slot.datoid =")
	c.Assert(catalogQueries[2], qt.Contains, "FROM pg_event_trigger")
	c.Assert(catalogQueries[3], qt.Contains, "obj_description")
	c.Assert(catalogQueries[4], qt.Contains, "aclexplode")
	c.Assert(catalogQueries[5], qt.Contains, "n.nspname NOT LIKE 'pg\\_%'")
	c.Assert(catalogQueries[5], qt.Not(qt.Contains), "n.nspname NOT LIKE 'yb\\_%'")
	c.Assert(catalogQueries[6], qt.Contains, "NOT pg_has_role(n.nspowner, 'USAGE')")
	c.Assert(catalogQueries[7], qt.Contains, "cleanup_objects")
	c.Assert(catalogQueries[7], qt.Contains, "DROP EXTENSION IF EXISTS %I RESTRICT")
	c.Assert(catalogQueries[7], qt.Contains, "DROP COLLATION IF EXISTS %I.%I RESTRICT")
	c.Assert(catalogQueries[8], qt.Contains, "FROM pg_default_acl")
	c.Assert(catalogQueries[8], qt.Contains, "WHEN d.defaclrole = 0 THEN ''")
	c.Assert(catalogQueries[9], qt.Contains, "AS builtin")
	c.Assert(catalogQueries[11], qt.Contains, "SELECT e.extname FROM pg_extension")
	c.Assert(catalogQueries[12], qt.Contains, "residual_objects")
	c.Assert(catalogQueries[12], qt.Contains, "FROM pg_collation")
	c.Assert(catalogQueries[12], qt.Contains, "FROM pg_default_acl")
	c.Assert(catalogQueries[13], qt.Contains, "FROM pg_largeobject_metadata")
	c.Assert(catalogArgs[0], qt.HasLen, 0)
	c.Assert(catalogArgs[1], qt.HasLen, 0)
	c.Assert(catalogArgs[2], qt.HasLen, 0)
	c.Assert(catalogArgs[3], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[4], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[5], qt.HasLen, 0)
	c.Assert(catalogArgs[6], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[7], qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "audit"},
		{Ordinal: 2, Value: "public"},
		{Ordinal: 3, Value: "plpgsql"},
	})
	c.Assert(catalogArgs[8], qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "audit"},
		{Ordinal: 2, Value: "public"},
	})
	c.Assert(catalogArgs[9], qt.HasLen, 0)
	c.Assert(catalogArgs[10], qt.HasLen, 0)
	c.Assert(catalogArgs[11], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "plpgsql"}})
	c.Assert(catalogArgs[12], qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "public"},
		{Ordinal: 2, Value: "plpgsql"},
	})
	c.Assert(catalogArgs[13], qt.HasLen, 0)
	c.Assert(execQueries, qt.DeepEquals, []string{
		"SAVEPOINT ptah_cleanup_object;\n" +
			`DROP EXTENSION IF EXISTS "hstore" RESTRICT;` +
			"\nRELEASE SAVEPOINT ptah_cleanup_object",
		"SAVEPOINT ptah_cleanup_object;\n" +
			`DROP COLLATION IF EXISTS "public"."ptah_case_sensitive" RESTRICT;` +
			"\nRELEASE SAVEPOINT ptah_cleanup_object",
		"SAVEPOINT ptah_cleanup_object;\n" +
			`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC;` +
			"\nRELEASE SAVEPOINT ptah_cleanup_object",
		"\n\t\tSELECT lo_unlink(oid)\n\t\tFROM pg_largeobject_metadata\n\t\tORDER BY oid\n\t",
		`DROP SCHEMA IF EXISTS "audit" RESTRICT`,
		`DROP SCHEMA IF EXISTS "public" RESTRICT`,
		`CREATE SCHEMA "public" AUTHORIZATION "app_owner"`,
		`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM PUBLIC`,
		`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM "app_owner"`,
		`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM "app_reader"`,
		`GRANT CREATE ON SCHEMA "public" TO "app_owner"`,
		`GRANT USAGE ON SCHEMA "public" TO "app_owner"`,
		`GRANT USAGE ON SCHEMA "public" TO "app_reader" WITH GRANT OPTION`,
		`GRANT USAGE ON SCHEMA "public" TO PUBLIC`,
		`COMMENT ON SCHEMA "public" IS 'application root'`,
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 14)
	c.Assert(db.ExecCount(), qt.Equals, 15)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

// A kept schema the connecting role does not own is emptied where it stands:
// only its owner may drop it, so dropped and created again it failed every
// cleanup with `must be owner of schema public` (SQLSTATE 42501), which is
// every dev database an ordinary role owns on PostgreSQL 13 and 14.
func TestWriterDropDatabaseRealm_EmptiesInPlaceAKeptSchemaTheRoleDoesNotOwn(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.undroppable = [][]driver.Value{{"public", "postgres", false}}
	db := dbtest.OpenWithExec(t, queryHandler.query, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})

	err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries, qt.DeepEquals, []string{
		"SAVEPOINT ptah_cleanup_object;\n" +
			`DROP EXTENSION IF EXISTS "hstore" RESTRICT;` +
			"\nRELEASE SAVEPOINT ptah_cleanup_object",
		"SAVEPOINT ptah_cleanup_object;\n" +
			`DROP COLLATION IF EXISTS "public"."ptah_case_sensitive" RESTRICT;` +
			"\nRELEASE SAVEPOINT ptah_cleanup_object",
		"SAVEPOINT ptah_cleanup_object;\n" +
			`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC;` +
			"\nRELEASE SAVEPOINT ptah_cleanup_object",
		"\n\t\tSELECT lo_unlink(oid)\n\t\tFROM pg_largeobject_metadata\n\t\tORDER BY oid\n\t",
		`DROP SCHEMA IF EXISTS "audit" RESTRICT`,
	})
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

// A role that holds a grant option on a kept schema it does not own could
// change the schema's privileges in a run, which a cleanup that may not drop
// the schema could not take back, so the cleanup refuses before it drops
// anything.
func TestWriterDropDatabaseRealm_RefusesAKeptSchemaTheRoleCanGrantOnButNotDrop(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.undroppable = [][]driver.Value{{"public", "postgres", true}}
	db := dbtest.OpenWithExec(t, queryHandler.query, nil)

	err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `refusing to clean PostgreSQL database realm: schema "public" is owned by "postgres", `+
		`not by the connecting role, and the role holds a grant option on it; the cleanup cannot drop the schema `+
		`or take back what a run grants on it, so connect as "postgres" or revoke the grant option`)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

// TestWriterDropDatabaseRealm_LeavesExtensionOwnedObjectsToTheExtension names
// the queries a realm cleanup narrows to what an extension does not own. An
// extension's drop takes its schemas and members with it, so statements queued
// for them run after they are gone and fail (stokaro/ptah#3540); a kept
// extension refuses them (stokaro/ptah#3542). The check that the cleanup
// finished leaves them out for the second reason and reads the extensions
// themselves by name. TestWriterDropAllTables_CommitsAllCatalogObjects holds
// the schema-scoped cleanup to no filter at all: it drops no extension.
func TestWriterDropDatabaseRealm_LeavesExtensionOwnedObjectsToTheExtension(t *testing.T) {
	c := qt.New(t)
	var catalogQueries []string
	queryHandler := newPostgresRealmMetadataQuery()
	db := dbtest.OpenWithExec(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		catalogQueries = append(catalogQueries, query)
		return queryHandler.query(query, args)
	}, func(string, []driver.NamedValue) (driver.Result, error) {
		return driver.RowsAffected(0), nil
	})

	err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(catalogQueries, qt.HasLen, 14)
	planned, objects, verified, residual := catalogQueries[5], catalogQueries[7], catalogQueries[10], catalogQueries[12]
	for _, schemas := range []string{planned, verified} {
		c.Assert(schemas, qt.Contains, "d.classid = 'pg_namespace'::regclass")
		c.Assert(schemas, qt.Contains, "d.deptype = 'e'")
	}
	for _, query := range []string{objects, residual} {
		c.Assert(query, qt.Contains, "WITH RECURSIVE")
		c.Assert(query, qt.Contains, "extension_owned(classid, objid) AS (")
		c.Assert(query, qt.Contains, "d.deptype IN ('i', 'a')")
	}
	for _, catalog := range []string{"pg_constraint", "pg_class", "pg_proc", "pg_type", "pg_collation"} {
		c.Assert(objects, qt.Contains, "owned.classid = '"+catalog+"'::regclass")
	}
	c.Assert(residual, qt.Contains, "owned.classid = residual.classid")
}

// TestWriterDropDatabaseRealmKeeping_PassesTheKeptExtensions pins where the
// kept names go: to the lookup of the schemas they live in, past the extension
// drop, past the check that no user extension survived, and past the residual
// extension rows. The server's own plpgsql joins them, sorted, everywhere but
// the lookup.
func TestWriterDropDatabaseRealmKeeping_PassesTheKeptExtensions(t *testing.T) {
	c := qt.New(t)
	var catalogQueries []string
	var catalogArgs [][]driver.NamedValue
	queryHandler := newPostgresRealmMetadataQuery()
	db := dbtest.OpenWithExec(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		catalogQueries = append(catalogQueries, query)
		catalogArgs = append(catalogArgs, args)
		return queryHandler.query(query, args)
	}, func(string, []driver.NamedValue) (driver.Result, error) {
		return driver.RowsAffected(0), nil
	})

	err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropDatabaseRealmKeeping(t.Context(), dbreset.Kept{Extensions: []string{"vector", "hstore"}})

	c.Assert(err, qt.IsNil)
	c.Assert(catalogQueries, qt.HasLen, 15)
	c.Assert(catalogQueries[6], qt.Contains, "JOIN pg_namespace n ON n.oid = e.extnamespace")
	c.Assert(catalogArgs[6], qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "vector"},
		{Ordinal: 2, Value: "hstore"},
	})
	c.Assert(catalogArgs[8], qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "audit"},
		{Ordinal: 2, Value: "public"},
		{Ordinal: 3, Value: "hstore"},
		{Ordinal: 4, Value: "plpgsql"},
		{Ordinal: 5, Value: "vector"},
	})
	c.Assert(catalogArgs[12], qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "hstore"},
		{Ordinal: 2, Value: "plpgsql"},
		{Ordinal: 3, Value: "vector"},
	})
	c.Assert(catalogArgs[13], qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "public"},
		{Ordinal: 2, Value: "hstore"},
		{Ordinal: 3, Value: "plpgsql"},
		{Ordinal: 4, Value: "vector"},
	})
}

// A cleanup whose root is another schema drops "public" with the rest of the
// realm and brings it back with the owner, grants and comment it had. Brought
// back bare, it was owned by the connecting role and PUBLIC lost USAGE on it
// (stokaro/ptah#3657).
func TestWriterDropDatabaseRealm_CreatesAbsentRootSchema(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	queryHandler := newPostgresRealmAbsentRootQuery()
	db := dbtest.OpenWithExec(t, queryHandler.query, func(
		query string,
		_ []driver.NamedValue,
	) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "shadow")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries, qt.DeepEquals, []string{
		"\n\t\tSELECT lo_unlink(oid)\n\t\tFROM pg_largeobject_metadata\n\t\tORDER BY oid\n\t",
		`DROP SCHEMA IF EXISTS "audit" RESTRICT`,
		`DROP SCHEMA IF EXISTS "public" RESTRICT`,
		`CREATE SCHEMA "shadow"`,
		`CREATE SCHEMA "public" AUTHORIZATION "pg_database_owner"`,
		`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM PUBLIC`,
		`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM "pg_database_owner"`,
		`GRANT USAGE ON SCHEMA "public" TO PUBLIC`,
		`GRANT CREATE ON SCHEMA "public" TO "pg_database_owner"`,
		`GRANT USAGE ON SCHEMA "public" TO "pg_database_owner"`,
		`COMMENT ON SCHEMA "public" IS 'standard public schema'`,
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 15)
	c.Assert(db.ExecCount(), qt.Equals, 11)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

// A database whose "public" schema was already gone gets nothing invented for
// it: the cleanup restores what it dropped, and a schema the caller never had
// is not part of that. This is what separates restoring "public" from creating
// it unconditionally -- both look identical in
// TestWriterDropDatabaseRealm_CreatesAbsentRootSchema, where it was present.
func TestWriterDropDatabaseRealm_LeavesAnAbsentPublicSchemaAbsent(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	queryHandler := newPostgresRealmAbsentPublicQuery()
	db := dbtest.OpenWithExec(t, queryHandler.query, func(
		query string,
		_ []driver.NamedValue,
	) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "shadow")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries, qt.DeepEquals, []string{
		"\n\t\tSELECT lo_unlink(oid)\n\t\tFROM pg_largeobject_metadata\n\t\tORDER BY oid\n\t",
		`DROP SCHEMA IF EXISTS "audit" RESTRICT`,
		`CREATE SCHEMA "shadow"`,
	})
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_CockroachPreservesPublicSchemaContainer(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	queryHandler := newPostgresRealmCockroachQuery()
	db := dbtest.OpenWithExec(t, queryHandler.query, func(
		query string,
		_ []driver.NamedValue,
	) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries, qt.DeepEquals, []string{
		`DROP TABLE IF EXISTS "public"."stale_items" RESTRICT`,
		`DROP SCHEMA IF EXISTS "audit" RESTRICT`,
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 15)
	c.Assert(db.ExecCount(), qt.Equals, 2)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_RollsBackOnResidualObject(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmResidualQuery()
	db := dbtest.OpenWithExec(t, queryHandler.query, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`PostgreSQL database realm cleanup left residual collation "public"\."stale_collation"`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 12)
	c.Assert(db.ExecCount(), qt.Equals, 4)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsSystemRootSchema(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, emptyPostgresCleanupQuery)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "pg_catalog")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`refusing to clean PostgreSQL database realm with system root schema "pg_catalog"`)
	c.Assert(db.BeginCount(), qt.Equals, 0)
	c.Assert(db.QueryCount(), qt.Equals, 0)
	c.Assert(db.ExecCount(), qt.Equals, 0)
}

// postgresFamilySystemDatabases are refused by a realm cleanup on every
// server: a template every later CREATE DATABASE copies, or a database the
// server keeps for itself. queries counts what the cleanup reads before it
// refuses: the version and the database, and on CockroachDB the session's
// autocommit_before_ddl between them.
var postgresFamilySystemDatabases = []struct {
	name     string
	version  string
	database string
	queries  int
}{
	{name: "postgres template0", version: "PostgreSQL 18.0", database: "template0", queries: 2},
	{name: "postgres template1", version: "PostgreSQL 18.0", database: "TEMPLATE1", queries: 2},
	{name: "cockroach system", version: "CockroachDB CCL v26.2.4", database: "system", queries: 3},
	{name: "yugabyte template0", version: "YugabyteDB 2026.1", database: "template0", queries: 2},
	{name: "yugabyte template1", version: "YugabyteDB 2026.1", database: "template1", queries: 2},
	{name: "yugabyte system platform", version: "YugabyteDB 2026.1", database: "system_platform", queries: 2},
}

// postgresFamilyDefaultDatabases are the user databases each server is created
// with. A realm cleanup refuses them on a server the operator named and cleans
// them on a server the run owns.
var postgresFamilyDefaultDatabases = []struct {
	name     string
	version  string
	database string
	// queries counts what a refusing cleanup reads, as for
	// postgresFamilySystemDatabases.
	queries int
}{
	{name: "postgres database", version: "PostgreSQL 18.0", database: "POSTGRES", queries: 2},
	{name: "cockroach defaultdb", version: "CockroachDB CCL v26.2.4", database: "defaultdb", queries: 3},
	{name: "cockroach postgres", version: "CockroachDB CCL v26.2.4", database: "postgres", queries: 3},
	{name: "yugabyte database", version: "YugabyteDB 2026.1", database: "yugabyte", queries: 2},
	{name: "yugabyte postgres", version: "YugabyteDB 2026.1", database: "postgres", queries: 2},
}

// realmCleanupServers are both answers to whose server a cleanup runs on.
var realmCleanupServers = []struct {
	name   string
	server dbreset.Server
}{
	{name: "named server", server: dbreset.NamedServer},
	{name: "owned server", server: dbreset.OwnedServer},
}

func TestWriterDropDatabaseRealm_RejectsSystemDatabasesBeforeMutation(t *testing.T) {
	for _, owner := range realmCleanupServers {
		for _, test := range postgresFamilySystemDatabases {
			t.Run(owner.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				queryHandler := &postgresRealmQuery{
					version:  test.version,
					database: test.database,
				}
				db := dbtest.Open(t, queryHandler.query)
				writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

				err := writer.DropDatabaseRealmKeeping(t.Context(), dbreset.Kept{Server: owner.server})

				c.Assert(err, qt.ErrorMatches, `refusing to clean protected PostgreSQL-family database "`+test.database+`"`)
				c.Assert(db.QueryCount(), qt.Equals, test.queries)
				c.Assert(db.BeginCount(), qt.Equals, 1)
				c.Assert(db.ExecCount(), qt.Equals, 0)
				c.Assert(db.CommitCount(), qt.Equals, 0)
				c.Assert(db.RollbackCount(), qt.Equals, 1)
			})
		}
	}
}

// TestWriterDropDatabaseRealm_RejectsDefaultDatabasesOnANamedServer refuses a
// server's default database before anything changes, and says the refusal is
// about the server: the same cleanup on a server the run owns runs.
func TestWriterDropDatabaseRealm_RejectsDefaultDatabasesOnANamedServer(t *testing.T) {
	for _, test := range postgresFamilyDefaultDatabases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			queryHandler := &postgresRealmQuery{
				version:  test.version,
				database: test.database,
			}
			db := dbtest.Open(t, queryHandler.query)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(err, qt.ErrorIs, dbreset.ErrServerDefaultDatabase)
			c.Assert(err, qt.ErrorMatches, `refusing to clean protected PostgreSQL-family database "`+test.database+`": `+
				`it is the server's default database, which is reset only on a server the run owns`)
			c.Assert(db.QueryCount(), qt.Equals, test.queries)
			c.Assert(db.ExecCount(), qt.Equals, 0)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}

// TestWriterDropDatabaseRealm_CleansTheDefaultDatabaseOfAnOwnedServer runs the
// realm cleanup a named server refuses, on each default database of a server
// the run owns. The control is the test above.
func TestWriterDropDatabaseRealm_CleansTheDefaultDatabaseOfAnOwnedServer(t *testing.T) {
	for _, test := range postgresFamilyDefaultDatabases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			queryHandler := newPostgresRealmMetadataQuery()
			queryHandler.version = test.version
			queryHandler.database = test.database
			queryHandler.publicObjects = nil
			db := dbtest.OpenWithExec(t, queryHandler.query, nil)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropDatabaseRealmKeeping(t.Context(), dbreset.Kept{Server: dbreset.OwnedServer})

			c.Assert(err, qt.IsNil)
			c.Assert(db.CommitCount(), qt.Equals, 1)
			c.Assert(db.RollbackCount(), qt.Equals, 0)
		})
	}
}

func TestWriterDropDatabaseRealm_PostgresAllowsOtherFamilyProtectedNames(t *testing.T) {
	tests := []string{"defaultdb", "system", "system_platform", "yugabyte"}

	for _, database := range tests {
		t.Run(database, func(t *testing.T) {
			c := qt.New(t)
			queryHandler := newPostgresRealmMetadataQuery()
			queryHandler.database = database
			queryHandler.publicObjects = nil
			db := dbtest.OpenWithExec(t, queryHandler.query, nil)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(db.CommitCount(), qt.Equals, 1)
			c.Assert(db.RollbackCount(), qt.Equals, 0)
		})
	}
}

func TestWriterDropDatabaseRealm_RejectsDatabaseScopedArtifactBeforeMutation(t *testing.T) {
	tests := []struct {
		name string
		kind string
		item string
	}{
		{name: "subscription", kind: "subscription", item: "events_subscription"},
		{name: "logical replication slot", kind: "logical replication slot", item: "events_slot"},
		{name: "event trigger", kind: "event trigger", item: "audit_ddl"},
		{name: "foreign-data wrapper", kind: "foreign-data wrapper", item: "custom_fdw"},
		{name: "foreign server", kind: "foreign server", item: "warehouse"},
		{name: "user mapping", kind: "user mapping", item: "app@warehouse"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			queryHandler := newPostgresRealmMetadataQuery()
			queryHandler.databaseArtifacts = [][]driver.Value{{test.kind, test.item}}
			db := dbtest.OpenWithExec(t, queryHandler.query, nil)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(
				err,
				qt.ErrorMatches,
				`refusing to clean PostgreSQL database realm with unsupported database-scoped `+
					test.kind+` "`+test.item+`"`,
			)
			c.Assert(db.BeginCount(), qt.Equals, 1)
			c.Assert(db.QueryCount(), qt.Equals, 3)
			c.Assert(db.ExecCount(), qt.Equals, 0)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}

// TestWriterDropDatabaseRealmKeeping_DropsThePublicationsTheRunCreated pins
// that a realm cleanup whose claim listed the database's objects drops a
// publication the claim did not find, before it changes anything else, and
// keeps the one the claim found.
func TestWriterDropDatabaseRealmKeeping_DropsThePublicationsTheRunCreated(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.databaseArtifacts = [][]driver.Value{{"publication", "Events Feed"}, {"publication", "supabase_realtime"}}
	db := dbtest.OpenWithExec(t, queryHandler.query, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealmKeeping(t.Context(), dbreset.Kept{
		ArtifactsRecorded: true,
		Artifacts:         []dbreset.Object{{Kind: "publication", Name: "supabase_realtime"}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries[0], qt.Equals, `DROP PUBLICATION "Events Feed"`)
	c.Assert(strings.Join(execQueries, "\n"), qt.Not(qt.Contains), "supabase_realtime")
	c.Assert(db.CommitCount(), qt.Equals, 1)
}

// TestWriterDropDatabaseRealm_RefusesBeforeDroppingAPublication pins that a
// realm cleanup refusing another artifact the run created drops no
// publication first.
func TestWriterDropDatabaseRealm_RefusesBeforeDroppingAPublication(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.databaseArtifacts = [][]driver.Value{{"publication", "events_publication"}, {"subscription", "events_subscription"}}
	db := dbtest.OpenWithExec(t, queryHandler.query, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealmKeeping(t.Context(), dbreset.Kept{ArtifactsRecorded: true})

	c.Assert(err, qt.ErrorMatches,
		`refusing to clean PostgreSQL database realm with unsupported database-scoped subscription "events_subscription"`)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

// TestWriterDropDatabaseRealm_RefusesAPublicationWithoutAClaim pins that a
// realm cleanup with no claim behind it refuses a publication rather than drop
// it: nothing says the run created it, and it may be the dev database's own.
func TestWriterDropDatabaseRealm_RefusesAPublicationWithoutAClaim(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.databaseArtifacts = [][]driver.Value{{"publication", "supabase_realtime"}}
	db := dbtest.OpenWithExec(t, queryHandler.query, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`refusing to clean PostgreSQL database realm with unsupported database-scoped publication "supabase_realtime"`)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_FailsClosedWithoutDatabaseArtifactVisibility(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.databaseArtifactErr = errors.New("permission denied for view pg_replication_slots")
	db := dbtest.OpenWithExec(t, queryHandler.query, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`failed to inspect PostgreSQL database-scoped artifacts: permission denied for view pg_replication_slots`,
	)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 3)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RollsBackOnPreservedDependency(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.publicObjects = [][]driver.Value{{
		"function",
		"public",
		"is_allowed",
		nil,
		`DROP FUNCTION IF EXISTS "public"."is_allowed"() RESTRICT`,
	}}
	queryHandler.defaultACLs = nil
	var execQueries []string
	db := dbtest.OpenWithExec(t, queryHandler.query, func(
		query string,
		args []driver.NamedValue,
	) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return failPostgresPreservedFunctionDrop(query, args)
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `(?s)refusing to clean PostgreSQL database realm: failed to drop function is_allowed: SQL execution failed: cannot drop function because other objects depend on it.*`)
	c.Assert(execQueries, qt.DeepEquals, []string{
		"SAVEPOINT ptah_cleanup_object;\n" +
			`DROP FUNCTION IF EXISTS "public"."is_allowed"() RESTRICT;` +
			"\nRELEASE SAVEPOINT ptah_cleanup_object",
		"ROLLBACK TO SAVEPOINT ptah_cleanup_object;\nRELEASE SAVEPOINT ptah_cleanup_object",
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 10)
	c.Assert(db.ExecCount(), qt.Equals, 2)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RollsBackOnResidualExtension(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmResidualExtensionQuery()
	db := dbtest.OpenWithExec(t, queryHandler.query, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`PostgreSQL database realm cleanup left residual user extension "hstore"`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 11)
	c.Assert(db.ExecCount(), qt.Equals, 4)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_CanceledContext(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, emptyPostgresCleanupQuery)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db.BeginCount(), qt.Equals, 0)
	c.Assert(db.QueryCount(), qt.Equals, 0)
	c.Assert(db.ExecCount(), qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_RejectsUnsupportedPrivilegeBeforeMutation(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmUnsupportedPrivilegeQuery()
	db := dbtest.OpenWithExec(t, queryHandler.query, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`refusing to restore unsupported PostgreSQL schema privilege "TEMPORARY"`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 5)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropAllTables_RejectsCrossSchemaPartitionEdges(t *testing.T) {
	tests := []postgresPartitionEdgeTest{
		{
			name:          "external child",
			managedName:   "events",
			parentSchema:  "public",
			parentName:    "events",
			childSchema:   "archive",
			childName:     "events_2025",
			wantErrorExpr: `refusing to clean schema "public": partition "archive"\."events_2025" is attached to partitioned table "public"\."events" across the schema boundary`,
		},
		{
			name:          "external parent",
			managedName:   "events_2025",
			parentSchema:  "archive",
			parentName:    "events",
			childSchema:   "public",
			childName:     "events_2025",
			wantErrorExpr: `refusing to clean schema "public": partition "public"\."events_2025" is attached to partitioned table "archive"\."events" across the schema boundary`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var execQueries []string
			db := dbtest.OpenWithExec(t, postgresPartitionEdgeQuery(test), func(
				query string,
				_ []driver.NamedValue,
			) (driver.Result, error) {
				execQueries = append(execQueries, query)
				return driver.RowsAffected(0), nil
			})
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErrorExpr)
			c.Assert(execQueries, qt.DeepEquals, []string{
				`LOCK TABLE "public"."` + test.managedName + `" IN ACCESS EXCLUSIVE MODE`,
			})
			c.Assert(db.BeginCount(), qt.Equals, 1)
			c.Assert(db.QueryCount(), qt.Equals, 5)
			c.Assert(db.ExecCount(), qt.Equals, 1)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}

func TestWriterDropAllTables_PreservesDropErrorWhenSavepointRecoveryFails(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, postgresPolicyCleanupQuery, failPostgresDropAndSavepointRollback)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(err.Error(), qt.Contains, "failed to roll back cleanup savepoint: rollback failed")
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 5)
	c.Assert(db.ExecCount(), qt.Equals, 2)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

// TestWriterDropAllTables_RevokesEachDefaultPrivilegeGranteeHappyPath feeds the
// cleanup the ACL shapes the declared lines store and pins the revoke it builds
// for each grantee, in the order it runs them.
//
// The FOR ALL ROLES row is CockroachDB's role 0, which the query selects as an
// empty grantor. The row with no grantor after the slash is CockroachDB
// v25.4.16's text[] ACL, the shape aclexplode answers no rows for, so a cleanup
// built on aclexplode revokes nothing there (stokaro/ptah#3802). The empty
// grantee is PUBLIC and is written as the keyword, while a role named "PUBLIC"
// stays quoted: written bare, its revoke would take the default from PUBLIC
// instead and leave the role's in place.
func TestWriterDropAllTables_RevokesEachDefaultPrivilegeGranteeHappyPath(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	db := dbtest.OpenWithExec(t, postgresDefaultACLCleanupQuery([][]driver.Value{
		{"public", "app_owner", "r", "TABLES", `["=r/app_owner","app_reader=ar/app_owner","\"odd role\"=w*/app_owner","\"PUBLIC\"=r/app_owner"]`},
		{"public", "", "S", "SEQUENCES", `["app_reader=U*/"]`},
		{"public", "app_owner", "S", "SEQUENCES", `["app_reader=rU/"]`},
	}), func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries, qt.DeepEquals, []string{
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA "public" REVOKE ALL PRIVILEGES ON SEQUENCES FROM "app_reader"`),
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON SEQUENCES FROM "app_reader"`),
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM "PUBLIC"`),
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC`),
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM "app_reader"`),
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM "odd role"`),
	})
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

// TestWriterDropAllTables_UnreadableDefaultACLFailurePath refuses a cleanup whose default
// privileges it cannot read, before anything is dropped. A grantee lost to a
// letter the parser did not understand is a default privilege the cleanup
// leaves behind while reporting a clean schema.
func TestWriterDropAllTables_UnreadableDefaultACLFailurePath(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	db := dbtest.OpenWithExec(t, postgresDefaultACLCleanupQuery([][]driver.Value{
		{"public", "app_owner", "r", "TABLES", `["app_reader=rZ/app_owner"]`},
	}), func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, aclitem.ErrMalformed)
	c.Assert(err, qt.ErrorMatches,
		`failed to read the default privileges app_owner set on TABLES in schema public: malformed ACL item: .*`)
	c.Assert(execQueries, qt.HasLen, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

// TestWriterDropAllTables_UnrevokedDefaultPrivilegeFailurePath names the
// default privilege a refused revoke left in place. Role 0 has no name, so the
// FOR ALL ROLES default is named for all roles, as the note on `db read` names
// it, rather than for an empty grantor.
func TestWriterDropAllTables_UnrevokedDefaultPrivilegeFailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, postgresDefaultACLCleanupQuery([][]driver.Value{
		{"public", "", "S", "SEQUENCES", `["app_reader=U*/"]`},
	}), failPostgresAllRolesRevoke)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`(?s)refusing to clean schema "public": failed to drop default privilege all roles/S/app_reader: SQL execution failed: boom.*`)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

// TestWriterDropAllTables_CockroachRevokesWhatShowNamesHappyPath revokes on
// CockroachDB the defaults SHOW DEFAULT PRIVILEGES names, one statement per
// grantor, class and grantee however many privileges each holds.
//
// CockroachDB v26.2.7 refuses every read of pg_default_acl once a default
// names a role that needs quoting, so a cleanup reading the relation there
// revokes nothing and stops (stokaro/ptah#3816); SHOW answers on every line.
// The rows are what v26.2.7 printed: the FOR ALL ROLES row has no role, PUBLIC
// is `public`, functions are `routines`, and DROP is a privilege PostgreSQL
// does not have, which the relation would not have shown at all.
func TestWriterDropAllTables_CockroachRevokesWhatShowNamesHappyPath(t *testing.T) {
	c := qt.New(t)
	var queries, execQueries []string
	db := dbtest.OpenWithExec(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		return cockroachShowDefaultPrivilegesQuery(query, args)
	}, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(queries[4:], qt.DeepEquals, []string{
		"SELECT rolname FROM pg_roles ORDER BY rolname",
		`SHOW DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA "public"`,
		`SHOW DEFAULT PRIVILEGES FOR ROLE "d-dash", "d_owner" IN SCHEMA "public"`,
	})
	c.Assert(execQueries, qt.DeepEquals, []string{
		`ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA "public" REVOKE ALL PRIVILEGES ON SEQUENCES FROM "d_reader"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "d_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TYPES FROM "d-dash"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "d_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "d_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM "d_reader"`,
	})
	c.Assert(db.CommitCount(), qt.Equals, 1)
}

// cockroachShowDefaultPrivilegesQuery plays CockroachDB v26.2.7 for a cleanup
// whose schema holds only default privileges, answering SHOW DEFAULT
// PRIVILEGES as that server did.
func cockroachShowDefaultPrivilegesQuery(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, postgresRoleNamesMarker):
		return postgresRoleNamesResult([][]driver.Value{{"d-dash"}, {"d_owner"}}), nil
	case strings.HasPrefix(query, postgresShowDefaultPrivilegesMarker+" FOR ALL ROLES"):
		return postgresShowDefaultPrivilegesResult([][]driver.Value{
			{nil, true, "sequences", "d_reader", "USAGE", false},
		}), nil
	case strings.HasPrefix(query, postgresShowDefaultPrivilegesMarker):
		return postgresShowDefaultPrivilegesResult([][]driver.Value{
			{"d_owner", false, "routines", "public", "EXECUTE", false},
			{"d_owner", false, "tables", "d_reader", "DROP", false},
			{"d_owner", false, "tables", "d_reader", "INSERT", false},
			{"d_owner", false, "tables", "d_reader", "SELECT", false},
			{"d_owner", false, "types", "d-dash", "USAGE", true},
		}), nil
	default:
		return postgresCleanupCatalogQuery(query, "CockroachDB CCL v26.2.7", nil)
	}
}

// postgresDefaultACLCleanupQuery answers a cleanup whose schema holds nothing
// but the default privileges acls names, one pg_default_acl row each.
func postgresDefaultACLCleanupQuery(acls [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, postgresDefaultACLMarker) {
			return postgresDefaultACLResult(acls), nil
		}
		return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", nil)
	}
}

// postgresCleanupSavepoint is statement as the PostgreSQL cleanup runs it,
// inside a savepoint of its own.
func postgresCleanupSavepoint(statement string) string {
	return "SAVEPOINT ptah_cleanup_object;\n" + statement + ";\nRELEASE SAVEPOINT ptah_cleanup_object"
}

type postgresPartitionEdgeTest struct {
	name          string
	managedName   string
	parentSchema  string
	parentName    string
	childSchema   string
	childName     string
	wantErrorExpr string
}

func emptyPostgresCleanupQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", nil)
}

func postgresCleanupQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", [][]driver.Value{
		{"constraint", "public", "users_parent_fkey", "users", `ALTER TABLE "public"."users" DROP CONSTRAINT IF EXISTS "users_parent_fkey" RESTRICT`},
		{"view", "public", "active_users", nil, `DROP VIEW IF EXISTS "public"."active_users" RESTRICT`},
		{"materialized view", "public", "user_stats", nil, `DROP MATERIALIZED VIEW IF EXISTS "public"."user_stats" RESTRICT`},
		{"foreign table", "public", "remote_users", nil, `DROP FOREIGN TABLE IF EXISTS "public"."remote_users" RESTRICT`},
		{"table", "public", "users", nil, `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
		{"sequence", "public", "users_id_seq", nil, `DROP SEQUENCE IF EXISTS "public"."users_id_seq" RESTRICT`},
		{"procedure", "public", "refresh_users", nil, `DROP PROCEDURE IF EXISTS "public"."refresh_users"() RESTRICT`},
		{"aggregate", "public", "sum_text", nil, `DROP AGGREGATE IF EXISTS "public"."sum_text"(text) RESTRICT`},
		{"function", "public", "normalize_email", nil, `DROP FUNCTION IF EXISTS "public"."normalize_email"(text) RESTRICT`},
		{"type", "public", "status", nil, `DROP TYPE IF EXISTS "public"."status" RESTRICT`},
	})
}

func postgresPolicyCleanupQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", [][]driver.Value{{
		"function",
		"public",
		"is_allowed",
		nil,
		`DROP FUNCTION IF EXISTS "public"."is_allowed"() RESTRICT`,
	}})
}

func postgresInternalDependencyQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", [][]driver.Value{
		{"table", "public", "users", nil, `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
		{"view", "public", "active_users", nil, `DROP VIEW IF EXISTS "public"."active_users" RESTRICT`},
	})
}

// cockroachInternalDependencyQuery is [postgresInternalDependencyQuery]
// answered by CockroachDB.
func cockroachInternalDependencyQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return postgresCleanupCatalogQuery(query, "CockroachDB CCL v26.4.1", [][]driver.Value{
		{"table", "public", "users", nil, `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
		{"view", "public", "active_users", nil, `DROP VIEW IF EXISTS "public"."active_users" RESTRICT`},
	})
}

type postgresRealmQuery struct {
	version             string
	database            string
	databaseArtifacts   [][]driver.Value
	databaseArtifactErr error
	metadata            [][]driver.Value
	privileges          [][]driver.Value
	// schemaMetadata and schemaPrivileges answer for the schema the query
	// names, where metadata and privileges answer for every schema.
	schemaMetadata    map[string][][]driver.Value
	schemaPrivileges  map[string][][]driver.Value
	initialSchemas    [][]driver.Value
	finalSchemas      [][]driver.Value
	publicObjects     [][]driver.Value
	defaultACLs       [][]driver.Value
	globalDefaultACLs [][]driver.Value
	// roles and globalShow answer the CockroachDB read of the global default
	// privileges: the role list, and SHOW DEFAULT PRIVILEGES without IN
	// SCHEMA.
	roles             [][]driver.Value
	globalShow        [][]driver.Value
	residualExtension [][]driver.Value
	residualObjects   [][]driver.Value
	// undroppable answers which kept schemas the connecting role may not
	// drop: name, owner, and whether the role holds a grant option on it.
	undroppable      [][]driver.Value
	schemaQueryCount int
}

func newPostgresRealmMetadataQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:  "PostgreSQL 18.0",
		database: "ptah_test",
		metadata: [][]driver.Value{{
			"app_owner",
			false,
			"application root",
		}},
		privileges: [][]driver.Value{
			{"app_owner", "CREATE", false},
			{"app_owner", "USAGE", false},
			{"app_reader", "USAGE", true},
			{"PUBLIC", "USAGE", false},
		},
		initialSchemas: [][]driver.Value{{"audit"}, {"public"}},
		finalSchemas:   [][]driver.Value{{"public"}},
		publicObjects: [][]driver.Value{
			{
				"extension",
				"public",
				"hstore",
				nil,
				`DROP EXTENSION IF EXISTS "hstore" RESTRICT`,
			},
			{
				"collation",
				"public",
				"ptah_case_sensitive",
				nil,
				`DROP COLLATION IF EXISTS "public"."ptah_case_sensitive" RESTRICT`,
			},
		},
		defaultACLs: [][]driver.Value{{"public", "app_owner", "r", "TABLES", `["=r/app_owner"]`}},
	}
}

// newPostgresRealmAbsentRootQuery answers for a database whose root schema,
// "shadow", does not exist, and whose "public" is the one PostgreSQL 15 and
// newer creates: owned by pg_database_owner, with USAGE for PUBLIC.
func newPostgresRealmAbsentRootQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:  "PostgreSQL 18.0",
		database: "ptah_test",
		schemaMetadata: map[string][][]driver.Value{
			"public": {{"pg_database_owner", false, `standard public schema`}},
		},
		schemaPrivileges: map[string][][]driver.Value{
			"public": {
				{"PUBLIC", "USAGE", false},
				{"pg_database_owner", "CREATE", false},
				{"pg_database_owner", "USAGE", false},
			},
		},
		initialSchemas: [][]driver.Value{{"audit"}, {"public"}},
		finalSchemas:   [][]driver.Value{{"public"}, {"shadow"}},
	}
}

func newPostgresRealmAbsentPublicQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:        "PostgreSQL 18.0",
		database:       "ptah_test",
		initialSchemas: [][]driver.Value{{"audit"}},
		finalSchemas:   [][]driver.Value{{"shadow"}},
	}
}

func newPostgresRealmCockroachQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:        "CockroachDB CCL v26.2.4",
		database:       "ptah_test",
		metadata:       [][]driver.Value{{"root", true, ""}},
		initialSchemas: [][]driver.Value{{"audit"}, {"public"}},
		finalSchemas:   [][]driver.Value{{"public"}},
		publicObjects: [][]driver.Value{{
			"table",
			"public",
			"stale_items",
			nil,
			`DROP TABLE IF EXISTS "public"."stale_items" RESTRICT`,
		}},
	}
}

func newPostgresRealmResidualQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:         "PostgreSQL 18.0",
		database:        "ptah_test",
		metadata:        [][]driver.Value{{"app_owner", true, ""}},
		initialSchemas:  [][]driver.Value{{"public"}},
		finalSchemas:    [][]driver.Value{{"public"}},
		residualObjects: [][]driver.Value{{"collation", "public", "stale_collation"}},
	}
}

func newPostgresRealmResidualExtensionQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:           "PostgreSQL 18.0",
		database:          "ptah_test",
		metadata:          [][]driver.Value{{"app_owner", true, ""}},
		initialSchemas:    [][]driver.Value{{"public"}},
		finalSchemas:      [][]driver.Value{{"public"}},
		residualExtension: [][]driver.Value{{"hstore"}},
	}
}

func newPostgresRealmUnsupportedPrivilegeQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:    "YugabyteDB 2026.1",
		database:   "ptah_test",
		metadata:   [][]driver.Value{{"yugabyte", false, ""}},
		privileges: [][]driver.Value{{"yugabyte", "TEMPORARY", false}},
	}
}

func (q *postgresRealmQuery) query(
	query string,
	args []driver.NamedValue,
) (dbtest.QueryResult, error) {
	var schema string
	if len(args) > 0 {
		schema, _ = args[0].Value.(string)
	}
	switch {
	case strings.Contains(query, autocommitBeforeDDLMarker):
		return autocommitBeforeDDLResult("on"), nil
	case strings.Contains(query, outsideTextDependentsMarker):
		return outsideTextDependentsResult(nil), nil
	case strings.Contains(query, outsideStructuredDependentsMarker):
		return outsideStructuredDependentsResult(nil), nil
	case strings.Contains(query, "NOT pg_has_role(n.nspowner, 'USAGE')"):
		return dbtest.QueryResult{
			Columns: []string{"nspname", "owner", "grantable"},
			Rows:    q.undroppable,
		}, nil
	case strings.Contains(query, "database_scoped_artifacts"):
		return dbtest.QueryResult{
			Columns: []string{"object_kind", "object_name"},
			Rows:    q.databaseArtifacts,
		}, q.databaseArtifactErr
	case strings.Contains(query, "current_database()"):
		return postgresCurrentDatabaseResult(q.database), nil
	case strings.Contains(query, "SELECT version()"):
		return postgresVersionResult(q.version), nil
	case strings.Contains(query, "obj_description"):
		rows, named := q.schemaMetadata[schema]
		if !named {
			rows = q.metadata
		}
		return dbtest.QueryResult{
			Columns: []string{"owner", "acl_is_default", "comment_statement"},
			Rows:    rows,
		}, nil
	case strings.Contains(query, "cleanup_objects"):
		return dbtest.QueryResult{
			Columns: []string{"object_kind", "object_schema", "object_name", "object_qualifier", "drop_statement"},
			Rows:    q.publicObjects,
		}, nil
	case strings.Contains(query, postgresGlobalDefaultACLMarker):
		return postgresGlobalDefaultACLResult(q.globalDefaultACLs), nil
	case strings.Contains(query, postgresDefaultACLMarker):
		return postgresDefaultACLResult(q.defaultACLs), nil
	case strings.Contains(query, postgresRoleNamesMarker):
		return postgresRoleNamesResult(q.roles), nil
	case strings.HasPrefix(query, postgresShowDefaultPrivilegesMarker) && !strings.Contains(query, " IN SCHEMA "):
		return postgresShowDefaultPrivilegesResult(q.globalShow), nil
	case strings.HasPrefix(query, postgresShowDefaultPrivilegesMarker):
		return postgresShowDefaultPrivilegesResult(nil), nil
	case strings.Contains(query, "aclexplode"):
		rows, named := q.schemaPrivileges[schema]
		if !named {
			rows = q.privileges
		}
		return dbtest.QueryResult{
			Columns: []string{"grantee", "privilege_type", "is_grantable"},
			Rows:    rows,
		}, nil
	case strings.Contains(query, "SELECT e.extname FROM pg_extension"):
		return dbtest.QueryResult{
			Columns: []string{"extname"},
			Rows:    q.residualExtension,
		}, nil
	case strings.Contains(query, "residual_objects"):
		return dbtest.QueryResult{
			Columns: []string{"object_kind", "nspname", "relname"},
			Rows:    q.residualObjects,
		}, nil
	case strings.Contains(query, "n.nspname NOT LIKE"):
		q.schemaQueryCount++
		if q.schemaQueryCount > 1 {
			return dbtest.QueryResult{
				Columns: []string{"nspname"},
				Rows:    q.finalSchemas,
			}, nil
		}
		return dbtest.QueryResult{
			Columns: []string{"nspname"},
			Rows:    q.initialSchemas,
		}, nil
	default:
		return dbtest.QueryResult{}, nil
	}
}

func postgresCurrentDatabaseResult(database string) dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"current_database"},
		Rows:    [][]driver.Value{{database}},
	}
}

func failPostgresPreservedFunctionDrop(
	query string,
	_ []driver.NamedValue,
) (driver.Result, error) {
	if strings.Contains(query, "DROP FUNCTION") {
		return nil, errors.New("cannot drop function because other objects depend on it")
	}
	return driver.RowsAffected(0), nil
}

func postgresPartitionEdgeQuery(test postgresPartitionEdgeTest) func(
	string,
	[]driver.NamedValue,
) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "FROM pg_inherits") {
			return dbtest.QueryResult{
				Columns: []string{"parent_schema", "parent_name", "child_schema", "child_name"},
				Rows: [][]driver.Value{{
					test.parentSchema,
					test.parentName,
					test.childSchema,
					test.childName,
				}},
			}, nil
		}
		return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", [][]driver.Value{{
			"table",
			"public",
			test.managedName,
			`DROP TABLE IF EXISTS "public".` + quotePostgresTestIdent(test.managedName) + ` RESTRICT`,
		}})
	}
}

func postgresSchemaScopedExtensionQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if strings.Contains(query, "SELECT version()") {
		return postgresVersionResult("PostgreSQL 18.0"), nil
	}
	return dbtest.QueryResult{
		Columns: []string{"count", "first"},
		Rows:    [][]driver.Value{{int64(1), "hstore"}},
	}, nil
}

func postgresCleanupCatalogQuery(
	query,
	version string,
	objects [][]driver.Value,
) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "SELECT version()"):
		return postgresVersionResult(version), nil
	case strings.Contains(query, autocommitBeforeDDLMarker):
		return autocommitBeforeDDLResult("on"), nil
	case strings.Contains(query, outsideTextDependentsMarker):
		return outsideTextDependentsResult(nil), nil
	case strings.Contains(query, outsideStructuredDependentsMarker):
		return outsideStructuredDependentsResult(nil), nil
	case strings.Contains(query, "SELECT COUNT(*)") && strings.Contains(query, "FROM pg_extension"):
		return noPostgresSchemaOwnedExtensions(), nil
	case strings.Contains(query, "FROM pg_inherits"):
		return noPostgresCrossSchemaPartitionEdges(), nil
	case strings.Contains(query, postgresGlobalDefaultACLMarker):
		return postgresGlobalDefaultACLResult(nil), nil
	case strings.Contains(query, postgresDefaultACLMarker):
		return postgresDefaultACLResult(nil), nil
	case strings.Contains(query, postgresRoleNamesMarker):
		return postgresRoleNamesResult(nil), nil
	case strings.HasPrefix(query, postgresShowDefaultPrivilegesMarker):
		return postgresShowDefaultPrivilegesResult(nil), nil
	default:
		return dbtest.QueryResult{
			Columns: []string{"object_kind", "object_schema", "object_name", "object_qualifier", "drop_statement"},
			Rows:    objects,
		}, nil
	}
}

// autocommitBeforeDDLMarker is what the CockroachDB cleanup's read of its
// session setting selects, and no other statement the writer sends does.
const autocommitBeforeDDLMarker = "current_setting('autocommit_before_ddl', true)"

// autocommitBeforeDDLResult answers the read of autocommit_before_ddl with
// setting, which is "on", "off", or empty for a server without it.
func autocommitBeforeDDLResult(setting string) dbtest.QueryResult {
	return dbtest.QueryResult{Columns: []string{"coalesce"}, Rows: [][]driver.Value{{setting}}}
}

// outsideStructuredDependentsMarker is what the CockroachDB cleanup's read of
// the catalog rows that record a dependency on its objects selects, and no
// other statement does.
const outsideStructuredDependentsMarker = "'signature type'"

// outsideStructuredDependentsResult answers that read, one row per
// dependency: how it is recorded, the source's family, schema and name, the
// dependent's family, schema, name and kind, and the constraint, trigger or
// column that carries it.
func outsideStructuredDependentsResult(rows [][]driver.Value) dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{
			"how", "source_family", "source_schema", "source_name",
			"dependent_family", "dependent_schema", "dependent_name", "dependent_kind", "carrier",
		},
		Rows: rows,
	}
}

// outsideTextDependentsMarker is what the CockroachDB cleanup's read of the
// column defaults and routine bodies it matches as text selects, and no other
// statement does.
const outsideTextDependentsMarker = "FROM pg_attrdef ad"

// outsideTextDependentsResult answers that read: a default or a body with the
// schema, name and kind of the object that holds it, the column of a default,
// and the text; or a sequence or a type with its OID, or the database name.
func outsideTextDependentsResult(rows [][]driver.Value) dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"what", "schema", "name", "kind", "column", "text"},
		Rows:    rows,
	}
}

// postgresDefaultACLMarker is what the cleanup's default privilege query
// selects and no other statement the writer sends does: the ACL, whole.
const postgresDefaultACLMarker = "array_to_json(d.defaclacl)"

// postgresGlobalDefaultACLMarker is what the realm cleanup's read of the global
// default privileges selects beside each list, and no other statement does: the
// built-in default.
const postgresGlobalDefaultACLMarker = "AS builtin"

// postgresGlobalDefaultACLResult answers the read of the global default
// privileges, one row per global pg_default_acl row: grantor, class keyword,
// the ACL and the built-in ACL, each as array_to_json renders it.
func postgresGlobalDefaultACLResult(rows [][]driver.Value) dbtest.QueryResult {
	return dbtest.QueryResult{Columns: []string{"grantor", "object_type", "acl", "builtin"}, Rows: rows}
}

// postgresRoleNamesMarker is the role list the CockroachDB cleanup reads to
// ask SHOW DEFAULT PRIVILEGES about every role.
const postgresRoleNamesMarker = "SELECT rolname FROM pg_roles ORDER BY rolname"

// postgresRoleNamesResult answers the role list, one role per row.
func postgresRoleNamesResult(rows [][]driver.Value) dbtest.QueryResult {
	return dbtest.QueryResult{Columns: []string{"rolname"}, Rows: rows}
}

// postgresShowDefaultPrivilegesMarker is what every statement the CockroachDB
// cleanup reads its default privileges with starts with.
const postgresShowDefaultPrivilegesMarker = "SHOW DEFAULT PRIVILEGES"

// postgresShowDefaultPrivilegesResult answers SHOW DEFAULT PRIVILEGES in the
// columns CockroachDB gives it, one row per privilege.
func postgresShowDefaultPrivilegesResult(rows [][]driver.Value) dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"role", "for_all_roles", "object_type", "grantee", "privilege_type", "is_grantable"},
		Rows:    rows,
	}
}

// postgresDefaultACLResult answers the cleanup's default privilege query, one
// row per pg_default_acl row: schema, grantor, class code, class keyword and
// the ACL as array_to_json renders it.
func postgresDefaultACLResult(rows [][]driver.Value) dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"nspname", "grantor", "defaclobjtype", "object_type", "acl"},
		Rows:    rows,
	}
}

func postgresVersionResult(version string) dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"version"},
		Rows:    [][]driver.Value{{version}},
	}
}

func noPostgresSchemaOwnedExtensions() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"count", "first"},
		Rows:    [][]driver.Value{{int64(0), ""}},
	}
}

func noPostgresCrossSchemaPartitionEdges() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"parent_schema", "parent_name", "child_schema", "child_name"},
	}
}

func quotePostgresTestIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func failPostgresTypeDrop(query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "DROP TYPE") {
		return nil, errors.New("boom")
	}
	return driver.RowsAffected(0), nil
}

func failPostgresAllRolesRevoke(query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "FOR ALL ROLES") {
		return nil, errors.New("boom")
	}
	return driver.RowsAffected(0), nil
}

func failPostgresPolicyDependency(query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "DROP FUNCTION") {
		return nil, errors.New("cannot drop function public.is_allowed() because policy audit_policy depends on it")
	}
	return driver.RowsAffected(0), nil
}

func failPostgresDropAndSavepointRollback(query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "DROP FUNCTION") {
		return nil, context.Canceled
	}
	if strings.Contains(query, "ROLLBACK TO SAVEPOINT") {
		return nil, errors.New("rollback failed")
	}
	return driver.RowsAffected(0), nil
}

type postgresInternalDependencyExec struct {
	viewDropped bool
}

func (e *postgresInternalDependencyExec) execute(
	query string,
	_ []driver.NamedValue,
) (driver.Result, error) {
	if strings.Contains(query, "DROP VIEW") {
		e.viewDropped = true
	}
	if strings.Contains(query, "DROP TABLE") && !e.viewDropped {
		return nil, errors.New("cannot drop table because view depends on it")
	}
	return driver.RowsAffected(0), nil
}

// TestWriterDropDatabaseRealm_DialectRootSchema_FailurePath refuses
// a root schema the writer's dialect owns, or any member owns for a writer
// built without one, before anything is sent.
func TestWriterDropDatabaseRealm_DialectRootSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "CockroachDB's own", dialect: platform.CockroachDB, schema: "crdb_internal"},
		{name: "any member's, without a dialect", dialect: "", schema: "crdb_internal"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, emptyPostgresCleanupQuery)
			writer := postgres.NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, test.schema, test.dialect, capability.ForDialect(platform.Postgres))

			err := writer.DropDatabaseRealm(c.Context())

			c.Assert(err, qt.ErrorMatches,
				`refusing to clean PostgreSQL database realm with system root schema "`+test.schema+`"`)
			c.Assert(db.BeginCount(), qt.Equals, 0)
			c.Assert(db.QueryCount(), qt.Equals, 0)
		})
	}
}

// TestWriterDropDatabaseRealm_DialectRootSchema_HappyPath cleans a
// root schema another member owns: crdb_internal is a user's schema on
// PostgreSQL 18, so the cleanup opens its transaction.
func TestWriterDropDatabaseRealm_DialectRootSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, emptyPostgresCleanupQuery)
	writer := postgres.NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, "crdb_internal", platform.Postgres, capability.ForDialect(platform.Postgres))

	_ = writer.DropDatabaseRealm(c.Context())

	c.Assert(db.BeginCount(), qt.Equals, 1)
}
