package postgres_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/dbschema/dbtest"
	"github.com/stokaro/ptah/internal/dbschema/postgres"
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
	c.Assert(catalogQueries, qt.HasLen, 4)
	c.Assert(catalogQueries[0], qt.Equals, "SELECT version()")
	c.Assert(catalogQueries[1], qt.Contains, "SELECT COUNT(*)")
	c.Assert(catalogQueries[1], qt.Contains, "FROM pg_extension")
	c.Assert(catalogQueries[2], qt.Contains, "cleanup_objects")
	c.Assert(catalogQueries[2], qt.Contains, "view_dependencies")
	c.Assert(catalogQueries[2], qt.Contains, "ORDER BY priority, dependency_depth DESC")
	c.Assert(catalogQueries[2], qt.Contains, "FROM pg_constraint")
	c.Assert(catalogQueries[2], qt.Contains, "con.contype = 'f'")
	c.Assert(catalogQueries[2], qt.Contains, "p.prokind IN ('f', 'p', 'a', 'w')")
	c.Assert(catalogQueries[2], qt.Contains, "d.deptype = 'i'")
	c.Assert(catalogQueries[2], qt.Contains, "t.typtype IN ('e', 'd', 'r')")
	c.Assert(catalogQueries[2], qt.Contains, "RESTRICT")
	c.Assert(catalogQueries[2], qt.Not(qt.Contains), "CASCADE")
	c.Assert(catalogQueries[2], qt.Not(qt.Contains), "dependent_objects")
	c.Assert(catalogQueries[2], qt.Not(qt.Contains), "d.deptype = 'e'")
	c.Assert(catalogQueries[3], qt.Contains, "FROM pg_inherits")
	c.Assert(catalogQueries[3], qt.Contains, "child.relispartition")
	c.Assert(catalogArgs[0], qt.HasLen, 0)
	c.Assert(catalogArgs[1], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[2], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[3], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(execQueries[0], qt.Equals,
		`LOCK TABLE "public"."remote_users", "public"."users" IN ACCESS EXCLUSIVE MODE`)
	c.Assert(events[0], qt.Contains, "query: SELECT version()")
	c.Assert(events[1], qt.Contains, "FROM pg_extension")
	c.Assert(events[2], qt.Contains, "cleanup_objects")
	c.Assert(events[3], qt.Contains, "exec: LOCK TABLE")
	c.Assert(events[4], qt.Contains, "FROM pg_inherits")
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 4)
	c.Assert(db.ExecCount(), qt.Equals, 31)
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
	c.Assert(db.QueryCount(), qt.Equals, 4)
	c.Assert(db.ExecCount(), qt.Equals, 36)
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
	c.Assert(db.QueryCount(), qt.Equals, 4)
	c.Assert(db.ExecCount(), qt.Equals, 4)
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
	c.Assert(db.QueryCount(), qt.Equals, 4)
	c.Assert(db.ExecCount(), qt.Equals, 11)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropAllTables_CockroachUsesCatalogOrderWithoutSavepoints(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	db := dbtest.OpenWithExec(t, cockroachOrderedDependencyQuery, func(
		query string,
		_ []driver.NamedValue,
	) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries, qt.DeepEquals, []string{
		`DROP VIEW IF EXISTS "public"."z_dependent_view" RESTRICT`,
		`DROP VIEW IF EXISTS "public"."a_base_view" RESTRICT`,
		`DROP TABLE IF EXISTS "public"."users" RESTRICT`,
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 3)
	c.Assert(db.ExecCount(), qt.Equals, 3)
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
	c.Assert(catalogQueries, qt.HasLen, 9)
	c.Assert(catalogQueries[0], qt.Equals, "SELECT version()")
	c.Assert(catalogQueries[1], qt.Equals, "SELECT current_database()")
	c.Assert(catalogQueries[2], qt.Contains, "obj_description")
	c.Assert(catalogQueries[2], qt.Contains, "COMMENT ON SCHEMA %I IS %L")
	c.Assert(catalogQueries[3], qt.Contains, "aclexplode")
	c.Assert(catalogQueries[4], qt.Contains, "n.nspname NOT LIKE 'pg\\_%'")
	c.Assert(catalogQueries[4], qt.Not(qt.Contains), "n.nspname NOT LIKE 'yb\\_%'")
	c.Assert(catalogQueries[5], qt.Contains, "cleanup_objects")
	c.Assert(catalogQueries[5], qt.Contains, "DROP EXTENSION IF EXISTS %I RESTRICT")
	c.Assert(catalogQueries[5], qt.Contains, "DROP COLLATION IF EXISTS %I.%I RESTRICT")
	c.Assert(catalogQueries[5], qt.Contains, "ALTER DEFAULT PRIVILEGES FOR ROLE %I")
	c.Assert(catalogQueries[7], qt.Contains, "SELECT e.extname FROM pg_extension")
	c.Assert(catalogQueries[8], qt.Contains, "residual_objects")
	c.Assert(catalogQueries[8], qt.Contains, "FROM pg_collation")
	c.Assert(catalogQueries[8], qt.Contains, "FROM pg_default_acl")
	c.Assert(catalogArgs[0], qt.HasLen, 0)
	c.Assert(catalogArgs[1], qt.HasLen, 0)
	c.Assert(catalogArgs[2], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[3], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(catalogArgs[4], qt.HasLen, 0)
	c.Assert(catalogArgs[5], qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "audit"},
		{Ordinal: 2, Value: "public"},
		{Ordinal: 3, Value: "plpgsql"},
	})
	c.Assert(catalogArgs[6], qt.HasLen, 0)
	c.Assert(catalogArgs[7], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "plpgsql"}})
	c.Assert(catalogArgs[8], qt.DeepEquals, []driver.NamedValue{{Ordinal: 1, Value: "public"}})
	c.Assert(execQueries, qt.DeepEquals, []string{
		`SAVEPOINT ptah_cleanup_object`,
		`DROP EXTENSION IF EXISTS "hstore" RESTRICT`,
		`RELEASE SAVEPOINT ptah_cleanup_object`,
		`SAVEPOINT ptah_cleanup_object`,
		`DROP COLLATION IF EXISTS "public"."ptah_case_sensitive" RESTRICT`,
		`RELEASE SAVEPOINT ptah_cleanup_object`,
		`SAVEPOINT ptah_cleanup_object`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC`,
		`RELEASE SAVEPOINT ptah_cleanup_object`,
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
	c.Assert(db.QueryCount(), qt.Equals, 9)
	c.Assert(db.ExecCount(), qt.Equals, 20)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

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
		`DROP SCHEMA IF EXISTS "audit" RESTRICT`,
		`DROP SCHEMA IF EXISTS "public" RESTRICT`,
		`CREATE SCHEMA "shadow"`,
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 8)
	c.Assert(db.ExecCount(), qt.Equals, 3)
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
	c.Assert(db.QueryCount(), qt.Equals, 8)
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
	c.Assert(db.QueryCount(), qt.Equals, 8)
	c.Assert(db.ExecCount(), qt.Equals, 3)
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

func TestWriterDropDatabaseRealm_RejectsProtectedDatabasesBeforeMutation(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name     string
		version  string
		database string
	}{
		{name: "postgres database", version: "PostgreSQL 18.0", database: "POSTGRES"},
		{name: "postgres template0", version: "PostgreSQL 18.0", database: "template0"},
		{name: "postgres template1", version: "PostgreSQL 18.0", database: "template1"},
		{name: "cockroach defaultdb", version: "CockroachDB CCL v26.2.4", database: "defaultdb"},
		{name: "cockroach postgres", version: "CockroachDB CCL v26.2.4", database: "postgres"},
		{name: "cockroach system", version: "CockroachDB CCL v26.2.4", database: "system"},
		{name: "yugabyte database", version: "YugabyteDB 2026.1", database: "yugabyte"},
		{name: "yugabyte postgres", version: "YugabyteDB 2026.1", database: "postgres"},
		{name: "yugabyte template0", version: "YugabyteDB 2026.1", database: "template0"},
		{name: "yugabyte template1", version: "YugabyteDB 2026.1", database: "template1"},
		{name: "yugabyte system platform", version: "YugabyteDB 2026.1", database: "system_platform"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			queryHandler := &postgresRealmQuery{
				version:  test.version,
				database: test.database,
			}
			db := dbtest.Open(t, queryHandler.query)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(
				err,
				qt.ErrorMatches,
				`refusing to clean protected PostgreSQL-family database ".*"`,
			)
			c.Assert(db.QueryCount(), qt.Equals, 2)
			c.Assert(db.BeginCount(), qt.Equals, 1)
			c.Assert(db.ExecCount(), qt.Equals, 0)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}

func TestWriterDropDatabaseRealm_PostgresAllowsOtherFamilyProtectedNames(t *testing.T) {
	c := qt.New(t)
	tests := []string{"defaultdb", "system", "system_platform", "yugabyte"}

	for _, database := range tests {
		c.Run(database, func(c *qt.C) {
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

func TestWriterDropDatabaseRealm_RollsBackOnPreservedDependency(t *testing.T) {
	c := qt.New(t)
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.publicObjects = [][]driver.Value{{
		"function",
		"public",
		"is_allowed",
		`DROP FUNCTION IF EXISTS "public"."is_allowed"() RESTRICT`,
	}}
	db := dbtest.OpenWithExec(t, queryHandler.query, failPostgresPreservedFunctionDrop)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `(?s)refusing to clean PostgreSQL database realm: failed to drop function is_allowed: SQL execution failed: cannot drop function because other objects depend on it.*`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 6)
	c.Assert(db.ExecCount(), qt.Equals, 4)
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
	c.Assert(db.QueryCount(), qt.Equals, 7)
	c.Assert(db.ExecCount(), qt.Equals, 3)
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
	c.Assert(db.QueryCount(), qt.Equals, 4)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropAllTables_RejectsCrossSchemaPartitionEdges(t *testing.T) {
	c := qt.New(t)
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
		c.Run(test.name, func(c *qt.C) {
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
			c.Assert(db.QueryCount(), qt.Equals, 4)
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
	c.Assert(db.QueryCount(), qt.Equals, 4)
	c.Assert(db.ExecCount(), qt.Equals, 4)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
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
		{"constraint", "public", "users_parent_fkey", `ALTER TABLE "public"."users" DROP CONSTRAINT IF EXISTS "users_parent_fkey" RESTRICT`},
		{"view", "public", "active_users", `DROP VIEW IF EXISTS "public"."active_users" RESTRICT`},
		{"materialized view", "public", "user_stats", `DROP MATERIALIZED VIEW IF EXISTS "public"."user_stats" RESTRICT`},
		{"foreign table", "public", "remote_users", `DROP FOREIGN TABLE IF EXISTS "public"."remote_users" RESTRICT`},
		{"table", "public", "users", `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
		{"sequence", "public", "users_id_seq", `DROP SEQUENCE IF EXISTS "public"."users_id_seq" RESTRICT`},
		{"procedure", "public", "refresh_users", `DROP PROCEDURE IF EXISTS "public"."refresh_users"() RESTRICT`},
		{"aggregate", "public", "sum_text", `DROP AGGREGATE IF EXISTS "public"."sum_text"(text) RESTRICT`},
		{"function", "public", "normalize_email", `DROP FUNCTION IF EXISTS "public"."normalize_email"(text) RESTRICT`},
		{"type", "public", "status", `DROP TYPE IF EXISTS "public"."status" RESTRICT`},
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
		`DROP FUNCTION IF EXISTS "public"."is_allowed"() RESTRICT`,
	}})
}

func postgresInternalDependencyQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", [][]driver.Value{
		{"table", "public", "users", `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
		{"view", "public", "active_users", `DROP VIEW IF EXISTS "public"."active_users" RESTRICT`},
	})
}

func cockroachOrderedDependencyQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return postgresCleanupCatalogQuery(
		query,
		"CockroachDB CCL v26.2.4",
		[][]driver.Value{
			{"view", "public", "z_dependent_view", `DROP VIEW IF EXISTS "public"."z_dependent_view" RESTRICT`},
			{"view", "public", "a_base_view", `DROP VIEW IF EXISTS "public"."a_base_view" RESTRICT`},
			{"table", "public", "users", `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
		},
	)
}

type postgresRealmQuery struct {
	version           string
	database          string
	metadata          [][]driver.Value
	privileges        [][]driver.Value
	initialSchemas    [][]driver.Value
	finalSchemas      [][]driver.Value
	publicObjects     [][]driver.Value
	residualExtension [][]driver.Value
	residualObjects   [][]driver.Value
	schemaQueryCount  int
}

func newPostgresRealmMetadataQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:  "PostgreSQL 18.0",
		database: "ptah_test",
		metadata: [][]driver.Value{{
			"app_owner",
			false,
			`COMMENT ON SCHEMA "public" IS 'application root'`,
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
				`DROP EXTENSION IF EXISTS "hstore" RESTRICT`,
			},
			{
				"collation",
				"public",
				"ptah_case_sensitive",
				`DROP COLLATION IF EXISTS "public"."ptah_case_sensitive" RESTRICT`,
			},
			{
				"default privilege",
				"public",
				"app_owner/r/PUBLIC",
				`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC`,
			},
		},
	}
}

func newPostgresRealmAbsentRootQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:        "PostgreSQL 18.0",
		database:       "ptah_test",
		initialSchemas: [][]driver.Value{{"audit"}, {"public"}},
		finalSchemas:   [][]driver.Value{{"shadow"}},
	}
}

func newPostgresRealmCockroachQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:        "CockroachDB CCL v26.2.4",
		database:       "ptah_test",
		metadata:       [][]driver.Value{{"root", true, `COMMENT ON SCHEMA "public" IS NULL`}},
		initialSchemas: [][]driver.Value{{"audit"}, {"public"}},
		finalSchemas:   [][]driver.Value{{"public"}},
		publicObjects: [][]driver.Value{{
			"table",
			"public",
			"stale_items",
			`DROP TABLE IF EXISTS "public"."stale_items" RESTRICT`,
		}},
	}
}

func newPostgresRealmResidualQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:         "PostgreSQL 18.0",
		database:        "ptah_test",
		metadata:        [][]driver.Value{{"app_owner", true, `COMMENT ON SCHEMA "public" IS NULL`}},
		initialSchemas:  [][]driver.Value{{"public"}},
		finalSchemas:    [][]driver.Value{{"public"}},
		residualObjects: [][]driver.Value{{"collation", "public", "stale_collation"}},
	}
}

func newPostgresRealmResidualExtensionQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:           "PostgreSQL 18.0",
		database:          "ptah_test",
		metadata:          [][]driver.Value{{"app_owner", true, `COMMENT ON SCHEMA "public" IS NULL`}},
		initialSchemas:    [][]driver.Value{{"public"}},
		finalSchemas:      [][]driver.Value{{"public"}},
		residualExtension: [][]driver.Value{{"hstore"}},
	}
}

func newPostgresRealmUnsupportedPrivilegeQuery() *postgresRealmQuery {
	return &postgresRealmQuery{
		version:    "YugabyteDB 2026.1",
		database:   "ptah_test",
		metadata:   [][]driver.Value{{"yugabyte", false, `COMMENT ON SCHEMA "public" IS NULL`}},
		privileges: [][]driver.Value{{"yugabyte", "TEMPORARY", false}},
	}
}

func (q *postgresRealmQuery) query(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "current_database()"):
		return postgresCurrentDatabaseResult(q.database), nil
	case strings.Contains(query, "SELECT version()"):
		return postgresVersionResult(q.version), nil
	case strings.Contains(query, "obj_description"):
		return dbtest.QueryResult{
			Columns: []string{"owner", "acl_is_default", "comment_statement"},
			Rows:    q.metadata,
		}, nil
	case strings.Contains(query, "cleanup_objects"):
		return dbtest.QueryResult{
			Columns: []string{"object_kind", "object_schema", "object_name", "drop_statement"},
			Rows:    q.publicObjects,
		}, nil
	case strings.Contains(query, "aclexplode"):
		return dbtest.QueryResult{
			Columns: []string{"grantee", "privilege_type", "is_grantable"},
			Rows:    q.privileges,
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
	if strings.HasPrefix(query, "DROP FUNCTION") {
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
	query string,
	version string,
	objects [][]driver.Value,
) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "SELECT version()"):
		return postgresVersionResult(version), nil
	case strings.Contains(query, "SELECT COUNT(*)") && strings.Contains(query, "FROM pg_extension"):
		return noPostgresSchemaOwnedExtensions(), nil
	case strings.Contains(query, "FROM pg_inherits"):
		return noPostgresCrossSchemaPartitionEdges(), nil
	default:
		return dbtest.QueryResult{
			Columns: []string{"object_kind", "object_schema", "object_name", "drop_statement"},
			Rows:    objects,
		}, nil
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
