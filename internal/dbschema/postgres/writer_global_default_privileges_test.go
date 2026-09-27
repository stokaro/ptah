package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// TestWriterDropDatabaseRealm_ResetsGlobalDefaultPrivilegesHappyPath returns
// the global default privileges to the built-in ones after the schema-scoped
// revokes: a global default applies in every schema and belongs to the
// database, so the realm cleanup, which owns the database, is the cleanup that
// resets it (stokaro/ptah#3772). For each grantee the revoke runs before the
// grant back, or it would take back what the grant restored.
func TestWriterDropDatabaseRealm_ResetsGlobalDefaultPrivilegesHappyPath(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	queryHandler := newPostgresRealmMetadataQuery()
	queryHandler.globalDefaultACLs = [][]driver.Value{
		{"app_owner", "FUNCTIONS", `["app_owner=X/app_owner"]`, `["=X/app_owner","app_owner=X/app_owner"]`},
		{"app_owner", "SCHEMAS", `["app_owner=UC/app_owner","app_reader=U/app_owner"]`, `["app_owner=UC/app_owner"]`},
	}
	db := dbtest.OpenWithExec(t, queryHandler.query, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries[2:6], qt.DeepEquals, []string{
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC`),
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC`),
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" GRANT EXECUTE ON FUNCTIONS TO PUBLIC`),
		postgresCleanupSavepoint(`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE ALL PRIVILEGES ON SCHEMAS FROM "app_reader"`),
	})
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

// TestWriterDropDatabaseRealm_CockroachResetsGlobalDefaultPrivilegesHappyPath
// is the reset on CockroachDB, which names the global default privileges
// through SHOW DEFAULT PRIVILEGES without IN SCHEMA. What SHOW names beside
// the built-in default is reset; the built-in rows are left alone.
func TestWriterDropDatabaseRealm_CockroachResetsGlobalDefaultPrivilegesHappyPath(t *testing.T) {
	c := qt.New(t)
	var execQueries []string
	queryHandler := newPostgresRealmCockroachQuery()
	queryHandler.roles = [][]driver.Value{{"app_owner"}}
	queryHandler.globalShow = [][]driver.Value{
		{"app_owner", false, "routines", "public", "EXECUTE", false},
		{"app_owner", false, "routines", "app_owner", "ALL", true},
		{"app_owner", false, "schemas", "app_owner", "ALL", true},
		{"app_owner", false, "sequences", "app_owner", "ALL", true},
		{"app_owner", false, "tables", "app_owner", "ALL", true},
		{"app_owner", false, "tables", "app_reader", "SELECT", false},
		{"app_owner", false, "types", "app_owner", "ALL", true},
	}
	db := dbtest.OpenWithExec(t, queryHandler.query, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(execQueries, qt.DeepEquals, []string{
		`DROP TABLE IF EXISTS "public"."stale_items" RESTRICT`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE ALL PRIVILEGES ON TYPES FROM PUBLIC`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" GRANT USAGE ON TYPES TO PUBLIC`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE ALL PRIVILEGES ON TABLES FROM "app_reader"`,
		`DROP SCHEMA IF EXISTS "audit" RESTRICT`,
	})
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

// TestWriterDropAllTables_LeavesGlobalDefaultPrivilegesAlone holds the cleanup
// of one schema to its schema: a global default applies in every schema, so a
// cleanup that owns one of them does not read it, let alone reset it.
func TestWriterDropAllTables_LeavesGlobalDefaultPrivilegesAlone(t *testing.T) {
	c := qt.New(t)
	var catalogQueries []string
	var execQueries []string
	db := dbtest.OpenWithExec(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		catalogQueries = append(catalogQueries, query)
		return postgresGlobalDefaultACLCleanupQuery(query, args)
	}, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		execQueries = append(execQueries, query)
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(catalogQueries, qt.Not(qt.Any(qt.Contains)), postgresGlobalDefaultACLMarker)
	c.Assert(execQueries, qt.HasLen, 0)
}

// TestWriterResetObjects_ListsGlobalDefaultPrivilegesHappyPath lists the
// global default privileges a realm cleanup would reset, once per grantee and
// under no schema, so the claim on a dev database refuses one that holds them
// rather than resetting what somebody set.
func TestWriterResetObjects_ListsGlobalDefaultPrivilegesHappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, postgresGlobalDefaultACLCleanupQuery)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	objects, err := writer.ResetObjects(t.Context(), dbreset.Scope{Schemas: []string{"public"}})

	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []dbreset.Object{
		{Kind: "default privilege", Name: "app_owner/f/PUBLIC"},
		{Kind: "default privilege", Name: "app_owner/r/app_owner"},
	})
}

// postgresGlobalDefaultACLCleanupQuery plays a PostgreSQL 18 database whose
// only default privileges are global: app_owner took EXECUTE on functions away
// from PUBLIC and DELETE on tables from itself.
func postgresGlobalDefaultACLCleanupQuery(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, postgresGlobalDefaultACLMarker):
		return postgresGlobalDefaultACLResult([][]driver.Value{
			{"app_owner", "FUNCTIONS", `["app_owner=X/app_owner"]`, `["=X/app_owner","app_owner=X/app_owner"]`},
			{"app_owner", "TABLES", `["app_owner=arwDxtm/app_owner"]`, `["app_owner=arwdDxtm/app_owner"]`},
		}), nil
	case strings.Contains(query, "FROM pg_largeobject_metadata"):
		return dbtest.QueryResult{Columns: []string{"oid"}}, nil
	default:
		return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", nil)
	}
}
