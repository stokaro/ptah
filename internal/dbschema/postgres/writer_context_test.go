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
	db := dbtest.OpenWithExec(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		catalogQueries = append(catalogQueries, query)
		catalogArgs = append(catalogArgs, args)
		return postgresCleanupQuery(query, args)
	}, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(catalogQueries, qt.HasLen, 2)
	c.Assert(catalogQueries[0], qt.Contains, "SELECT COUNT(*)")
	c.Assert(catalogQueries[0], qt.Contains, "FROM pg_extension")
	c.Assert(catalogQueries[1], qt.Contains, "cleanup_objects")
	c.Assert(catalogQueries[1], qt.Contains, "FROM pg_constraint")
	c.Assert(catalogQueries[1], qt.Contains, "con.contype = 'f'")
	c.Assert(catalogQueries[1], qt.Contains, "p.prokind IN ('f', 'p', 'a', 'w')")
	c.Assert(catalogQueries[1], qt.Contains, "d.deptype = 'i'")
	c.Assert(catalogQueries[1], qt.Contains, "t.typtype IN ('e', 'd', 'r')")
	c.Assert(catalogQueries[1], qt.Contains, "RESTRICT")
	c.Assert(catalogQueries[1], qt.Not(qt.Contains), "CASCADE")
	c.Assert(catalogQueries[1], qt.Not(qt.Contains), "dependent_objects")
	c.Assert(catalogQueries[1], qt.Not(qt.Contains), "d.deptype = 'e'")
	c.Assert(catalogArgs, qt.DeepEquals, [][]driver.NamedValue{
		{{Ordinal: 1, Value: "public"}},
		{{Ordinal: 1, Value: "public"}},
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 2)
	c.Assert(db.ExecCount(), qt.Equals, 30)
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
	c.Assert(db.ExecCount(), qt.Equals, 35)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropAllTables_RejectsSchemaScopedExtensions(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, func(
		query string,
		_ []driver.NamedValue,
	) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{
			Columns: []string{"count", "first"},
			Rows: [][]driver.Value{{
				int64(1),
				"hstore",
			}},
		}, nil
	}, nil)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`refusing to clean schema "public": extension "hstore" is owned by it; schema-scoped cleanup cannot prove that every extension member is confined to the schema`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.QueryCount(), qt.Equals, 1)
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
	c.Assert(db.QueryCount(), qt.Equals, 2)
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
	c.Assert(db.QueryCount(), qt.Equals, 2)
	c.Assert(db.ExecCount(), qt.Equals, 10)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropAllTables_PreservesDropErrorWhenSavepointRecoveryFails(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, postgresPolicyCleanupQuery, failPostgresDropAndSavepointRollback)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(err.Error(), qt.Contains, "failed to roll back cleanup savepoint: rollback failed")
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.ExecCount(), qt.Equals, 4)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func emptyPostgresCleanupQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if strings.Contains(query, "FROM pg_extension") {
		return noPostgresSchemaOwnedExtensions(), nil
	}
	return dbtest.QueryResult{}, nil
}

func postgresCleanupQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if strings.Contains(query, "FROM pg_extension") {
		return noPostgresSchemaOwnedExtensions(), nil
	}
	return dbtest.QueryResult{
		Columns: []string{"object_kind", "object_name", "drop_statement"},
		Rows: [][]driver.Value{
			{"constraint", "users_parent_fkey", `ALTER TABLE "public"."users" DROP CONSTRAINT IF EXISTS "users_parent_fkey" RESTRICT`},
			{"view", "active_users", `DROP VIEW IF EXISTS "public"."active_users" RESTRICT`},
			{"materialized view", "user_stats", `DROP MATERIALIZED VIEW IF EXISTS "public"."user_stats" RESTRICT`},
			{"foreign table", "remote_users", `DROP FOREIGN TABLE IF EXISTS "public"."remote_users" RESTRICT`},
			{"table", "users", `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
			{"sequence", "users_id_seq", `DROP SEQUENCE IF EXISTS "public"."users_id_seq" RESTRICT`},
			{"procedure", "refresh_users", `DROP PROCEDURE IF EXISTS "public"."refresh_users"() RESTRICT`},
			{"aggregate", "sum_text", `DROP AGGREGATE IF EXISTS "public"."sum_text"(text) RESTRICT`},
			{"function", "normalize_email", `DROP FUNCTION IF EXISTS "public"."normalize_email"(text) RESTRICT`},
			{"type", "status", `DROP TYPE IF EXISTS "public"."status" RESTRICT`},
		},
	}, nil
}

func noPostgresSchemaOwnedExtensions() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"count", "first"},
		Rows:    [][]driver.Value{{int64(0), ""}},
	}
}

func postgresPolicyCleanupQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if strings.Contains(query, "FROM pg_extension") {
		return noPostgresSchemaOwnedExtensions(), nil
	}
	return dbtest.QueryResult{
		Columns: []string{"object_kind", "object_name", "drop_statement"},
		Rows: [][]driver.Value{{
			"function",
			"is_allowed",
			`DROP FUNCTION IF EXISTS "public"."is_allowed"() RESTRICT`,
		}},
	}, nil
}

func postgresInternalDependencyQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if strings.Contains(query, "FROM pg_extension") {
		return noPostgresSchemaOwnedExtensions(), nil
	}
	return dbtest.QueryResult{
		Columns: []string{"object_kind", "object_name", "drop_statement"},
		Rows: [][]driver.Value{
			{"table", "users", `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
			{"view", "active_users", `DROP VIEW IF EXISTS "public"."active_users" RESTRICT`},
		},
	}, nil
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
