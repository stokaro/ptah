//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	noTransactionSessionSchema       = "ptah_no_transaction_session"
	noTransactionSessionTable        = "session_target"
	noTransactionSessionRetryTable   = "session_retry_target"
	noTransactionSessionGate         = "session_retry_gate"
	noTransactionSessionTracker      = "schema_migrations_no_transaction_session"
	noTransactionSessionRetryTracker = "schema_migrations_no_transaction_session_retry"
)

func TestPostgreSQLNoTransactionPinsSessionIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openNoTransactionSessionDB(c.TB, dsn)
	resetNoTransactionSessionFixtures(c.TB, db)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })

	var held *sql.Conn
	actions := []func(context.Context) error{
		func(ctx context.Context) error {
			var connErr error
			held, connErr = conn.Conn(ctx)
			return connErr
		},
		func(context.Context) error { return nil },
	}
	observer := migrator.StatementObserverFunc(func(ctx context.Context, event migrator.StatementEvent) error {
		return actions[event.Index-1](ctx)
	})
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"000001_session.up.sql": {Data: fmt.Appendf(nil,
				"-- +ptah no_transaction\nSET search_path = %s;\nCREATE TABLE %s (id BIGINT PRIMARY KEY);",
				noTransactionSessionSchema,
				noTransactionSessionTable,
			)},
			"000001_session.down.sql": {Data: fmt.Appendf(nil,
				"DROP TABLE %s.%s;",
				noTransactionSessionSchema,
				noTransactionSessionTable,
			)},
		},
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	mig := migrator.NewMigrator(conn, provider).WithMigrationsTable("", noTransactionSessionTracker)

	migrationErr := mig.MigrateUp(c.Context())
	c.Assert(held, qt.IsNotNil)
	c.Assert(held.Close(), qt.IsNil)
	c.Assert(migrationErr, qt.IsNil)
	c.Assert(noTransactionSessionRelationCount(c.TB, db, noTransactionSessionSchema, noTransactionSessionTable), qt.Equals, 1)
	c.Assert(noTransactionSessionRelationCount(c.TB, db, "public", noTransactionSessionTable), qt.Equals, 0)
}

func TestPostgreSQLNoTransactionResumeRestoresSessionPrefixIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openNoTransactionSessionDB(c.TB, dsn)
	resetNoTransactionSessionFixtures(c.TB, db)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })

	var held *sql.Conn
	actions := []func(context.Context) error{
		func(ctx context.Context) error {
			var connErr error
			held, connErr = conn.Conn(ctx)
			return connErr
		},
		func(context.Context) error { return nil },
		func(context.Context) error { return nil },
	}
	observer := migrator.StatementObserverFunc(func(ctx context.Context, event migrator.StatementEvent) error {
		return actions[event.Index-1](ctx)
	})
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"000001_session_retry.up.sql": {Data: fmt.Appendf(nil,
				"-- +ptah no_transaction\nSET search_path = %s;\n"+
					"INSERT INTO %s (id) VALUES (1);\nCREATE TABLE %s (id BIGINT PRIMARY KEY);",
				noTransactionSessionSchema,
				noTransactionSessionGate,
				noTransactionSessionRetryTable,
			)},
			"000001_session_retry.down.sql": {Data: fmt.Appendf(nil,
				"DROP TABLE %s.%s;",
				noTransactionSessionSchema,
				noTransactionSessionRetryTable,
			)},
		},
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	mig := migrator.NewMigrator(conn, provider).WithMigrationsTable("", noTransactionSessionRetryTracker)

	c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)
	c.Assert(held, qt.IsNotNil)
	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE %s.%s (id BIGINT PRIMARY KEY)",
		noTransactionSessionSchema,
		noTransactionSessionGate,
	))
	c.Assert(err, qt.IsNil)
	c.Assert(
		mig.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}),
		qt.IsNil,
	)
	c.Assert(held.Close(), qt.IsNil)
	c.Assert(noTransactionSessionRelationCount(c.TB, db, noTransactionSessionSchema, noTransactionSessionRetryTable), qt.Equals, 1)
	c.Assert(noTransactionSessionRelationCount(c.TB, db, "public", noTransactionSessionRetryTable), qt.Equals, 0)

	var rows int
	err = db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s",
		noTransactionSessionSchema,
		noTransactionSessionGate,
	)).Scan(&rows)
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}

func openNoTransactionSessionDB(tb testing.TB, dsn string) *sql.DB {
	c := qt.New(tb)
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

func resetNoTransactionSessionFixtures(tb testing.TB, db *sql.DB) {
	c := qt.New(tb)
	c.Helper()
	reset := func() {
		_, err := db.Exec("DROP SCHEMA IF EXISTS " + noTransactionSessionSchema + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS public." + noTransactionSessionTable + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS public." + noTransactionSessionRetryTable + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS public." + noTransactionSessionTracker + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS public." + noTransactionSessionRetryTracker + " CASCADE")
		c.Check(err, qt.IsNil)
	}
	reset()
	c.Cleanup(reset)
	_, err := db.Exec("CREATE SCHEMA " + noTransactionSessionSchema)
	c.Assert(err, qt.IsNil)
}

func noTransactionSessionRelationCount(tb testing.TB, db *sql.DB, schema, name string) int {
	c := qt.New(tb)
	c.Helper()
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`,
		schema,
		name,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}
