package dbschema_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mssql"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbschema/sqlite"
)

func TestSQLTransactionCancellationEvidenceUsesTheBeginContext(t *testing.T) {
	for _, test := range []struct {
		name   string
		writer func(*sql.DB) catalog.SchemaWriter
	}{
		{"sqlite", func(db *sql.DB) catalog.SchemaWriter { return sqlite.NewSQLiteWriter(db, "main") }},
		{"mysql", func(db *sql.DB) catalog.SchemaWriter { return mysql.NewMySQLWriter(db, "app", platform.MySQL) }},
		{"postgres", func(db *sql.DB) catalog.SchemaWriter { return postgres.NewPostgreSQLWriter(db, "public") }},
		{"sqlserver", func(db *sql.DB) catalog.SchemaWriter { return mssql.NewSQLServerWriter(db, "dbo") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, nil)
			writer := test.writer(db.SQL)
			ctx, cancel := context.WithCancel(c.Context())
			c.Cleanup(cancel)
			tx, err := writer.BeginTransaction(ctx)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = tx.Rollback() })
			evidence, ok := tx.(catalog.CancellationRollback)
			c.Assert(ok, qt.IsTrue)
			c.Assert(evidence.RolledBackByCancellation(), qt.IsFalse)
			statementCtx, cancelStatement := context.WithCancel(ctx)
			cancelStatement()
			c.Assert(tx.ExecuteSQL(statementCtx, "SELECT 1"), qt.ErrorIs, context.Canceled)
			c.Assert(evidence.RolledBackByCancellation(), qt.IsFalse)
			cancel()
			_ = tx.Rollback()
			c.Assert(evidence.RolledBackByCancellation(), qt.IsTrue)
			c.Assert(tx.Rollback(), qt.IsNil)
			c.Assert(evidence.RolledBackByCancellation(), qt.IsTrue)

			commitCtx, cancelCommitted := context.WithCancel(c.Context())
			c.Cleanup(cancelCommitted)
			committed, err := writer.BeginTransaction(commitCtx)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = committed.Rollback() })
			c.Assert(committed.Commit(), qt.IsNil)
			cancelCommitted()
			c.Assert(committed.Rollback(), qt.IsNil)
			committedEvidence, ok := committed.(catalog.CancellationRollback)
			c.Assert(ok, qt.IsTrue)
			c.Assert(committedEvidence.RolledBackByCancellation(), qt.IsFalse)
			c.Assert(db.CommitCount(), qt.Equals, 1)
		})
	}
}

func TestSQLiteCancellationEvidenceMatchesRolledBackSchema(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "canceled.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	ctx, cancel := context.WithCancel(c.Context())
	c.Cleanup(cancel)
	tx, err := conn.SchemaWriter().BeginTransaction(ctx)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	c.Assert(tx.ExecuteSQL(ctx, "CREATE TABLE canceled_schema (id INTEGER)"), qt.IsNil)
	cancel()
	_ = tx.Rollback()
	evidence, ok := tx.(catalog.CancellationRollback)
	c.Assert(ok, qt.IsTrue)
	c.Assert(evidence.RolledBackByCancellation(), qt.IsTrue)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM sqlite_master WHERE name = 'canceled_schema'").Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}
