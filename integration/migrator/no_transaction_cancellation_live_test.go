//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestNoTransactionCancellationDuringExecPreservesUnknownOutcome(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	migrationsTable := fmt.Sprintf("schema_migrations_deadline_%d", suffix)
	itemsTable := fmt.Sprintf("ptah_deadline_items_%d", suffix)
	firstStatementDone := make(chan struct{})
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	observer := migrator.StatementObserverFunc(func(context.Context, migrator.StatementEvent) error {
		close(firstStatementDone)
		return nil
	})
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"000001_deadline.up.sql": {Data: fmt.Appendf(nil,
				"-- +ptah no_transaction\nCREATE TABLE %s (id BIGINT PRIMARY KEY); SELECT pg_sleep(10)",
				itemsTable,
			)},
			"000001_deadline.down.sql": {Data: fmt.Appendf(nil, "DROP TABLE %s", itemsTable)},
		},
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	m := migrator.NewMigrator(conn, provider).WithMigrationsTable("", migrationsTable)
	c.Assert(m.Initialize(ctx), qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+itemsTable)
		_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+migrationsTable)
	}()

	go func() {
		<-firstStatementDone
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	err = m.MigrateUp(execCtx)
	c.Assert(err, qt.IsNotNil)

	var state, failure string
	var applied, total int
	c.Assert(
		conn.QueryRowContext(
			ctx,
			"SELECT state, applied, total, error FROM "+migrationsTable+" WHERE version = 1",
		).Scan(&state, &applied, &total, &failure),
		qt.IsNil,
	)
	c.Assert(state, qt.Equals, "pending")
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 2)
	c.Assert(failure, qt.Equals, "statement execution outcome is unknown after process interruption")

	err = m.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.ErrorMatches, `migration 1 cannot resume automatically: the outcome of statement 2 is unknown.*`)
}
