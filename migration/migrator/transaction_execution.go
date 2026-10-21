package migrator

import (
	"context"
	"fmt"
	"time"

	"ptah.run/dbschema"
	"ptah.run/internal/sqliterebuild"
	"ptah.run/migration/migrationfile"
)

// transactionMigration supplies the direction-specific operations. Execution,
// timeout cleanup, verification, revision writes, and commit share one ordering
// so apply and rollback cannot diverge in how they preserve uncertain outcomes.
type transactionMigration struct {
	direction     MigrationDirection
	timeouts      migrationfile.Timeouts
	execute       func(context.Context, *dbschema.DatabaseConnection, migrationExecutionMode) error
	verify        func(*Migrator, context.Context, *dbschema.DatabaseConnection, *Migration) error
	record        func(context.Context, *dbschema.DatabaseConnection) error
	bodyFailure   string
	verifyFailure string
	recordFailure string
	success       string
}

func (m *Migrator) runTransactionalMigration(ctx context.Context, migration *Migration, startedAt time.Time, run transactionMigration) error {
	scoped := *m
	sqlText := migrationSQLForDirection(migration, run.direction)
	fail := func(err error, prefix string) error {
		return m.failMigrationWithDirtyStateWithMode(ctx, migration, startedAt, err, sqlText, prefix, MigrationTxModeFile, run.direction)
	}
	// Preconditions read committed state before this transaction starts. They
	// must not query the pool while a single-connection transaction owns it.
	tx, err := sqliterebuild.BeginTransactionForSQL(ctx, m.conn, sqlText)
	if err != nil {
		return fail(err, fmt.Sprintf("failed to begin transaction for migration %d", migration.Version))
	}
	txConn := m.conn.WithExecutor(tx)
	rollback := func(err error, prefix string) error {
		return fail(migrationFailureAfterRollback(migration.Version, err, tx.Rollback(), tx), prefix)
	}
	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, run.timeouts, timeoutScopeTransaction)
	if err != nil {
		return rollback(err, fmt.Sprintf("failed to apply timeouts for migration %d", migration.Version))
	}
	executionCtx := scoped.withTransactionalProgressRecorder(ctx, txConn, migration, startedAt, run.direction)
	executionCtx = scoped.withPostgresIndexObservation(executionCtx, txConn)
	if err := run.execute(executionCtx, txConn, migrationExecutionTransactional); err != nil {
		err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		return rollback(err, fmt.Sprintf("%s %d", run.bodyFailure, migration.Version))
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return rollback(err, "")
	}
	if err := run.verify(&scoped, ctx, txConn, migration); err != nil {
		return rollback(err, fmt.Sprintf("%s %d", run.verifyFailure, migration.Version))
	}
	if err := run.record(ctx, txConn); err != nil {
		return rollback(err, fmt.Sprintf("%s %d", run.recordFailure, migration.Version))
	}
	if err := commitMigrationTransaction(tx); err != nil {
		return fail(err, fmt.Sprintf("failed to commit transaction for migration %d", migration.Version))
	}
	m.logger.Info(run.success, "version", migration.Version, "description", migration.Description)
	return nil
}
