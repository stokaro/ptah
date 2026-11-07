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
	body := m.bodyConnection()
	tx, err := sqliterebuild.BeginTransactionForSQL(ctx, body, sqlText)
	if err != nil {
		return fail(err, fmt.Sprintf("failed to begin transaction for migration %d", migration.Version))
	}
	txConn := body.WithExecutor(tx)
	rollback := func(err error, prefix string, stage bodyStage) error {
		evidence := m.rollbackEvidence(ctx, body, err, sqlText, stage)
		failure := migrationFailureAfterRollback(migration.Version, err, tx.Rollback(), tx)
		return fail(m.withRollbackSurvivors(ctx, failure, evidence), prefix)
	}
	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, run.timeouts, timeoutScopeTransaction)
	if err != nil {
		return rollback(err, fmt.Sprintf("failed to apply timeouts for migration %d", migration.Version), bodyNotStarted)
	}
	executionCtx := scoped.withTransactionalProgressRecorder(ctx, txConn, migration, startedAt, run.direction)
	executionCtx = scoped.withPostgresIndexObservation(executionCtx, txConn)
	executionCtx = withStatementBound(executionCtx, txConn, run.timeouts)
	if err := run.execute(executionCtx, txConn, migrationExecutionTransactional); err != nil {
		err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		return rollback(err, fmt.Sprintf("%s %d", run.bodyFailure, migration.Version), bodyStopped)
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return rollback(err, "", bodyFinished)
	}
	if err := run.verify(&scoped, ctx, txConn, migration); err != nil {
		return rollback(err, fmt.Sprintf("%s %d", run.verifyFailure, migration.Version), bodyFinished)
	}
	if err := run.record(ctx, txConn); err != nil {
		return rollback(err, fmt.Sprintf("%s %d", run.recordFailure, migration.Version), bodyFinished)
	}
	// Read ahead of the commit: a failed commit ends the transaction, and the
	// session then reads the same as one the server ended before a schema
	// statement.
	evidence := m.rollbackEvidence(ctx, body, nil, sqlText, bodyFinished)
	if err := m.commitMigrationTransaction(tx, sqlText); err != nil {
		failure := migrationFailureAfterCommit(migration.Version, err, tx)
		return fail(m.withRollbackSurvivors(ctx, failure, evidence), fmt.Sprintf("failed to commit transaction for migration %d", migration.Version))
	}
	m.logger.Info(run.success, "version", migration.Version, "description", migration.Description)
	return nil
}
