package migrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"ptah.run/internal/filetx"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/migrationfile"
)

// WithOwnTransactions returns a copy of the migrator for a directory written
// for a runner that opens no transaction around a file and sends each file as
// it is, as golang-migrate does. Such a file decides its own transactions, and
// the migrator applies it the way that runner does, in place of wrapping it in
// a transaction of its own:
//
//   - A file that opens and closes its own transactions runs as written, on
//     one session, outside any transaction of the migrator's. The migrator
//     records its progress where the file's own transactions commit, and its
//     revision after its last statement. A file that fails is rolled back on
//     that session, and the failure is recorded with the statements the file
//     committed before it. Which files qualify, and which forms of
//     transaction control are refused before anything runs, is the reading
//     the dev database replay uses; it reads PostgreSQL alone.
//   - A file whose one statement the engine refuses inside a transaction, such
//     as CREATE INDEX CONCURRENTLY, runs without one, as if it carried the
//     no-transaction directive. A file that names a transaction mode keeps it.
//
// Every other file keeps the per-file transaction. A file that opens its own
// transaction cannot run with tx-mode all, and one that also asks for a
// transaction around the file is refused; either refusal comes before any
// migration runs. Off by default: in Ptah's and Atlas's layouts the migrator
// wraps each file, and a file opts out with a directive.
func (m *Migrator) WithOwnTransactions(enabled bool) *Migrator {
	tmp := *m
	tmp.ownTransactions = enabled
	return &tmp
}

// upOwnTransactions reads the up body of migration for the transactions it
// opens and closes itself, when the migrator applies such bodies as written.
func (m *Migrator) upOwnTransactions(migration *Migration) (filetx.Blocks, error) {
	if !m.ownTransactions || migration.upSQLFunc == nil {
		return filetx.Blocks{}, nil
	}
	read := m.services().OwnTransactions
	if read == nil {
		return filetx.Blocks{}, nil
	}
	statements := splitSQLStatementsForConnection(m.conn, migration.UpSQL)
	blocks, err := read(statements)
	if err == nil {
		return blocks, nil
	}
	failure := &MigrationExecutionError{
		Err:            fmt.Errorf("migration %d: %w", migration.Version, err),
		StatementIndex: 1,
		Total:          len(statements),
	}
	if structure, ok := errors.AsType[*filetx.StructureError](err); ok && structure.Index < len(statements) {
		failure.Statement = strings.TrimSpace(statements[structure.Index])
		failure.StatementIndex = structure.Index + 1
	}
	return filetx.Blocks{}, failure
}

// validateUpOwnTransactions refuses, before any migration runs, a body whose
// transaction control the migrator cannot follow, and one that opens its own
// transaction where the run would wrap it in one anyway: under tx-mode all, or
// with a directive asking for a transaction around the file.
func (m *Migrator) validateUpOwnTransactions(migrations []*Migration) error {
	if !m.ownTransactions {
		return nil
	}
	for _, migration := range migrations {
		blocks, err := m.upOwnTransactions(migration)
		if err != nil {
			return err
		}
		if blocks.Empty() {
			continue
		}
		if m.txMode == MigrationTxModeAll {
			return fmt.Errorf(
				"migration %d opens and closes its own transaction and cannot run with tx-mode all: %s",
				migration.Version, txModeAllExclusionAdvice)
		}
		txMode := migration.parsedUpTxModeForDialect(m.connectionDialect())
		if txMode.Source != 0 && txMode.Mode == migrationfile.FileTxModeFile {
			return fmt.Errorf("migration %d asks for a transaction around the file and opens its own", migration.Version)
		}
	}
	return nil
}

// upOwnTransactionMode decides how migration runs when the migrator applies
// bodies the way a runner that wraps none does. It returns the transactions
// the body opens itself, when it runs as written, and the mode it runs in
// otherwise. A migrator without that policy, and a body whose mode is not the
// per-file transaction, keep mode.
func (m *Migrator) upOwnTransactionMode(migration *Migration, mode MigrationTxMode) (filetx.Blocks, MigrationTxMode, error) {
	if !m.ownTransactions || mode != MigrationTxModeFile {
		return filetx.Blocks{}, mode, nil
	}
	blocks, err := m.upOwnTransactions(migration)
	if err != nil {
		return filetx.Blocks{}, mode, err
	}
	if !blocks.Empty() {
		return blocks, MigrationTxModeNone, nil
	}
	if m.runsAloneOutsideATransaction(migration) {
		return filetx.Blocks{}, MigrationTxModeNone, nil
	}
	return filetx.Blocks{}, mode, nil
}

// runsAloneOutsideATransaction reports a body that names no transaction mode
// and holds one statement the engine refuses inside a transaction. A runner
// that sends the file as it is sends that statement on its own, and the engine
// runs a statement sent alone outside a transaction.
func (m *Migrator) runsAloneOutsideATransaction(migration *Migration) bool {
	if migration.parsedUpTxModeForDialect(m.connectionDialect()).Source != 0 {
		return false
	}
	var statements []string
	for _, statement := range splitSQLStatementsForConnection(m.conn, migration.UpSQL) {
		if strings.TrimSpace(statement) != "" {
			statements = append(statements, statement)
		}
	}
	if len(statements) != 1 {
		return false
	}
	info := m.conn.Info()
	return txrequire.Analyze(info.Dialect, info.Capabilities, preflightStatements(migration.UpSQL, statements)).RequiresAutocommit()
}

// applyUpMigrationOwnTransactions runs a body that opens and closes its own
// transactions as written, on one session the run pins, outside any
// transaction of the migrator's; see [Migrator.WithOwnTransactions].
//
// It is the no-transaction path with two differences. The body's transaction
// control is not refused, because it is the body's own. And progress is
// recorded where the body commits: a statement outside the body's
// transactions is marked and checkpointed as it runs, while the statements of
// one of its transactions are recorded together, by a mark before the
// statement that closes it and a checkpoint after.
func (m *Migrator) applyUpMigrationOwnTransactions(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	plan upRetryPlan,
	blocks filetx.Blocks,
) error {
	timeouts, err := m.effectiveUpTimeouts(migration)
	if err != nil {
		return err
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		boundedCtx, restoreTimeouts, err := scoped.applySessionTimeouts(ctx, migration, timeouts)
		if err != nil {
			return err
		}
		if err := scoped.restoreNoTransactionSessionPrefix(ctx, migration, MigrationDirectionUp, plan.resumeFrom); err != nil {
			failure := fmt.Errorf("failed to restore session state for migration %d: %w", migration.Version, err)
			return scoped.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
		}
		if err := scoped.recordPendingMigrationRevisionOn(ctx, scoped.conn, migration, startedAt, plan); err != nil {
			failure := fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
			return scoped.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
		}
		executionCtx := withMigrationResume(boundedCtx, plan.resumeFrom)
		return scoped.applyUpMigrationOwnTransactionsOnSession(executionCtx, migration, startedAt, restoreTimeouts, blocks)
	})
}

func (m *Migrator) applyUpMigrationOwnTransactionsOnSession(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	restoreTimeouts restoreTimeoutsFunc,
	blocks filetx.Blocks,
) error {
	executionConn := m.noTransactionConnection()
	fail := func(err error, prefix string) error {
		return m.failMigrationWithDirtyStateWithMode(
			ctx, migration, startedAt, err, migration.UpSQL, prefix, MigrationTxModeNone, MigrationDirectionUp)
	}
	executionCtx := m.withOwnTransactionProgress(ctx, migration, startedAt, blocks)
	executionCtx = m.withPostgresIndexObservation(executionCtx, executionConn)
	if err := migration.executeUp(executionCtx, executionConn, migrationExecutionNoTransaction); err != nil {
		err = m.rollBackOwnTransaction(ctx, migration.Version, err)
		failure := fail(ownTransactionFailureOf(err, blocks), fmt.Sprintf("failed to apply migration %d", migration.Version))
		return m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return fail(err, "")
	}
	if err := m.refuseUpCompletionOverUnsafeIndex(ctx, executionConn, migration); err != nil {
		return fail(
			completedMigrationIndexObservationError(migration, m.connectionDialect(), MigrationDirectionUp, err),
			fmt.Sprintf("failed to verify migration %d", migration.Version),
		)
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	if err := m.completeMigrationRevision(recordCtx, migration, startedAt); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
	}
	m.logger.Info("Applied migration that runs its own transactions", "version", migration.Version, "description", migration.Description)
	return nil
}

// withOwnTransactionProgress installs the progress recorders of a body that
// runs its own transactions. A statement outside them is recorded as the
// no-transaction path records it. A statement inside one records nothing:
// its transaction has not committed, and a failure takes it back. The
// statement that closes one is marked in flight for the whole transaction,
// from the statement that opened it, and checkpointed once it commits.
func (m *Migrator) withOwnTransactionProgress(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	blocks filetx.Blocks,
) context.Context {
	return withStatementProgressRecorder(
		ctx,
		func(ctx context.Context, event StatementEvent) error {
			index := event.Index - 1
			if blocks.Inside(index) {
				return nil
			}
			if opened, ok := blocks.Opening(index); ok {
				event.Index = opened + 1
			}
			return m.markMigrationStatementInFlight(ctx, migration, startedAt, event, MigrationDirectionUp)
		},
		func(ctx context.Context, event StatementEvent) error {
			if blocks.Inside(event.Index - 1) {
				return nil
			}
			return m.checkpointMigrationRevision(ctx, migration, startedAt, event, MigrationDirectionUp)
		},
	)
}

// rollBackOwnTransaction ends whatever transaction a failed body left open on
// its session before the failure is recorded. The session is pinned for the
// rest of the migration, and a transaction left aborted refuses every
// statement sent after it. A ROLLBACK where none is open is a warning, not an
// error. It runs on a context the failure did not cancel.
func (m *Migrator) rollBackOwnTransaction(ctx context.Context, version int64, failure error) error {
	if m.conn.Writer().IsDryRun() {
		return failure
	}
	rollbackCtx, release := context.WithTimeout(context.WithoutCancel(ctx), ownTransactionRollbackTimeout)
	defer release()
	if err := executeSQLOn(rollbackCtx, m.noTransactionConnection(), "ROLLBACK"); err != nil {
		return errors.Join(failure, fmt.Errorf("failed to roll back migration %d: %w", version, err))
	}
	return failure
}

// ownTransactionFailure is the failure of a statement of a body that runs its
// own transactions, with how many of the body's statements stay committed:
// the statements before it, less those of the transaction it took back.
type ownTransactionFailure struct {
	err     error
	applied int
}

func (e *ownTransactionFailure) Error() string { return e.err.Error() }

func (e *ownTransactionFailure) Unwrap() error { return e.err }

// ownTransactionFailureOf records how far a failed body that runs its own
// transactions got. A statement that failed keeps the statements
// [filetx.Blocks.Committed] names. A statement that ran and whose observer
// failed after it is committed too, unless it ran inside one of the body's
// transactions, which the rollback took back. A failure that names no
// statement, such as a progress checkpoint that could not be written, already
// says how far the body got, and is returned as it is.
func ownTransactionFailureOf(err error, blocks filetx.Blocks) error {
	if observed, ok := errors.AsType[*StatementObservationError](err); ok && observed.Event.Index > 0 {
		index := observed.Event.Index - 1
		applied := index + 1
		if opened, inside := blocks.Opening(index); inside && blocks.Inside(index) {
			applied = opened
		}
		return &ownTransactionFailure{err: err, applied: applied}
	}
	var execErr *MigrationExecutionError
	if !errors.As(err, &execErr) || execErr.StatementIndex <= 0 {
		return err
	}
	return &ownTransactionFailure{err: err, applied: blocks.Committed(execErr.StatementIndex - 1)}
}
