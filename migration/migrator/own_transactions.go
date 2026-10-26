package migrator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"ptah.run/dbschema"
	"ptah.run/internal/filetx"
	"ptah.run/internal/migratorext/builtin"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/migrationfile"
)

// WithOwnTransactions loads a directory written for a runner that opens no
// transaction around a file and sends each file as it is, as golang-migrate
// does. Such a file decides its own transactions, and every loaded migration
// is applied, and replayed on a dev database, the way that runner runs it, in
// place of the per-file transaction:
//
//   - A file that opens and closes its own transactions runs on one session,
//     outside any transaction of the migrator's, in the transactions
//     PostgreSQL runs it in when it is sent as one query, as golang-migrate
//     sends it by default; see [filetx.Blocks.AsOneQuery]. The statements are
//     still sent one at a time. Under tx-mode none, or with a directive asking
//     for no transaction, each statement outside the file's transactions
//     commits on its own instead, as golang-migrate's x-multi-statement mode
//     runs it. Progress is recorded where a transaction commits, and the
//     revision after the file's last statement. A file that fails is rolled
//     back on that session, and the failure records the statements that
//     stayed committed; a retry resumes after them. Which files qualify, and
//     which forms of transaction control are refused before anything runs, is
//     the engine's reading; only PostgreSQL offers one.
//   - A file of one statement runs without a transaction, as if it carried the
//     no-transaction directive: on PostgreSQL any such file, as golang-migrate
//     sends it on its own, and elsewhere one whose statement the engine
//     refuses inside a transaction, such as CREATE INDEX CONCURRENTLY. A file
//     that names a transaction mode keeps it.
//
// Every other file keeps the per-file transaction. A file that opens its own
// transaction cannot run with tx-mode all, and one that also asks for a
// transaction around the file is refused; either refusal comes before any
// migration runs. Without this option the migrator wraps each file, which is
// the contract of Ptah's and Atlas's layouts, where a file opts out with a
// directive.
func WithOwnTransactions() FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.ownTransactions = true
	}
}

// upOwnTransactions reads the up body for the transactions it opens and closes
// itself, when its directory runs such bodies as written, with the statements
// the reading indexes. The engine's services offer the reading; an engine that
// offers none reads no body as one with transactions of its own.
func (m *Migration) upOwnTransactions(conn *dbschema.DatabaseConnection) (filetx.Blocks, []string, error) {
	if !m.ownTransactions || m.upSQLFunc == nil {
		return filetx.Blocks{}, nil, nil
	}
	read := builtin.Lookup(databaseConnectionDialect(conn)).OwnTransactions
	if read == nil {
		return filetx.Blocks{}, nil, nil
	}
	statements := splitSQLStatementsForConnection(conn, m.UpSQL)
	blocks, err := read(statements)
	if err == nil {
		return blocks, statements, nil
	}
	failure := &MigrationExecutionError{
		Err:            fmt.Errorf("migration %d: %w", m.Version, err),
		StatementIndex: 1,
		Total:          len(statements),
	}
	if structure, ok := errors.AsType[*filetx.StructureError](err); ok && structure.Index < len(statements) {
		failure.Statement = strings.TrimSpace(statements[structure.Index])
		failure.StatementIndex = structure.Index + 1
	}
	return filetx.Blocks{}, nil, failure
}

// upRunsAloneOutsideATransaction reports a body of a directory that runs such
// bodies as written, naming no transaction mode, that holds one statement
// ([filetx.RunsAlone]), the predicate lint and the writer of new files ask too.
//
// golang-migrate sends such a file on its own, and PostgreSQL runs a statement
// sent on its own outside a transaction block, whatever the statement:
// measured with golang-migrate v4.20.1 on PostgreSQL 15.19 through both
// drivers, a file holding only `VACUUM base;`, or a DO block that commits
// between two statements, applies (stokaro/ptah#4979). The engine that offers
// the reading of a body's own transactions is the one this was measured on.
// On any other engine only a statement it refuses inside a transaction leaves
// the per-file transaction ([txrequire.AloneOutsideATransaction]).
func (m *Migration) upRunsAloneOutsideATransaction(conn *dbschema.DatabaseConnection) bool {
	if !m.ownTransactions || m.upSQLFunc == nil {
		return false
	}
	dialect := databaseConnectionDialect(conn)
	if m.parsedUpTxModeForDialect(dialect).Mode != migrationfile.FileTxModeUnspecified {
		return false
	}
	statements := splitSQLStatementsForConnection(conn, m.UpSQL)
	if !filetx.RunsAlone(statements) {
		return false
	}
	if builtin.Lookup(dialect).OwnTransactions != nil {
		return true
	}
	info := conn.Info()
	return txrequire.AloneOutsideATransaction(info.Dialect, info.Capabilities, statements)
}

// validateUpOwnTransactions refuses, before any migration runs, a body whose
// transaction control the migrator cannot follow, and one that opens its own
// transaction where the run would wrap it in one anyway: under tx-mode all, or
// with a directive asking for a transaction around the file.
func (m *Migrator) validateUpOwnTransactions(migrations []*Migration) error {
	for _, migration := range migrations {
		blocks, _, err := migration.upOwnTransactions(m.conn)
		if err != nil {
			return err
		}
		if blocks.Empty() {
			continue
		}
		if m.txMode == MigrationTxModeAll {
			return fmt.Errorf(
				"migration %d opens and closes its own transaction, which tx-mode all would nest in the one transaction "+
					"it runs every migration in; apply the directory with tx-mode file", migration.Version)
		}
		txMode := migration.parsedUpTxModeForDialect(m.connectionDialect())
		if txMode.Source != 0 && txMode.Mode == migrationfile.FileTxModeFile {
			return fmt.Errorf("migration %d asks for a transaction around the file and opens its own", migration.Version)
		}
	}
	return nil
}

// ownTransactionRun is how an up migration of a directory that runs bodies
// the way a runner that wraps none does runs.
type ownTransactionRun struct {
	// blocks are the transactions the body opens itself, empty for a body
	// that runs in mode, and statements the statements they index.
	blocks     filetx.Blocks
	statements []string
	// transactions are the transactions the body's statements commit in: the
	// body sent as one query ([filetx.Blocks.AsOneQuery]) when oneQuery is
	// set, and blocks otherwise.
	transactions filetx.Blocks
	oneQuery     bool
	// mode is the transaction mode the body runs in.
	mode MigrationTxMode
}

// upOwnTransactionMode decides how migration runs when its directory runs
// bodies the way a runner that wraps none does: as written, with the
// transactions it opens itself, or in a mode. A migration of another
// directory, and every migration under tx-mode all, keep mode.
func (m *Migrator) upOwnTransactionMode(migration *Migration, mode MigrationTxMode) (ownTransactionRun, error) {
	kept := ownTransactionRun{mode: mode}
	if !migration.ownTransactions || mode == MigrationTxModeAll {
		return kept, nil
	}
	blocks, statements, err := migration.upOwnTransactions(m.conn)
	if err != nil {
		return kept, err
	}
	if !blocks.Empty() {
		run := ownTransactionRun{blocks: blocks, statements: statements, transactions: blocks, mode: MigrationTxModeNone}
		if mode == MigrationTxModeFile {
			run.transactions = blocks.AsOneQuery(len(statements))
			run.oneQuery = true
		}
		return run, nil
	}
	if mode == MigrationTxModeFile && migration.upRunsAloneOutsideATransaction(m.conn) {
		return ownTransactionRun{mode: MigrationTxModeNone}, nil
	}
	return kept, nil
}

// applyUpMigrationOwnTransactions runs a body that opens and closes its own
// transactions, on one session the run pins, outside any transaction of the
// migrator's; see [WithOwnTransactions].
//
// It is the no-transaction path with three differences. The body's
// transaction control is not refused, because it is the body's own. Progress
// is recorded where the body commits: the statements of one transaction are
// recorded together, by a mark before the statement that closes it and a
// checkpoint after, and a statement that commits on its own is marked and
// checkpointed as it runs. And a resumed attempt reads a committed transaction
// of the prefix as such; see [Migrator.restoreSessionPrefix].
func (m *Migrator) applyUpMigrationOwnTransactions(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	plan upRetryPlan,
	run ownTransactionRun,
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
		if err := scoped.restoreSessionPrefix(ctx, migration, MigrationDirectionUp, plan.resumeFrom, run); err != nil {
			failure := fmt.Errorf("failed to restore session state for migration %d: %w", migration.Version, err)
			return scoped.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
		}
		if err := scoped.recordPendingMigrationRevisionOn(ctx, scoped.conn, migration, startedAt, plan); err != nil {
			failure := fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
			return scoped.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
		}
		if plan.resumeFrom > 1 {
			scoped.logger.Info(
				"Resuming migration after a partially applied attempt",
				"version", migration.Version,
				"resumeFromStatement", plan.resumeFrom,
			)
		}
		executionCtx := withMigrationResume(boundedCtx, plan.resumeFrom)
		return scoped.applyUpMigrationOwnTransactionsOnSession(executionCtx, migration, startedAt, restoreTimeouts, run)
	})
}

func (m *Migrator) applyUpMigrationOwnTransactionsOnSession(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	restoreTimeouts restoreTimeoutsFunc,
	run ownTransactionRun,
) error {
	executionConn := m.noTransactionConnection()
	fail := func(err error, prefix string) error {
		return m.failMigrationWithDirtyStateWithMode(
			ctx, migration, startedAt, err, migration.UpSQL, prefix, MigrationTxModeNone, MigrationDirectionUp)
	}
	session := &ownTransactionSession{conn: executionConn, transactions: run.transactions, oneQuery: run.oneQuery}
	executionCtx := m.withOwnTransactionProgress(ctx, migration, startedAt, run, session)
	executionCtx = session.withBeginCommitter(executionCtx)
	executionCtx = m.withPostgresIndexObservation(executionCtx, executionConn)
	err := migration.executeUp(executionCtx, executionConn, migrationExecutionNoTransaction)
	if err == nil {
		err = m.commitOneQueryEnd(executionCtx, migration, startedAt, session, run.statements)
	}
	if err != nil {
		failure := m.ownTransactionFailure(ctx, migration.Version, err, run.transactions)
		failure = fail(failure, fmt.Sprintf("failed to apply migration %d", migration.Version))
		return m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return fail(&committedProgressFailure{err: err, applied: len(run.statements)}, "")
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
// runs its own transactions, the transactions run.transactions names. A
// statement outside them is recorded as the no-transaction path records it.
// A statement inside one records nothing: its transaction has not committed,
// and a failure takes it back. The statement that closes one is marked in
// flight for the whole transaction, naming the transaction's first statement,
// and checkpointed once it commits, so an interrupted run's mark says the
// transaction from that statement on may or may not have committed. session,
// when the body runs as one query, opens the transaction a statement joins
// before the statement runs.
func (m *Migrator) withOwnTransactionProgress(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	run ownTransactionRun,
	session *ownTransactionSession,
) context.Context {
	return withStatementProgressRecorder(
		ctx,
		func(ctx context.Context, event StatementEvent) error {
			if err := session.before(ctx, event); err != nil {
				return err
			}
			index := event.Index - 1
			if run.transactions.Inside(index) {
				return nil
			}
			if opened, ok := run.transactions.Opening(index); ok {
				event.Index = opened + 1
				event.Statement = strings.TrimSpace(run.statements[opened])
			}
			return m.markMigrationStatementInFlight(ctx, migration, startedAt, event, MigrationDirectionUp)
		},
		func(ctx context.Context, event StatementEvent) error {
			session.after(ctx, event)
			if run.transactions.Inside(event.Index - 1) {
				return nil
			}
			return m.checkpointMigrationRevision(ctx, migration, startedAt, event, MigrationDirectionUp)
		},
	)
}

// commitOneQueryEnd commits, at the end of a body that runs as one query, the
// transaction the statements after its last COMMIT ran in, the way the end of
// the query commits it. It records the commit as the statement that closes one
// of the body's transactions is recorded: a mark naming the transaction's
// first statement before, and a checkpoint after.
func (m *Migrator) commitOneQueryEnd(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	session *ownTransactionSession,
	statements []string,
) error {
	if !session.open {
		return nil
	}
	total := len(statements)
	start, _ := session.transactions.Opening(total)
	first := strings.TrimSpace(statements[start])
	failed := func(phase string, err error) error {
		return &endOfQueryCommitError{phase: phase, total: total, err: &MigrationExecutionError{
			Err:            fmt.Errorf("failed to commit statements %d to %d at the end of the file: %w", start+1, total, err),
			Statement:      first,
			StatementIndex: start + 1,
			Total:          total,
		}}
	}
	mark := StatementEvent{Statement: first, Index: start + 1, Total: total}
	if err := m.markMigrationStatementInFlight(ctx, migration, startedAt, mark, MigrationDirectionUp); err != nil {
		return failed("before", err)
	}
	if err := session.finish(ctx); err != nil {
		return failed("commit", markedStatementFailure(ctx, err))
	}
	checkpoint := StatementEvent{Statement: first, Index: total, Total: total}
	if err := m.checkpointMigrationRevision(ctx, migration, startedAt, checkpoint, MigrationDirectionUp); err != nil {
		return failed("after", err)
	}
	return nil
}

// endOfQueryCommitError is a failure of the commit that ends a body run as
// one query, in phase: the mark before it ("before"), the COMMIT ("commit"),
// or the checkpoint after it ("after"). It wraps the error that names the
// first statement of the transaction it would have committed.
type endOfQueryCommitError struct {
	phase string
	total int
	err   error
}

func (e *endOfQueryCommitError) Error() string { return e.err.Error() }

func (e *endOfQueryCommitError) Unwrap() error { return e.err }

// unknownOutcomeError refuses to resume a migration whose interrupted run left
// the outcome of statement, a 1-based index, unknown.
type unknownOutcomeError struct {
	version   int64
	statement int
}

func (e *unknownOutcomeError) Error() string {
	return fmt.Sprintf(
		"migration %d cannot resume automatically: the outcome of statement %d is unknown after an interrupted run; "+
			"inspect the database, then use 'ptah migrations repair --version %d'",
		e.version, e.statement, e.version)
}

// describeUnknownOutcome rewrites a refusal to resume after an unknown outcome
// for a migration of a directory written for a runner that wraps nothing; see
// [WithOwnTransactions]. When the recorded statement is the first of one of
// the body's transactions, the mark before the transaction's commit named it,
// and the step whose outcome is unknown is the commit, which decides every
// statement of the transaction together: an operator who repaired to the
// statement the refusal named would run the rest of the transaction twice.
// The advice names no command, because the directory's runner has its own.
func (r ownTransactionRun) describeUnknownOutcome(err error, migration *Migration, total int) error {
	unknown, ok := errors.AsType[*unknownOutcomeError](err)
	if !ok || !migration.ownTransactions || unknown.statement < 1 {
		return err
	}
	rest := func(from int) string {
		if from > total {
			return "nothing is left to run"
		}
		return fmt.Sprintf("run statements %d to %d by hand", from, total)
	}
	first := unknown.statement - 1
	closing, inside := r.transactions.Closing(first)
	if opened, _ := r.transactions.Opening(first); !inside || opened != first {
		return fmt.Errorf(
			"migration %d cannot resume automatically: the outcome of statement %d is unknown after an interrupted "+
				"run; inspect the database: if it took effect, %s; if it did not, %s; then record the migration applied",
			unknown.version, unknown.statement, rest(unknown.statement+1), rest(unknown.statement))
	}
	commit := fmt.Sprintf("the COMMIT at statement %d", closing+1)
	if closing >= total {
		commit = "the commit at the end of the file"
	}
	last := min(closing+1, total)
	return fmt.Errorf(
		"migration %d cannot resume automatically: statements %d to %d commit together, and whether %s took effect "+
			"is unknown after an interrupted run; inspect the database: if they committed, %s; if they did not, %s; "+
			"then record the migration applied",
		unknown.version, first+1, last, commit, rest(last+1), rest(first+1))
}

// committedProgressFailure is a failure with how many of the body's
// statements are known to stay committed. For a body that runs its own
// transactions they are the statements before the one that stopped it, less
// those of the transaction it took back. For a body that committed every
// statement before a later step failed, such as putting the session's
// timeouts back, they are all of them.
type committedProgressFailure struct {
	err     error
	applied int
}

func (e *committedProgressFailure) Error() string { return e.err.Error() }

func (e *committedProgressFailure) Unwrap() error { return e.err }

// ownTransactionFailure ends whatever transaction the failed body may have
// left open on its session, sending nothing when it left none, and returns the
// failure with how many statements stayed committed. blocks are the
// transactions the body's statements commit in. A failure that left nothing
// committed, and whose session is known to hold no transaction any more, is
// reported as rolled back, as a failed body of the per-file transaction is.
func (m *Migrator) ownTransactionFailure(ctx context.Context, version int64, err error, blocks filetx.Blocks) error {
	stopped, open := ownTransactionStop(err, blocks)
	var rollbackErr error
	if open {
		rollbackErr = m.rollBackOwnTransaction(ctx, version)
	}
	if stopped < 0 {
		return errors.Join(err, rollbackErr)
	}
	applied := ownTransactionApplied(err, blocks, stopped)
	failure := &committedProgressFailure{err: err, applied: applied}
	if rollbackErr != nil {
		return errors.Join(failure, rollbackErr)
	}
	if applied > 0 || migrationAppliedFloor(ctx) > 0 || ctx.Err() != nil || statementOutcomeUnknown(err) {
		return failure
	}
	return &migrationTransactionRolledBackError{version: version, cause: failure}
}

// ownTransactionStop returns the zero-based index of the statement a failed
// body stopped at, -1 when the failure names none, and whether the session may
// still hold a transaction the body opened.
//
// A statement that failed inside a transaction leaves it aborted. The mark
// written before a closing statement failing leaves the transaction open,
// because the closing statement was never sent. A closing statement that
// failed ended its transaction, and one outside every transaction opened
// none. The commit at the end of a body run as one query stops it past its
// last statement, with the transaction open only when the mark before the
// commit failed. A failure that names no statement may have left one open.
func ownTransactionStop(err error, blocks filetx.Blocks) (int, bool) {
	if end, ok := errors.AsType[*endOfQueryCommitError](err); ok {
		return end.total, end.phase == "before"
	}
	if progress, ok := errors.AsType[*statementProgressError](err); ok && progress.event.Index > 0 {
		index := progress.event.Index - 1
		return index, progress.phase == "before" && blocks.Closes(index)
	}
	if observed, ok := errors.AsType[*StatementObservationError](err); ok && observed.Event.Index > 0 {
		index := observed.Event.Index - 1
		return index, blocks.Inside(index)
	}
	if execErr, ok := errors.AsType[*MigrationExecutionError](err); ok && execErr.StatementIndex > 0 {
		index := execErr.StatementIndex - 1
		return index, blocks.Inside(index)
	}
	return -1, true
}

// ownTransactionApplied is how many statements stay committed after a body
// stopped at the statement at index. A statement that did not run, because
// it failed or because the mark before it could not be written, keeps the
// statements [filetx.Blocks.Committed] names. A statement that ran, before its
// checkpoint or its observer failed, is committed too, unless it ran inside
// one of the body's transactions, which the rollback took back.
func ownTransactionApplied(err error, blocks filetx.Blocks, index int) int {
	ran := false
	if end, ok := errors.AsType[*endOfQueryCommitError](err); ok {
		if end.phase == "after" {
			return end.total
		}
		return blocks.Committed(index)
	}
	if progress, ok := errors.AsType[*statementProgressError](err); ok {
		ran = progress.phase == "after"
	} else if _, ok := errors.AsType[*StatementObservationError](err); ok {
		ran = true
	}
	if !ran || blocks.Inside(index) {
		return blocks.Committed(index)
	}
	return index + 1
}

// statementOutcomeUnknown reports a failure whose statement may have taken
// effect, the connection's answer rather than the server's.
func statementOutcomeUnknown(err error) bool {
	marked, ok := errors.AsType[*markedStatementError](err)
	return ok && marked.outcomeUnknown
}

// rollBackOwnTransaction ends the transaction a failed body may have left open
// on its session, before the failure is recorded. The session is pinned for
// the rest of the migration, and a transaction left aborted refuses every
// statement sent after it, the restored timeouts among them. It runs on a
// context the failure did not cancel. The caller sends it only when a
// transaction may be open, and a session the failure closed needs nothing:
// the server ends a session's transaction with the session.
func (m *Migrator) rollBackOwnTransaction(ctx context.Context, version int64) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	rollbackCtx, release := context.WithTimeout(context.WithoutCancel(ctx), ownTransactionRollbackTimeout)
	defer release()
	err := executeSQLOn(rollbackCtx, m.noTransactionConnection(), "ROLLBACK")
	if err == nil || errors.Is(err, sql.ErrConnDone) || errors.Is(err, driver.ErrBadConn) {
		return nil
	}
	return fmt.Errorf("failed to roll back migration %d: %w", version, err)
}
