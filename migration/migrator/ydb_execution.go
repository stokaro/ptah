package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasretry"
	"ptah.run/internal/yqlquery"
)

// How a migration body runs on YDB.
//
// YDB runs a scheme statement only outside a transaction and never in the same
// query as a data statement, and a named expression, an action, a DECLARE or a
// PRAGMA exists only in the query that holds it. So the migrator runs a YDB
// body as the queries internal/yqlquery splits it into -- each scheme
// statement a query of its own, each run of data statements one query -- and
// those queries are the units it counts, records progress over and resumes
// from, in place of statements.
//
// A scheme query, and a BATCH query, which YDB runs only outside a
// transaction, is marked in flight before it runs and checkpointed after, as
// every statement outside a transaction is: an interruption between the two
// leaves an outcome nobody knows, and a resume refuses to guess it. A data
// query runs in one serializable transaction together with the checkpoint that
// records it, so the two commit together or not at all.
//
// The revision row is therefore the record of what a body committed, and a
// failure never records less than the row holds (see
// [revisionFailureRecord]). A resume after an interruption starts at the
// first query the row does not record, which for a data query is exactly the
// one that did not commit.

// ydbQueryTexts is the texts of the queries a YDB body runs as. A body the
// split refuses is refused before anything runs (see refuseUnsplittableSQL),
// so the statements returned for it here are counted but never executed.
func ydbQueryTexts(sqlText string) []string {
	queries, err := yqlquery.Split(sqlText)
	if err != nil {
		return sqlutil.SplitStatementsForDialect(platform.YDB, sqlText)
	}
	texts := make([]string, 0, len(queries))
	for _, query := range queries {
		texts = append(texts, query.Text)
	}
	return texts
}

// sourceStatementsForDialect returns the source text of each unit the migrator
// runs a body as, which is what partial hashes digest and what a failure
// records as the failing statement. On YDB a unit is a query, whose source is
// its statements' sources.
func sourceStatementsForDialect(sqlText, dialect string) []sqlutil.SourceStatement {
	if platform.NormalizeDialect(dialect) != platform.YDB {
		return sqlutil.SplitSourceStatements(sqlText, dialect)
	}
	queries, err := yqlquery.Split(sqlText)
	if err != nil {
		return sqlutil.SplitSourceStatements(sqlText, dialect)
	}
	statements := make([]sqlutil.SourceStatement, 0, len(queries))
	for _, query := range queries {
		statements = append(statements, sqlutil.SourceStatement{
			Text:       query.Source,
			Terminated: strings.HasSuffix(query.Source, ";"),
		})
	}
	return statements
}

// refuseUnsplittableSQL refuses a YDB body the migrator cannot split the way
// YDB reads it, or one holding a statement that controls a transaction. Every
// other dialect passes.
//
// YQL refuses COMMIT inside a query (`COMMIT not supported inside YDB
// query`, measured on 26.2.1.14), and the migrator decides where each
// transaction begins and ends, so a body that names one is refused here,
// before its first query, rather than by the server halfway through.
func refuseUnsplittableSQL(dialect, sqlText string) error {
	if platform.NormalizeDialect(dialect) != platform.YDB {
		return nil
	}
	queries, err := yqlquery.Split(sqlText)
	if err != nil {
		return err
	}
	for _, query := range queries {
		for _, statement := range query.Statements {
			if isTransactionControlStatement(statement, platform.YDB) {
				return fmt.Errorf("%q controls a transaction, and on YDB the migrator runs each data query "+
					"in a transaction of its own; remove it", statement)
			}
		}
	}
	return nil
}

// refuseUnsplittableMigrations refuses, before a run touches anything, a
// migration whose body in direction refuseUnsplittableSQL refuses.
func (m *Migrator) refuseUnsplittableMigrations(migrations []*Migration, direction MigrationDirection) error {
	for _, migration := range migrations {
		if err := refuseUnsplittableSQL(m.connectionDialect(), migrationSQLForDirection(migration, direction)); err != nil {
			return fmt.Errorf("migration %d cannot run %s on %s: %w",
				migration.Version, direction, m.connectionDialect(), err)
		}
	}
	return nil
}

// runsQueriesOnTheirOwn reports whether the target runs every query of a
// migration on its own, whatever transaction mode the run asked for: YDB has
// no transaction a scheme statement can join, so a file's transaction would
// hold nothing but data queries, which already commit one by one together
// with their checkpoints. `file` is therefore run the way `none` is, with
// progress recorded after each query, and `all` is refused by the
// transactional-DDL capability before anything runs.
func (m *Migrator) runsQueriesOnTheirOwn() bool {
	return platform.NormalizeDialect(m.connectionDialect()) == platform.YDB
}

// effectiveTxMode is the mode a migration runs in on this target, given the
// mode it resolved to.
func (m *Migrator) effectiveTxMode(mode MigrationTxMode) MigrationTxMode {
	if mode == MigrationTxModeFile && m.runsQueriesOnTheirOwn() {
		return MigrationTxModeNone
	}
	return mode
}

// withRecordedStatementProgress installs what records a body's progress while
// it runs outside a transaction: a mark before each statement, a checkpoint
// after it, and on YDB the committer that runs a data query and its checkpoint
// in one transaction.
func (m *Migrator) withRecordedStatementProgress(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	direction MigrationDirection,
) context.Context {
	ctx = withStatementProgressRecorder(
		ctx,
		func(ctx context.Context, event StatementEvent) error {
			return m.markMigrationStatementInFlight(ctx, migration, startedAt, event, direction)
		},
		func(ctx context.Context, event StatementEvent) error {
			return m.checkpointMigrationRevision(ctx, migration, startedAt, event, direction)
		},
	)
	if !m.runsQueriesOnTheirOwn() || m.conn.Writer().IsDryRun() {
		return ctx
	}
	return withStatementCommitter(ctx, statementCommitter{
		claims: isYDBDataQuery,
		run: func(ctx context.Context, event StatementEvent) error {
			return m.commitYDBDataQuery(ctx, migration, startedAt, event, direction)
		},
	})
}

// isYDBDataQuery reports whether a unit of a YDB body is a data query.
func isYDBDataQuery(query string) bool {
	kind, ok := yqlquery.KindOf(query)
	return ok && kind == yqlquery.Data
}

// ydbTransactionAttempts bounds how often a transaction YDB aborted for a
// conflicting one is run again.
const ydbTransactionAttempts = 5

// commitYDBDataQuery runs one data query of a YDB body and the checkpoint that
// records it in one serializable transaction; see [dataQueryCommit].
func (m *Migrator) commitYDBDataQuery(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	event StatementEvent,
	direction MigrationDirection,
) error {
	return dataQueryCommit{
		begin: m.conn.BeginTx,
		query: event.Statement,
		checkpoint: func() (string, []any) {
			return m.checkpointMigrationRevisionStatement(migration, startedAt, event, direction)
		},
		recorded: func(ctx context.Context) (bool, error) {
			revision, err := m.getMigrationRevision(ctx, migration)
			if err != nil {
				return false, err
			}
			return checkpointRecorded(revision, event.Index), nil
		},
		wait: waitForYDBTransactionRetry,
	}.run(ctx)
}

// dataQueryCommit runs a data query and the checkpoint that records it in one
// serializable transaction.
//
// YDB's optimistic locking aborts the transaction of two conflicting ones that
// commits second, with `Transaction locks invalidated`, and nothing of an
// aborted transaction is applied, so the whole transaction runs again. Any
// other failure leaves the transaction uncommitted and is returned, and the
// caller records the query as not applied.
//
// A commit that fails without saying whether it took effect is answered by
// recorded, which reads the revision back: the checkpoint commits with the
// query, so a recorded checkpoint is the proof that the query did too.
type dataQueryCommit struct {
	begin func(context.Context, *sql.TxOptions) (*sql.Tx, error)
	query string
	// checkpoint is the statement that records the query, and its arguments.
	// It is built after the query ran in each attempt, so the execution time
	// it records includes the query and is not carried over from an attempt
	// YDB aborted.
	checkpoint func() (string, []any)
	recorded   func(context.Context) (bool, error)
	wait       func(context.Context, int) error
}

func (d dataQueryCommit) run(ctx context.Context) error {
	var err error
	for attempt := range ydbTransactionAttempts {
		var committing bool
		committing, err = d.try(ctx)
		if err == nil {
			return nil
		}
		if committing && !atlasretry.IsRetryable(err) {
			return d.outcome(ctx, err)
		}
		if !atlasretry.IsRetryable(err) || attempt == ydbTransactionAttempts-1 {
			return err
		}
		if waitErr := d.wait(ctx, attempt); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}
	return err
}

// try is one attempt. committing reports that the failure, if any, was the
// commit's own, whose outcome the server did not report.
func (d dataQueryCommit) try(ctx context.Context) (committing bool, err error) {
	tx, err := d.begin(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, d.query); err != nil {
		// YDB has ended the transaction already: a rollback of it answers
		// `Transaction not found`, and the statement's failure is the one
		// that matters.
		_ = tx.Rollback()
		return false, err
	}
	checkpoint, args := d.checkpoint()
	if _, err := tx.ExecContext(ctx, checkpoint, args...); err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("record the query in the revision table: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return true, err
	}
	return false, nil
}

// outcome answers a commit that failed without saying whether it took effect.
func (d dataQueryCommit) outcome(ctx context.Context, commitErr error) error {
	readCtx, cancel := durableRevisionWriteContext(ctx)
	defer cancel()
	recorded, err := d.recorded(readCtx)
	if err != nil {
		return errors.Join(commitErr, fmt.Errorf("read back whether the query committed: %w", err))
	}
	if recorded {
		return nil
	}
	return commitErr
}

// checkpointRecorded reports whether revision records the unit at index as
// committed: its progress reaches the unit, and no failure was written over
// the checkpoint, which clears the failure columns.
func checkpointRecorded(revision *MigrationRevision, index int) bool {
	return revision != nil && revision.Applied >= index && revision.Error == ""
}

// revisionFailureRecord records a failed migration on YDB without moving its
// progress backwards.
//
// A failure states its progress from the error: the units before the one that
// failed. On YDB that can say less than the revision row, which is the record:
// a data query commits together with its checkpoint, so a commit whose answer
// was lost -- and that the read-back in [dataQueryCommit] could not settle --
// may have committed both. Writing the failure's count over the row would make
// a resume run that query again.
//
// So the failure is written in one serializable transaction with a read of the
// row, and records the larger of the two counts. A row that cannot be read gets
// nothing written: it already says what committed, and a resume reads it. A
// transaction YDB aborts -- because the lost commit landed after the read --
// runs again and reads the row it conflicted with.
type revisionFailureRecord struct {
	begin func(context.Context, *sql.TxOptions) (*sql.Tx, error)
	// read returns the revision row, or nil when there is none.
	read func(context.Context, *sql.Tx) (*MigrationRevision, error)
	// statement is the write that records the failure with applied units
	// committed, and its arguments.
	statement func(applied int) (string, []any)
	// applied is the progress the failure states.
	applied int
	wait    func(context.Context, int) error
}

func (r revisionFailureRecord) run(ctx context.Context) error {
	var err error
	for attempt := range ydbTransactionAttempts {
		err = r.try(ctx)
		if err == nil || !atlasretry.IsRetryable(err) || attempt == ydbTransactionAttempts-1 {
			return err
		}
		if waitErr := r.wait(ctx, attempt); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}
	return err
}

func (r revisionFailureRecord) try(ctx context.Context) error {
	tx, err := r.begin(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	revision, err := r.read(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("read the revision before recording the failure over it: %w", err)
	}
	applied := r.applied
	if revision != nil {
		applied = max(applied, revision.Applied)
	}
	query, args := r.statement(applied)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// ydbFailureRecord is the [revisionFailureRecord] that writes failed over
// the migration's revision row, given the progress the failure states.
func (m *Migrator) ydbFailureRecord(failed failedRevision, applied int) revisionFailureRecord {
	return revisionFailureRecord{
		begin: m.conn.BeginTx,
		read: func(ctx context.Context, tx *sql.Tx) (*MigrationRevision, error) {
			query := sqlutil.Rebind(m.conn.Info().Dialect, m.getRevisionSQL())
			revision, err := m.scanRevisionRow(
				tx.QueryRowContext(ctx, query, m.migrationRevisionVersionArg(failed.migration)),
			)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			return &revision, nil
		},
		statement: func(applied int) (string, []any) { return m.failedRevisionStatement(failed, applied) },
		applied:   applied,
		wait:      waitForYDBTransactionRetry,
	}
}

// waitForYDBTransactionRetry waits longer after each aborted attempt.
func waitForYDBTransactionRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt+1) * 50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
