package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasretry"
	"ptah.run/internal/migratorext"
)

// How a migration body runs on the built-in YDB engine.
//
// YDB runs a scheme statement only outside a transaction and never in the same
// query as a data statement, and a named expression, an action, a DECLARE or a
// PRAGMA exists only in the query that holds it. So the migrator runs a YDB
// body as the queries internal/yqlquery splits it into -- each scheme
// statement a query of its own, each run of data statements one query -- and
// those queries are the units it counts, records progress over and resumes
// from (see query_units.go). A data query commits with its checkpoint through
// database/sql, and a failure is written with a read of the row in one
// serializable transaction, so it never records less than the row holds.

// builtinUnits are the queries a built-in engine's services divide a body
// into.
type builtinUnits struct {
	units   migratorext.Units
	dialect string
}

func (b builtinUnits) split(sqlText string) ([]queryUnit, error) {
	split, err := b.units.Split(sqlText)
	if err != nil {
		return nil, err
	}
	units := make([]queryUnit, 0, len(split))
	for _, unit := range split {
		units = append(units, queryUnit{
			text: unit.Text, source: unit.Source, statements: unit.Statements, mixed: unit.Mixed, data: b.units.IsData(unit.Text),
		})
	}
	return units, nil
}

// refuse refuses a body the migrator cannot split the way the engine reads
// it: one holding a statement that controls a transaction, and one holding a
// query the engine refuses whole.
//
// YQL refuses COMMIT inside a query (`COMMIT not supported inside YDB
// query`, measured on 26.2.1.14), and the migrator decides where each
// transaction begins and ends, so a body that names one is refused here,
// before its first query, rather than by the server halfway through.
//
// A block or an action call that runs a scheme statement and a statement that
// reads or writes a table is refused by YDB whole (`Queries with mixed data and
// scheme operations are not supported`; see [yqlquery.Reader.Mixed]). A scheme
// statement runs outside any transaction, so the queries before such a block
// would stay applied when it fails; it is refused here, before any of them.
func (b builtinUnits) refuse(sqlText string) error {
	units, err := b.split(sqlText)
	if err != nil {
		return err
	}
	for _, unit := range units {
		if unit.mixed {
			return fmt.Errorf("%q runs a scheme statement and a statement that reads or writes a table in one "+
				"query, which YDB refuses whole (Queries with mixed data and scheme operations are not supported); "+
				"move the scheme statement out of the block or action into a statement of its own", unit.source)
		}
		for _, statement := range unit.statements {
			if isTransactionControlStatement(statement, b.dialect) {
				return fmt.Errorf("%q controls a transaction, and on YDB the migrator runs each data query "+
					"in a transaction of its own; remove it", statement)
			}
		}
	}
	return nil
}

// sqlDataQueryCommit is the [dataQueryCommit] of a database/sql connection:
// each attempt runs the query and the checkpoint statement in one
// serializable transaction begin opens.
//
// YDB's optimistic locking aborts the transaction of two conflicting ones that
// commits second, with `Transaction locks invalidated`, which
// internal/atlasretry reads as retryable.
func sqlDataQueryCommit(
	begin func(context.Context, *sql.TxOptions) (*sql.Tx, error),
	query string,
	checkpoint func() (string, []any),
	recorded func(context.Context) (bool, error),
) dataQueryCommit {
	return dataQueryCommit{
		begin: func(ctx context.Context) (dataQueryAttempt, error) {
			tx, err := begin(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				return nil, err
			}
			return sqlDataQuery{tx: tx, query: query, statement: checkpoint}, nil
		},
		retryable: atlasretry.IsRetryable,
		recorded:  recorded,
		wait:      waitForConflictRetry,
	}
}

// sqlDataQuery is one attempt's database/sql transaction.
type sqlDataQuery struct {
	tx    *sql.Tx
	query string
	// statement is the checkpoint statement and its arguments.
	statement func() (string, []any)
}

// execute runs the query under the statement timeout's deadline. The
// transaction holds the checkpoint too and never commits after the deadline
// stopped the query, so the query is known not to have been applied.
func (q sqlDataQuery) execute(ctx context.Context) error {
	queryCtx, done, timeout := boundedQueryContext(ctx)
	_, err := q.tx.ExecContext(queryCtx, q.query)
	if timedOut := done(); err != nil && timedOut {
		return queryNotAppliedError(timeout, "YDB canceled the query, whose transaction never committed")
	}
	return err
}

func (q sqlDataQuery) checkpoint(ctx context.Context) error {
	statement, args := q.statement()
	_, err := q.tx.ExecContext(ctx, statement, args...)
	return err
}

func (q sqlDataQuery) commit() error { return q.tx.Commit() }

// rollback ends a transaction the attempt will not commit. After a failed
// statement YDB has ended the transaction already, and a rollback of it
// answers `Transaction not found`; the statement's failure is the one that
// matters.
func (q sqlDataQuery) rollback() { _ = q.tx.Rollback() }

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
	for attempt := range conflictAttempts {
		err = r.try(ctx)
		if err == nil || !atlasretry.IsRetryable(err) || attempt == conflictAttempts-1 {
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
		wait:      waitForConflictRetry,
	}
}
