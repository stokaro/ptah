package migrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/yqlquery"
	"ptah.run/migration/migrationfile"
)

// How the statement timeout bounds a YDB migration.
//
// YDB has no statement timeout a session sets, and its query service takes no
// operation timeout. The table service's does not reach a schema statement
// either: measured on 26.2.1.14 and 25.1.4.7, `ALTER TABLE ... ADD INDEX`
// sent with an operation timeout and a cancel-after of 800 ms each ran for
// 2.7 to 3.9 seconds and succeeded. So the migrator bounds each query of the
// migration with a deadline of its own, and answers for what YDB does when it
// fires, which depends on the query:
//
//   - A data query is canceled, and its transaction, which holds the
//     checkpoint that records it, never commits: measured on both lines, an
//     INSERT ... SELECT of 200000 rows stopped after 250 ms left the target
//     empty, then and ten seconds later. Nothing of it is applied, and the
//     failure says so.
//   - A schema statement YDB runs as a build -- an index added to a table that
//     exists, or a column added with a default -- keeps running when the
//     client stops waiting. The migrator cancels the build through the
//     operation service and waits for it to end (see ydbschema.Builds); a
//     build it stopped leaves nothing applied.
//   - Any other schema statement, and a build that cannot be found or that does
//     not end, keeps running on the server: a CREATE TABLE whose client gave
//     up after 30 ms on 25.1.4.7 created the table. Its outcome is unknown, and
//     the failure keeps the mark that says so, as an interrupted statement
//     does, so a resume refuses to guess.
//
// The deadline bounds the migration's own queries. The migrator's bookkeeping
// -- the revision row, the checkpoint in a data query's transaction, the
// commit -- runs without it, so a statement the bound stops is still recorded.

// buildWait bounds the search for, and the cancellation of, a build a timed
// out schema statement started. The statement's request may still be on its
// way to the scheme shard when its client gives up, so the build is looked for
// for a while rather than once.
var buildWait = ydbschema.BuildWait{Appear: 2 * time.Second, Settle: 30 * time.Second, Poll: 100 * time.Millisecond}

// boundsEachQuery reports whether the statement timeout on this dialect is a
// deadline the executor puts on each query, rather than a setting the
// migrator sends to the session.
func boundsEachQuery(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.YDB
}

// queryTimeoutContextKey carries the statement timeout the queries of a
// migration run under, on a target that bounds each query itself.
type queryTimeoutContextKey struct{}

// withQueryTimeout installs timeouts' statement timeout for the executor.
func withQueryTimeout(ctx context.Context, timeouts migrationfile.Timeouts) context.Context {
	if !timeouts.HasStatementTimeout {
		return ctx
	}
	return context.WithValue(ctx, queryTimeoutContextKey{}, timeouts.StatementTimeout)
}

// boundedQueryContext returns the context one query runs under: ctx with the
// statement timeout's deadline when one is installed, and ctx itself
// otherwise. timedOut reports whether the deadline, rather than ctx, ended the
// query.
func boundedQueryContext(ctx context.Context) (queryCtx context.Context, done func() (timedOut bool), timeout time.Duration) {
	timeout, bounded := ctx.Value(queryTimeoutContextKey{}).(time.Duration)
	if !bounded {
		return ctx, func() bool { return false }, 0
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	return queryCtx, func() bool {
		timedOut := errors.Is(queryCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
		cancel()
		return timedOut
	}, timeout
}

// executeBoundedStatement runs a statement of a migration under the statement
// timeout installed in ctx, and answers for a query the timeout stopped.
func executeBoundedStatement(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	stmt string,
	mode migrationExecutionMode,
) error {
	queryCtx, done, timeout := boundedQueryContext(ctx)
	err := executeMigrationStatement(queryCtx, conn, stmt, mode)
	if timedOut := done(); err == nil || !timedOut {
		return err
	}
	return stopTimedOutSchemeQuery(ctx, conn, stmt, timeout)
}

// stopTimedOutSchemeQuery answers for a schema query the statement timeout
// stopped waiting for: it cancels the build the query started, when there is
// one, and reports what is known about the query afterwards. A nil error
// means the query completed after all.
func stopTimedOutSchemeQuery(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	stmt string,
	timeout time.Duration,
) error {
	table, named := yqlquery.AlteredTable(stmt)
	if !named {
		return queryOutcomeUnknownError(timeout, "YDB keeps running a schema statement after the client stops waiting")
	}
	outcome, err := cancelBuild(ctx, conn, table)
	switch {
	case err != nil:
		return errors.Join(
			queryOutcomeUnknownError(timeout, "the build it may have started could not be canceled"),
			err)
	case outcome == ydbschema.BuildStopped:
		return queryNotAppliedError(timeout, fmt.Sprintf("YDB canceled the build it started on %s", table))
	case outcome == ydbschema.BuildCompleted:
		return nil
	case outcome == ydbschema.BuildUnsettled:
		return queryOutcomeUnknownError(timeout, fmt.Sprintf("the build on %s did not end when it was canceled", table))
	default:
		return queryOutcomeUnknownError(timeout, "YDB keeps running a schema statement after the client stops waiting")
	}
}

// buildCanceller is the YDB writer's reach into the operation service.
type buildCanceller interface {
	CancelRunningBuild(ctx context.Context, table string, wait ydbschema.BuildWait) (ydbschema.BuildOutcome, error)
}

// cancelBuild cancels the build running on table, through the writer of the
// connection the query ran on.
func cancelBuild(ctx context.Context, conn *dbschema.DatabaseConnection, table string) (ydbschema.BuildOutcome, error) {
	canceller, ok := conn.SchemaWriter().(buildCanceller)
	if !ok {
		return ydbschema.BuildUnsettled, fmt.Errorf("the %T writer reaches no build to cancel", conn.SchemaWriter())
	}
	return canceller.CancelRunningBuild(ctx, table, buildWait)
}

// queryNotAppliedError reports a query the statement timeout stopped, of
// which nothing was applied. It does not wrap the context's error: the
// migrator reads a deadline in a failure as an outcome nobody knows, and this
// one is known.
func queryNotAppliedError(timeout time.Duration, how string) error {
	return fmt.Errorf("the statement timeout of %s ran out, and %s, so nothing of the query was applied", timeout, how)
}

// queryOutcomeUnknownError reports a query the statement timeout stopped
// waiting for, which may still have been applied. It wraps
// context.DeadlineExceeded, which is what keeps the failure from being
// recorded as a statement that did not run.
func queryOutcomeUnknownError(timeout time.Duration, why string) error {
	return fmt.Errorf("the statement timeout of %s ran out, and %s, so whether the query was applied is unknown: "+
		"inspect the database and repair the revision before running it again: %w", timeout, why, context.DeadlineExceeded)
}
