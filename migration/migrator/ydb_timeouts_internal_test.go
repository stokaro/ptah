package migrator

// White-box testing required: the statement timeout on YDB is a deadline the
// executor puts on each query, carried in the context and answered by
// package-local code, and the exported path that reaches it needs a live YDB
// connection. The live tests under integration/dbschema/ydb drive that path
// against both certified lines; these pin how each stopped query is reported,
// which decides whether a resume may run it again.

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

func statementTimeout(d time.Duration) migrationfile.Timeouts {
	return migrationfile.Timeouts{StatementTimeout: d, HasStatementTimeout: true}
}

// A data query the statement timeout stops never commits, since its
// transaction holds the checkpoint too: it is rolled back, not run again, and
// reported as not applied, without the deadline a failure record reads as an
// outcome nobody knows.
func TestDataQueryCommit_StatementTimeoutStopsTheQuery(t *testing.T) {
	c := qt.New(t)
	fake := &fakeTxDriver{queryBlocks: true}
	ctx := withQueryTimeout(context.Background(), statementTimeout(20*time.Millisecond))

	err := newDataQueryCommit(c, fake, false, nil).run(ctx)

	c.Assert(err, qt.ErrorMatches, `the statement timeout of 20ms ran out, and YDB canceled the query, whose `+
		`transaction never committed, so nothing of the query was applied`)
	c.Assert(err, qt.Not(qt.ErrorIs), context.DeadlineExceeded)
	c.Assert(fake.log, qt.DeepEquals, []string{"begin isolation=6", "exec UPSERT data (0 args)", "rollback"})
	c.Assert(fake.commitAttempt, qt.Equals, 0)
}

// A run canceled from outside is not a statement timeout, even where one is
// installed: the failure is the cancellation's.
func TestDataQueryCommit_CancellationIsNotATimeout(t *testing.T) {
	c := qt.New(t)
	fake := &fakeTxDriver{queryBlocks: true}
	ctx, cancel := context.WithTimeout(
		withQueryTimeout(context.Background(), statementTimeout(time.Hour)), 20*time.Millisecond)
	c.Cleanup(cancel)

	err := newDataQueryCommit(c, fake, false, nil).run(ctx)

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(err, qt.Not(qt.ErrorMatches), `the statement timeout .*`)
}

// Without a statement timeout the query runs under the run's own context.
func TestBoundedQueryContext_WithoutATimeout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	queryCtx, done, timeout := boundedQueryContext(ctx)

	_, hasDeadline := queryCtx.Deadline()
	c.Assert(hasDeadline, qt.IsFalse)
	c.Assert(done(), qt.IsFalse)
	c.Assert(timeout, qt.Equals, time.Duration(0))
}

// A schema query that is not an ALTER TABLE of a named table started no build
// the migrator can find, and YDB keeps running it after the client gives up,
// so its outcome is unknown: the failure wraps the deadline, which keeps the
// in-flight mark a resume refuses to guess past.
func TestStopTimedOutSchemeQuery_UnknownOutcome(t *testing.T) {
	tests := []struct {
		name string
		stmt string
	}{
		{name: "create table", stmt: "CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id))"},
		{name: "named expression", stmt: "$t = 'dir/t';\nALTER TABLE $t ADD INDEX i GLOBAL ON (v)"},
		{name: "table path prefix", stmt: "PRAGMA TablePathPrefix('/local/dir');\nALTER TABLE t ADD INDEX i GLOBAL ON (v)"},
		{name: "batch", stmt: "BATCH UPDATE `t` SET v = 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := stopTimedOutSchemeQuery(context.Background(), nil, test.stmt, time.Second)

			c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
			c.Assert(err, qt.ErrorMatches, `the statement timeout of 1s ran out, and YDB keeps running a schema statement `+
				`after the client stops waiting, so whether the query was applied is unknown: .*`)
		})
	}
}
