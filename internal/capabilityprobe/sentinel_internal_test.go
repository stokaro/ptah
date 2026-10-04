package capabilityprobe

// White-box testing required: createSentinel's wait for a YDB database that is
// still binding its storage is reachable only through a live run against a
// fresh server, and the refusal it waits on lasts about a second. A scripted
// executor answers each CREATE TABLE the way that server does, and synctest's
// clock runs the one-second waits without spending them.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/ydbready"
)

// errStorageNotReady is the refusal local-ydb 26.2.1.14 answers a CREATE TABLE
// with for about a second after it first answers a query.
var errStorageNotReady = errors.New("Status: GENERIC_ERROR Issues: <main>: Error: " +
	"database doesn't have storage pools at all to create tablet channels to storage pool kind")

// scriptedExecutor answers the n-th statement with answers[n], and every
// statement after the last with the last answer.
type scriptedExecutor struct {
	answers []error
	calls   int
}

func (e *scriptedExecutor) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	answer := e.answers[min(e.calls, len(e.answers)-1)]
	e.calls++
	return driver.RowsAffected(0), answer
}

func (e *scriptedExecutor) ExecuteSQL(ctx context.Context, statement string, args ...any) error {
	_, err := e.ExecContext(ctx, statement, args...)
	return err
}

func (*scriptedExecutor) IsDryRun() bool { return false }

// sentinelRun creates the sentinel on a YDB session whose statements answer
// answers in turn, with ctx ending after timeout, and returns the attempts and
// how much clock time the run spent. The session's liveness check reads an
// in-memory SQLite database, which answers it.
func sentinelRun(c *qt.C, answers []error, timeout time.Duration) ([]Attempt, time.Duration) {
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	s := &session{
		conn:    conn.WithExecutor(&scriptedExecutor{answers: answers}),
		dialect: platform.YDB,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	attempts := s.createSentinel(ctx)
	return attempts, time.Since(start)
}

// accepted reports, in order, whether each attempt was accepted.
func accepted(attempts []Attempt) []bool {
	out := make([]bool, 0, len(attempts))
	for _, attempt := range attempts {
		out = append(out, attempt.Accepted)
	}
	return out
}

// A server that binds its storage while the sentinel waits takes the table on
// the attempt after its last refusal, one interval later per refusal.
func TestCreateSentinel_WaitsForYDBStorage_HappyPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := qt.New(t)

		attempts, elapsed := sentinelRun(c, []error{errStorageNotReady, errStorageNotReady, nil}, time.Hour)

		c.Assert(accepted(attempts), qt.DeepEquals, []bool{false, false, true})
		c.Assert(elapsed, qt.Equals, 2*ydbready.Interval)
	})
}

// The wait is bounded, it ends with the caller's context, and it is spent on
// the one refusal a fresh server gives: any other refusal is the answer.
func TestCreateSentinel_WaitsForYDBStorage_FailurePath(t *testing.T) {
	for _, tc := range []struct {
		name         string
		answers      []error
		timeout      time.Duration
		wantAttempts int
		wantElapsed  time.Duration
	}{{
		name:         "a server that never binds its storage",
		answers:      []error{errStorageNotReady},
		timeout:      time.Hour,
		wantAttempts: ydbready.Retries + 1,
		wantElapsed:  ydbready.Retries * ydbready.Interval,
	}, {
		name:         "a context that ends during the wait",
		answers:      []error{errStorageNotReady},
		timeout:      2*ydbready.Interval + ydbready.Interval/2,
		wantAttempts: 3,
		wantElapsed:  2*ydbready.Interval + ydbready.Interval/2,
	}, {
		name:         "another refusal",
		answers:      []error{errors.New("Status: SCHEME_ERROR Issues: <main>: Error: Type annotation")},
		timeout:      time.Hour,
		wantAttempts: 1,
		wantElapsed:  0,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := qt.New(t)

				attempts, elapsed := sentinelRun(c, tc.answers, tc.timeout)

				c.Assert(attempts, qt.HasLen, tc.wantAttempts)
				c.Assert(accepted(attempts), qt.Not(qt.Contains), true)
				c.Assert(elapsed, qt.Equals, tc.wantElapsed)
			})
		})
	}
}
