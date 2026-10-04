package dblock

// White-box testing required: whether a lock held on a session of its own is
// still held is decided by a watch that pings the session, and by the time its
// last answer was asked for. Driving that through Acquire needs a live server
// whose session is ended under the lock; integration/migrator does that on
// PostgreSQL and MySQL, and these pin each rule with a fake ping and a clock
// the test moves.

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// fakeSession answers a ping with err, and counts pings and unlocks.
type fakeSession struct {
	err     atomic.Pointer[error]
	pings   atomic.Int64
	unlocks atomic.Int64
}

func (s *fakeSession) ping(context.Context) error {
	s.pings.Add(1)
	if err := s.err.Load(); err != nil {
		return *err
	}
	return nil
}

func (s *fakeSession) unlock(context.Context) error {
	s.unlocks.Add(1)
	return nil
}

func (s *fakeSession) fail(err error) { s.err.Store(&err) }

// newFakeSessionHold returns a hold over a fake session with no watch running,
// so only the test decides when a check happens.
func newFakeSessionHold() (*sessionHold, *fakeSession, *fakeClock) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	session := &fakeSession{}
	return newIdleSessionHold(session.ping, session.unlock, clock.Now), session, clock
}

// The lock is held while the session answered within one check interval; past
// that, the session is asked again, and an answer renews it from the time the
// ping was sent.
func TestSessionHold_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		elapsed   time.Duration
		wantPings int64
	}{
		{name: "fresh from the acquisition", elapsed: 0, wantPings: 0},
		{name: "at the interval's end", elapsed: sessionCheckInterval, wantPings: 0},
		{name: "past the interval, the session answers", elapsed: sessionCheckInterval + time.Millisecond, wantPings: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			hold, session, clock := newFakeSessionHold()
			clock.advance(test.elapsed)

			c.Assert(hold.held(), qt.IsTrue)
			c.Assert(closed(hold.lost()), qt.IsFalse)
			c.Assert(session.pings.Load(), qt.Equals, test.wantPings)
			c.Assert(hold.held(), qt.IsTrue)
			c.Assert(session.pings.Load(), qt.Equals, test.wantPings)
		})
	}
}

// A session that does not answer has lost the lock, whether the watch or the
// holder asked.
func TestSessionHold_FailurePath(t *testing.T) {
	t.Run("the watch's ping fails", func(t *testing.T) {
		c := qt.New(t)
		hold, session, _ := newFakeSessionHold()
		session.fail(driver.ErrBadConn)

		hold.check()

		c.Assert(closed(hold.lost()), qt.IsTrue)
		c.Assert(hold.held(), qt.IsFalse)
	})
	t.Run("the holder asks a session that stopped answering", func(t *testing.T) {
		c := qt.New(t)
		hold, session, clock := newFakeSessionHold()
		session.fail(driver.ErrBadConn)
		clock.advance(sessionCheckInterval + time.Millisecond)

		c.Assert(hold.held(), qt.IsFalse)
		c.Assert(closed(hold.lost()), qt.IsTrue)
		c.Assert(session.pings.Load(), qt.Equals, int64(1))
	})
}

// A release is not a loss: it stops the watch and gives the lock back, and a
// ping that fails after it loses nothing.
func TestSessionHold_Release(t *testing.T) {
	c := qt.New(t)
	hold, session, clock := newFakeSessionHold()

	err := hold.release(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(session.unlocks.Load(), qt.Equals, int64(1))
	session.fail(driver.ErrBadConn)
	hold.check()
	clock.advance(time.Hour)
	c.Assert(hold.held(), qt.IsTrue)
	c.Assert(closed(hold.lost()), qt.IsFalse)
}

// A lock whose session ended has nothing to give back: its release sends no
// unlock and answers errSessionEnded.
func TestSessionHold_ReleaseOfALostLock(t *testing.T) {
	c := qt.New(t)
	hold, session, _ := newFakeSessionHold()
	session.fail(errors.New("terminating connection due to administrator command"))
	hold.check()

	err := hold.release(context.Background())

	c.Assert(err, qt.ErrorIs, errSessionEnded)
	c.Assert(session.unlocks.Load(), qt.Equals, int64(0))
}

// The watch pings the session on its own and stops at the release, which
// waits for it.
func TestSessionHold_WatchReportsALoss(t *testing.T) {
	c := qt.New(t)
	session := &fakeSession{}
	hold := newSessionHold(session.ping, session.unlock, time.Now)
	c.Cleanup(func() { _ = hold.release(context.Background()) })

	session.fail(driver.ErrBadConn)

	c.Assert(closesWithin(hold.lost(), 5*sessionCheckInterval), qt.IsTrue)
	c.Assert(session.pings.Load() >= 1, qt.IsTrue)
}

// A Lock over a session that ended reports the loss through Err, Done and the
// context Guard returns, and its release discards the connection without an
// error: the server released the lock with the session.
func TestLock_ReportsTheEndOfItsSession(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, nil
	})
	conn, err := db.SQL.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	hold, session, _ := newFakeSessionHold()
	lock := &Lock{conn: conn, name: "ptah_migrate", dialect: "postgres", release: hold.release, holding: hold}
	guarded, stop := lock.Guard(context.Background())
	c.Cleanup(stop)
	c.Assert(lock.Err(), qt.IsNil)

	session.fail(driver.ErrBadConn)
	hold.check()

	c.Assert(lock.Err(), qt.ErrorMatches, `advisory lock "ptah_migrate" on postgres was lost while it was held: .*`)
	c.Assert(IsLost(lock.Err()), qt.IsTrue)
	c.Assert(closed(lock.Done()), qt.IsTrue)
	c.Assert(closesWithin(guarded.Done(), 5*time.Second), qt.IsTrue)
	c.Assert(IsLost(context.Cause(guarded)), qt.IsTrue)
	c.Assert(lock.Release(context.Background()), qt.IsNil)
	c.Assert(session.unlocks.Load(), qt.Equals, int64(0))
}

// Settle is the work's own error while the lock holds. Once the lock is lost
// it is the loss, which names the work's error only when the loss did not
// cause it.
func TestLock_Settle(t *testing.T) {
	tests := []struct {
		name string
		// failedChecks is how many checks of the session fail before Settle.
		failedChecks int
		wantLost     bool
		workErr      error
		want         string
	}{
		{name: "held, and the work succeeded", want: "<nil>"},
		{name: "held, and the work failed", workErr: errors.New("syntax error"), want: "syntax error"},
		{
			name: "lost, and the work succeeded", failedChecks: 1, wantLost: true,
			want: `apply lock: advisory lock "ptah_migrate" on postgres was lost while it was held: .*; ` +
				`the apply stopped there`,
		},
		{
			name: "lost, and the loss canceled the work", failedChecks: 1, wantLost: true, workErr: context.Canceled,
			want: `apply lock: advisory lock "ptah_migrate" on postgres was lost while it was held: .*; ` +
				`the apply stopped there`,
		},
		{
			name: "lost, and the work failed on its own", failedChecks: 1, wantLost: true, workErr: errors.New("syntax error"),
			want: `apply lock: advisory lock "ptah_migrate" on postgres was lost while it was held: .*; ` +
				`the apply stopped there \(the apply reported: syntax error\)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			hold, session, _ := newFakeSessionHold()
			lock := &Lock{name: "ptah_migrate", dialect: "postgres", release: hold.release, holding: hold}
			for range test.failedChecks {
				session.fail(driver.ErrBadConn)
				hold.check()
			}

			got := lock.Settle("apply lock", "the apply", test.workErr)

			c.Assert(fmt.Sprint(got), qt.Matches, test.want)
			c.Assert(IsLost(got), qt.Equals, test.wantLost)
		})
	}
}
