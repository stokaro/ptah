package dblock

// White-box testing required: whether a YDB lock is still held is decided by a
// watch over a coordination session and the time its last answer was asked
// for. Driving that through Acquire needs a live server and a process that
// stops answering for longer than a session timeout; the live tests under
// integration/dbschema/ydb do that once, and these pin each rule with a fake
// session and a clock the test moves.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
)

// fakeClock is a clock the test moves.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// fakeSemaphoreSession answers DescribeSemaphore with owners or err, and
// records whether it was closed. A Close ends the fake lease's Release.
type fakeSemaphoreSession struct {
	id       uint64
	owners   []uint64
	err      error
	closed   chan struct{}
	closeOne sync.Once
}

func (s *fakeSemaphoreSession) DescribeSemaphore(
	context.Context, string, ...options.DescribeSemaphoreOption,
) (*coordination.SemaphoreDescription, error) {
	description := &coordination.SemaphoreDescription{}
	for _, owner := range s.owners {
		description.Owners = append(description.Owners, &coordination.SemaphoreSession{SessionID: owner})
	}
	return description, s.err
}

func (s *fakeSemaphoreSession) SessionID() uint64 { return s.id }

func (s *fakeSemaphoreSession) Close(context.Context) error {
	s.closeOne.Do(func() { close(s.closed) })
	return nil
}

// fakeSemaphoreLease is a lease whose Release answers err, or waits for the
// session to close when it is told to hang.
type fakeSemaphoreLease struct {
	ctx     context.Context
	cancel  context.CancelFunc
	session *fakeSemaphoreSession
	err     error
	hang    bool
}

func (l *fakeSemaphoreLease) Context() context.Context { return l.ctx }

func (l *fakeSemaphoreLease) Release() error {
	if l.hang {
		<-l.session.closed
		return errors.New("session closed")
	}
	l.cancel()
	return l.err
}

// newFakeHold returns a hold over a fake session that owns the semaphore, and
// whose watch is not running, so only the test decides when a check happens.
func newFakeHold(c *qt.C) (*ydbHold, *fakeSemaphoreSession, *fakeSemaphoreLease, *fakeClock) {
	c.Helper()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	session := &fakeSemaphoreSession{id: 7, owners: []uint64{7}, closed: make(chan struct{})}
	leaseCtx, cancel := context.WithCancel(context.Background())
	c.Cleanup(cancel)
	lease := &fakeSemaphoreLease{ctx: leaseCtx, cancel: cancel, session: session}
	watching := make(chan struct{})
	close(watching)
	hold := &ydbHold{
		session:   session,
		lease:     lease,
		name:      "ptah_migrate",
		now:       clock.Now,
		confirmed: clock.Now(),
		lostCh:    make(chan struct{}),
		stopWatch: func() {},
		watching:  watching,
	}
	return hold, session, lease, clock
}

// The lock is held while an answer that the session owns it is younger than
// half the session timeout; a check answered with ownership renews it from the
// time the question was asked.
func TestYDBHold_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		elapsed time.Duration
		checks  int
	}{
		{name: "fresh from the acquisition", elapsed: 0},
		{name: "at the validity's end", elapsed: ydbHoldValidity},
		{name: "renewed by a check", elapsed: ydbHoldValidity + time.Second, checks: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			hold, _, _, clock := newFakeHold(c)
			clock.advance(time.Second)
			for range test.checks {
				hold.check(context.Background())
			}
			clock.advance(test.elapsed - time.Second)

			c.Assert(hold.held(), qt.IsTrue)
			c.Assert(closed(hold.lost()), qt.IsFalse)
		})
	}
}

// Each row is a way to lose the lock: the server says another session owns
// it, or no confirming answer is young enough.
func TestYDBHold_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		owners   []uint64
		describe error
		elapsed  time.Duration
	}{
		{name: "another session owns it", owners: []uint64{8}},
		{name: "nobody owns it", owners: nil},
		{
			name: "no answer for longer than the validity", owners: []uint64{7},
			describe: errors.New("deadline exceeded"), elapsed: ydbHoldValidity + time.Millisecond,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			hold, session, _, clock := newFakeHold(c)
			session.owners = test.owners
			session.err = test.describe
			hold.check(context.Background())
			clock.advance(test.elapsed)

			c.Assert(hold.held(), qt.IsFalse)
			c.Assert(closed(hold.lost()), qt.IsTrue)
		})
	}
}

// The SDK ends the lease when it gives the session up, and that is a loss
// too.
func TestYDBHold_EndedLeaseIsALoss(t *testing.T) {
	c := qt.New(t)
	hold, _, lease, _ := newFakeHold(c)

	lease.cancel()

	c.Assert(hold.held(), qt.IsFalse)
	c.Assert(closed(hold.lost()), qt.IsTrue)
}

// closed reports whether ch is closed.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// A check that gets no answer changes nothing: the last answer stands until
// it is too old.
func TestYDBHold_UnansweredCheckKeepsTheLastAnswer(t *testing.T) {
	c := qt.New(t)
	hold, session, _, clock := newFakeHold(c)
	clock.advance(ydbHoldValidity - time.Second)
	session.err = errors.New("deadline exceeded")

	hold.check(context.Background())
	clock.advance(time.Second)

	c.Assert(hold.held(), qt.IsTrue)
	clock.advance(time.Millisecond)
	c.Assert(hold.held(), qt.IsFalse)
}

// A release is not a loss, and gives the lease back before it ends the
// session; a lease that does not answer is ended by closing the session once
// the release context ends.
func TestYDBHold_Release(t *testing.T) {
	tests := []struct {
		name    string
		hang    bool
		err     error
		timeout time.Duration
		wantErr string
	}{
		{name: "a release that answers", timeout: time.Second, wantErr: "<nil>"},
		{name: "a release that fails", err: errors.New("session expired"), timeout: time.Second,
			wantErr: `release the YDB semaphore "ptah_migrate": session expired`},
		{name: "a release that hangs", hang: true, timeout: 10 * time.Millisecond,
			wantErr: `release the YDB semaphore "ptah_migrate": context deadline exceeded`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			hold, session, lease, clock := newFakeHold(c)
			lease.hang = test.hang
			lease.err = test.err
			ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
			c.Cleanup(cancel)

			err := hold.release(ctx)

			c.Assert(fmt.Sprint(err), qt.Equals, test.wantErr)
			c.Assert(closed(session.closed), qt.IsTrue)
			clock.advance(time.Hour)
			c.Assert(hold.held(), qt.IsTrue)
			c.Assert(closed(hold.lost()), qt.IsFalse)
		})
	}
}

// A lock already lost has nothing to give back: its release ends the session
// without waiting on the lease, which on a live server waited for the whole
// release timeout.
func TestYDBHold_ReleaseOfALostLockEndsTheSession(t *testing.T) {
	c := qt.New(t)
	hold, session, lease, _ := newFakeHold(c)
	lease.hang = true
	session.owners = []uint64{8}
	hold.check(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	c.Cleanup(cancel)

	err := hold.release(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(closed(session.closed), qt.IsTrue)
	c.Assert(ctx.Err(), qt.IsNil)
}

// A lock reports its loss through Err, Done and the context Guard returns,
// and a lock that reports no loss answers none of them.
func TestLock_ReportsALoss(t *testing.T) {
	c := qt.New(t)
	hold, session, _, _ := newFakeHold(c)
	lock := &Lock{name: "ptah_migrate", dialect: "ydb", release: hold.release, holding: hold}
	guarded, stop := lock.Guard(context.Background())
	c.Cleanup(stop)
	c.Assert(lock.Err(), qt.IsNil)

	session.owners = []uint64{8}
	hold.check(context.Background())

	c.Assert(lock.Err(), qt.ErrorMatches, `advisory lock "ptah_migrate" on ydb was lost while it was held: .*`)
	c.Assert(IsLost(lock.Err()), qt.IsTrue)
	c.Assert(closed(lock.Done()), qt.IsTrue)
	<-guarded.Done()
	c.Assert(IsLost(context.Cause(guarded)), qt.IsTrue)
}

func TestLock_WithoutALossToReport(t *testing.T) {
	c := qt.New(t)
	lock := &Lock{name: "ptah_migrate", dialect: "postgres", release: func(context.Context) error { return nil }}
	guarded, stop := lock.Guard(context.Background())

	c.Assert(lock.Done(), qt.IsNil)
	c.Assert(lock.Err(), qt.IsNil)
	c.Assert(guarded.Err(), qt.IsNil)
	stop()
	c.Assert(context.Cause(guarded), qt.Equals, context.Canceled)
}
