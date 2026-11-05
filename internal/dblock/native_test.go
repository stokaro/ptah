package dblock_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
)

type nativeLockConnection struct{ service dbschema.SessionLocker }

func (nativeLockConnection) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "lock-fixture"}
}
func (nativeLockConnection) Conn(context.Context) (*sql.Conn, error) {
	return nil, fmt.Errorf("native lock unexpectedly requested a SQL connection")
}
func (c nativeLockConnection) SessionLocker() (dbschema.SessionLocker, error) { return c.service, nil }

type nativeLockService struct {
	calls  int
	held   dbschema.SessionLock
	err    error
	cancel context.CancelFunc
}

func (s *nativeLockService) TryLock(context.Context, string) (dbschema.SessionLock, error) {
	s.calls++
	if s.cancel != nil {
		s.cancel()
	}
	return s.held, s.err
}

type nativeLockFixture struct {
	releases        int
	cleanupCanceled bool
	cleanupBounded  bool
}

func (*nativeLockFixture) CheckOwnership(context.Context) error { return nil }
func (l *nativeLockFixture) Release(ctx context.Context) error {
	l.releases++
	l.cleanupCanceled = ctx.Err() != nil
	_, l.cleanupBounded = ctx.Deadline()
	return nil
}

func TestNativeLockDoesNotRetryAnUnknownBusyResult(t *testing.T) {
	c := qt.New(t)
	service := &nativeLockService{err: errors.Join(dbschema.ErrSessionLockBusy, ptaherr.ErrOutcomeUnknown)}
	held, err := dblock.Acquire(c.Context(), nativeLockConnection{service}, "migration", time.Second)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(held, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 1)
}

func TestNativeLockCancellationAfterAcquisitionReleases(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	owned := &nativeLockFixture{}
	service := &nativeLockService{held: owned, cancel: cancel}
	held, err := dblock.Acquire(ctx, nativeLockConnection{service}, "migration", time.Second)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(held, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 1)
	c.Assert(owned.releases, qt.Equals, 1)
	c.Assert(owned.cleanupCanceled, qt.IsFalse)
	c.Assert(owned.cleanupBounded, qt.IsTrue)
}

func TestNativeLockRefusesEmptyAcquisition(t *testing.T) {
	for _, test := range []struct {
		name string
		held dbschema.SessionLock
	}{
		{"nil", nil},
		{"typed nil", (*nativeLockFixture)(nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := &nativeLockService{held: test.held}
			held, err := dblock.Acquire(c.Context(), nativeLockConnection{service}, "migration", dblock.NoWait)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
			c.Assert(held, qt.IsNil)
		})
	}
}

func TestNativeLockNoWaitAttemptsOnce(t *testing.T) {
	c := qt.New(t)
	service := &nativeLockService{err: dbschema.ErrSessionLockBusy}
	held, err := dblock.Acquire(c.Context(), nativeLockConnection{service}, "migration", dblock.NoWait)
	c.Assert(dblock.IsTimeout(err), qt.IsTrue)
	c.Assert(held, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 1)
}

func TestNativeLockCanceledWaitDoesNotAttempt(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	service := &nativeLockService{err: dbschema.ErrSessionLockBusy}
	held, err := dblock.Acquire(ctx, nativeLockConnection{service}, "migration", 0)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(held, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 0)
}

// slowLockService answers each attempt after delay, the way a provider's lock
// call takes a round trip. An attempt whose context ends first has no answer,
// so its outcome is unknown, as the SDK client reports it.
type slowLockService struct {
	delay   time.Duration
	held    dbschema.SessionLock
	calls   int
	cancel  context.CancelFunc
	elapsed []time.Duration
}

func (s *slowLockService) TryLock(ctx context.Context, _ string) (dbschema.SessionLock, error) {
	s.calls++
	if s.cancel != nil {
		s.cancel()
	}
	started := time.Now()
	defer func() { s.elapsed = append(s.elapsed, time.Since(started)) }()
	select {
	case <-ctx.Done():
		return nil, errors.Join(ptaherr.ErrOutcomeUnknown, ctx.Err())
	case <-time.After(s.delay):
	}
	if s.held != nil {
		return s.held, nil
	}
	return nil, dbschema.ErrSessionLockBusy
}

// A wait whose deadline passes while an attempt is in flight reports the
// timeout: the attempt is not cut short, so it answers busy, and the busy
// answer after the deadline is the timeout. A run with a longer wait gets the
// next attempt.
func TestNativeLockTimeoutDuringAnAttempt_FailurePath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := qt.New(t)
		service := &slowLockService{delay: 3 * time.Second}

		held, err := dblock.Acquire(c.Context(), nativeLockConnection{service}, "migration", time.Second)

		c.Assert(dblock.IsTimeout(err), qt.IsTrue, qt.Commentf("error: %v", err))
		c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrOutcomeUnknown)
		c.Assert(err, qt.ErrorMatches, `timed out acquiring advisory lock "migration" on lock-fixture after 1s`)
		c.Assert(held, qt.IsNil)
		c.Assert(service.calls, qt.Equals, 1)
		c.Assert(service.elapsed, qt.DeepEquals, []time.Duration{3 * time.Second})
	})
}

// An acquisition that arrives after the deadline is released and reported as
// the timeout, on a context the caller's wait did not end.
func TestNativeLockAcquiredAfterTheDeadline_FailurePath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := qt.New(t)
		owned := &nativeLockFixture{}
		service := &slowLockService{delay: 3 * time.Second, held: owned}

		held, err := dblock.Acquire(c.Context(), nativeLockConnection{service}, "migration", time.Second)

		c.Assert(dblock.IsTimeout(err), qt.IsTrue, qt.Commentf("error: %v", err))
		c.Assert(held, qt.IsNil)
		c.Assert(owned.releases, qt.Equals, 1)
		c.Assert(owned.cleanupCanceled, qt.IsFalse)
		c.Assert(owned.cleanupBounded, qt.IsTrue)
	})
}

// An attempt that answers within the wait acquires the lock.
func TestNativeLockSlowAttemptWithinTheWait_HappyPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := qt.New(t)
		owned := &nativeLockFixture{}
		service := &slowLockService{delay: time.Second, held: owned}

		held, err := dblock.Acquire(c.Context(), nativeLockConnection{service}, "migration", 3*time.Second)

		c.Assert(err, qt.IsNil)
		c.Assert(held.Supported(), qt.IsTrue)
		c.Assert(owned.releases, qt.Equals, 0)
		c.Assert(held.Release(c.Context()), qt.IsNil)
	})
}

// The caller's cancellation still stops an attempt in flight, and its outcome
// stays unknown: the attempt had no answer.
func TestNativeLockCancellationDuringAnAttempt_FailurePath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		service := &slowLockService{delay: time.Hour, cancel: cancel}

		held, err := dblock.Acquire(ctx, nativeLockConnection{service}, "migration", time.Minute)

		c.Assert(err, qt.ErrorIs, context.Canceled)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
		c.Assert(held, qt.IsNil)
		c.Assert(service.calls, qt.Equals, 1)
		c.Assert(service.elapsed, qt.DeepEquals, []time.Duration{0})
	})
}
