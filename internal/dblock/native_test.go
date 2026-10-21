package dblock_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
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

func (*nativeLockFixture) Check(context.Context) error { return nil }
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
