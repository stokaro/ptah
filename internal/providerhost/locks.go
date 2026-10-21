package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/dbschema"
)

type sessionLocker struct{ session *session }
type sessionLock struct {
	mu         sync.Mutex
	session    *session
	scope      *protocol.LockScope
	released   bool
	releaseErr error
}

func (s *session) SessionLocker() (dbschema.SessionLocker, error) {
	if !slices.Contains(s.target.engine.GetFeatures(), provider.LocksFeature) || !slices.Contains(s.target.host.description.GetFeatures(), provider.LocksFeature) {
		return nil, unsupported("session locks for the selected engine")
	}
	return sessionLocker{session: s}, nil
}

func (s *session) lockError() error {
	if s.lockLost.Load() {
		return dbschema.ErrSessionLockLost
	}
	return nil
}

func (l sessionLocker) TryLock(ctx context.Context, name string) (dbschema.SessionLock, error) {
	if err := l.session.lockError(); err != nil {
		return nil, err
	}
	if l.session.closed.Load() {
		return nil, dbschema.ErrSessionLockLost
	}
	if l.session.IsDryRun() {
		return nil, unsupported("session lock acquisition during a dry run")
	}
	scope := &protocol.LockScope{SessionId: l.session.remote.GetId(), LockId: rand.Text(), Name: name}
	_, err := l.session.target.host.client.TryLock(ctx, &protocol.LockRequest{Scope: scope, RequestId: rand.Text()})
	if err != nil {
		if errors.Is(err, provider.ErrLockBusy) && !errors.Is(err, provider.ErrOutcomeUnknown) {
			return nil, errors.Join(dbschema.ErrSessionLockBusy, err)
		}
		if errors.Is(err, provider.ErrLockLost) || errors.Is(err, provider.ErrOutcomeUnknown) {
			l.session.lockLost.Store(true)
		}
		return nil, nativeOutcomeError(err)
	}
	return &sessionLock{session: l.session, scope: scope}, nil
}

func (l *sessionLock) request() *protocol.LockRequest {
	return &protocol.LockRequest{Scope: l.scope, RequestId: rand.Text()}
}

func (l *sessionLock) CheckOwnership(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return errors.Join(dbschema.ErrSessionLockLost, l.releaseErr)
	}
	return l.check(ctx)
}

func (l *sessionLock) check(ctx context.Context) error {
	if l.session.closed.Load() || l.session.lockLost.Load() {
		return dbschema.ErrSessionLockLost
	}
	_, err := l.session.target.host.client.CheckLock(ctx, l.request())
	if err != nil {
		l.session.lockLost.Store(true)
		return errors.Join(dbschema.ErrSessionLockLost, nativeOutcomeError(err))
	}
	return nil
}

func (l *sessionLock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return l.releaseErr
	}
	// Check once more before release so a stopped host cannot turn stale
	// acquisition evidence into a successful completion. Cleanup still runs
	// after loss, and its result cannot erase the loss.
	checkErr := l.check(ctx)
	l.released = true
	_, releaseErr := l.session.target.host.client.ReleaseLock(ctx, l.request())
	if releaseErr != nil {
		l.session.lockLost.Store(true)
	}
	l.releaseErr = errors.Join(checkErr, nativeOutcomeError(releaseErr))
	return l.releaseErr
}
