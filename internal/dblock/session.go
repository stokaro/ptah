package dblock

import (
	"context"
	"errors"
	"sync"
	"time"
)

// How a lock held on a session of its own reports a loss.
//
// A PostgreSQL, MySQL, MariaDB or SQL Server advisory lock belongs to the
// session that took it, and the server releases it when that session ends.
// [Acquire] takes the lock on a session of its own and leaves the work to the
// pool, so the end of that session does not reach the work: measured on
// PostgreSQL 18 and MySQL 8.4, a migration whose lock session was ended with
// `pg_terminate_backend` or `KILL` ran its 20-second statement and the
// statements after it to the end, while another session took the lock at
// once; the run only learned of it when the release failed (`FATAL:
// terminating connection due to administrator command (SQLSTATE 57P01)` and
// `invalid connection`).
//
// So the session is watched. Every sessionCheckInterval the watch pings it,
// and a ping that fails loses the lock: database/sql never moves a *sql.Conn
// to another server session, so a session that answers is the one holding the
// lock, and one that does not answer has lost it, or will once the server
// notices the connection is gone. A holder asking whether it still holds the
// lock trusts an answer for one sessionCheckInterval and asks the session
// itself after that, so a holder that was stopped and resumes learns of a loss
// before it does anything else.
const (
	sessionCheckInterval = time.Second
	// sessionCheckTimeout bounds one ping. A ping that does not answer in
	// time loses the lock: the driver closes a connection whose request it
	// gave up on, which ends the session and the lock with it.
	sessionCheckTimeout = DefaultReleaseTimeout
)

// errSessionEnded is what releasing a lock whose session has ended answers:
// there is nothing to give back, and the connection is discarded rather than
// returned to the pool.
var errSessionEnded = errors.New("the lock's session has ended")

// sessionHold is a lock held on a session of its own, and the watch that says
// whether the session still holds it.
type sessionHold struct {
	// ping asks the session whether it is still there.
	ping func(context.Context) error
	// unlock gives the lock back on the session.
	unlock func(context.Context) error
	now    func() time.Time

	mu        sync.Mutex
	confirmed time.Time
	released  bool

	lostOnce sync.Once
	lostCh   chan struct{}

	stopOnce sync.Once
	stopCh   chan struct{}
	watching chan struct{}
}

// newSessionHold returns the hold over a lock just taken on a session, with
// its watch running.
func newSessionHold(ping, unlock func(context.Context) error, now func() time.Time) *sessionHold {
	hold := newIdleSessionHold(ping, unlock, now)
	hold.watching = make(chan struct{})
	go hold.watch()
	return hold
}

// newIdleSessionHold returns the hold with no watch running, so a check
// happens only when something asks for one.
func newIdleSessionHold(ping, unlock func(context.Context) error, now func() time.Time) *sessionHold {
	watching := make(chan struct{})
	close(watching)
	return &sessionHold{
		ping:      ping,
		unlock:    unlock,
		now:       now,
		confirmed: now(),
		lostCh:    make(chan struct{}),
		stopCh:    make(chan struct{}),
		watching:  watching,
	}
}

// lost returns a channel that is closed when the lock is lost.
func (h *sessionHold) lost() <-chan struct{} { return h.lostCh }

// held reports whether the lock may still be held: it has not been lost, and
// the session answered within the last sessionCheckInterval or answers now.
// A released lock reads as held, since it was not taken away.
func (h *sessionHold) held() bool {
	h.mu.Lock()
	released, confirmed := h.released, h.confirmed
	h.mu.Unlock()
	if released {
		return true
	}
	if h.isLost() {
		return false
	}
	if h.now().Sub(confirmed) <= sessionCheckInterval {
		return true
	}
	h.check()
	return !h.isLost()
}

func (h *sessionHold) isLost() bool {
	select {
	case <-h.lostCh:
		return true
	default:
		return false
	}
}

func (h *sessionHold) markLost() {
	h.lostOnce.Do(func() { close(h.lostCh) })
}

func (h *sessionHold) watch() {
	defer close(h.watching)
	ticker := time.NewTicker(sessionCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.check()
			if h.isLost() {
				return
			}
		}
	}
}

// check pings the session. An answer is recorded with the time the ping was
// sent, and a failure loses the lock. The ping runs on a context of its own
// rather than one a release cancels: a driver may close a connection whose
// request was canceled, which would end the session the release is about to
// give the lock back on.
func (h *sessionHold) check() {
	asked := h.now()
	ctx, cancel := context.WithTimeout(context.Background(), sessionCheckTimeout)
	defer cancel()
	if err := h.ping(ctx); err != nil {
		h.mu.Lock()
		released := h.released
		h.mu.Unlock()
		if !released {
			h.markLost()
		}
		return
	}
	h.mu.Lock()
	if asked.After(h.confirmed) {
		h.confirmed = asked
	}
	h.mu.Unlock()
}

// release stops the watch and gives the lock back. A lock already lost has
// nothing to give back, and its release answers errSessionEnded.
func (h *sessionHold) release(ctx context.Context) error {
	wasHeld := h.held()
	h.mu.Lock()
	h.released = true
	h.mu.Unlock()
	h.stopOnce.Do(func() { close(h.stopCh) })
	<-h.watching
	if !wasHeld {
		return errSessionEnded
	}
	return h.unlock(ctx)
}
