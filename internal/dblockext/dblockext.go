// Package dblockext declares the lock service an engine gives internal/dblock:
// how the engine takes and gives back the session-scoped lock that serializes
// schema-changing runs.
//
// dblock owns what every lock shares: the session the lock lives on, the
// watch that reports a lost lock, the timeout and loss errors, the opt-out
// from an exclusion check, and what happens to a session after a failed
// acquisition. A service supplies only the engine's own statements or SDK
// calls. dblock looks an engine's [Services] up by dialect name, and a
// dialect without services takes a no-op lock.
//
// The built-in engines' services live in the packages below this one, and
// internal/dblockext/builtin lists them. This package names no engine.
package dblockext

import (
	"context"
	"database/sql"
	"time"

	"ptah.run/core/dataext"
)

// Request is one acquisition of a lock.
type Request struct {
	// Dialect is the canonical name of the dialect the lock is taken on.
	Dialect string
	// Name is the lock's name, trimmed and never empty.
	Name string
	// Timeout bounds the wait for a lock another session holds. A positive
	// timeout waits that long, zero waits until the context ends, and a
	// negative one does not wait.
	Timeout time.Duration
	// Preview reports a run that writes nothing, such as a dry run. A lock
	// whose acquisition would write something, such as the object the lock
	// lives on, is not taken for it.
	Preview bool
	// Held is the error to return when another session holds the lock for the
	// whole wait, or holds it at all under a negative Timeout. dblock reports
	// it as a timeout, so a service returns it, wrapped or joined, and not an
	// error of its own.
	Held error
}

// Session is the database session a [SessionLock] is taken and given back
// on. SQLSession and a pinned *dbschema.DatabaseConnection satisfy it.
type Session interface {
	QueryRowContext(ctx context.Context, query string, args ...any) dataext.Row
}

// Pool hands out database sessions of their own, which the caller closes.
// *sql.DB and *dbschema.DatabaseConnection satisfy it.
type Pool interface {
	Conn(ctx context.Context) (*sql.Conn, error)
}

// SessionLock is a lock that the database session which takes it holds, and
// that the server releases when that session ends, such as an advisory lock.
type SessionLock interface {
	// Acquire takes the lock on session. It returns request.Held when
	// another session holds the lock, and an [AmbiguousError] when session
	// may hold the lock after the failure, such as when the answer to the
	// acquiring statement was lost. Every other error leaves session without
	// the lock.
	Acquire(ctx context.Context, session Session, request Request) error
	// Release gives the lock back on the session that took it.
	Release(ctx context.Context, session Session, request Request) error
}

// ExclusionCheck proves that a [SessionLock] just taken keeps other sessions
// out. A lock held by a session can exclude nobody when something between the
// client and the server, such as a transaction-pooling proxy, hands two
// clients one server session.
type ExclusionCheck interface {
	// Excludes asks a second session from pool for the lock request names,
	// which a session has just taken. When the second session takes it too,
	// Excludes gives that claim back and returns an error that says the
	// lock excludes nobody. It returns nil when the second session does not
	// get the lock, and when it cannot tell.
	Excludes(ctx context.Context, pool Pool, request Request) error
}

// SeparateLock is a lock held by a session that the service opens itself,
// apart from every database session of the pool, such as a semaphore on a
// coordination node.
type SeparateLock interface {
	// Acquire takes the lock through pool. It returns request.Held when
	// another session holds the lock. It returns a nil Hold and a nil error
	// when request.Preview is set and taking the lock would write something.
	Acquire(ctx context.Context, pool Pool, request Request) (Hold, error)
}

// Hold is a lock a [SeparateLock] took.
type Hold interface {
	// Lost returns a channel that is closed when the server takes the lock
	// away from its holder.
	Lost() <-chan struct{}
	// Held reports whether the lock may still be held: it was not lost, and
	// the server confirmed it recently enough. A released lock reads as
	// held, since nothing took it away.
	Held() bool
	// Release gives the lock back and ends the session that held it.
	Release(ctx context.Context) error
}

// AmbiguousError reports an acquisition that failed in a way that leaves the
// session's hold on the lock unknown. dblock discards such a session rather
// than returning it to the pool, where it could keep the lock.
type AmbiguousError struct {
	// Err is the failure.
	Err error
}

func (e *AmbiguousError) Error() string {
	return e.Err.Error()
}

func (e *AmbiguousError) Unwrap() error {
	return e.Err
}

// Services is the lock one engine offers. An engine sets Session or
// Separate, and never both. The zero Services offer no lock.
type Services struct {
	// Dialects are the canonical names of the dialects these services serve.
	Dialects []string
	// Session takes a lock that a database session holds.
	Session SessionLock
	// Exclusion proves that a Session lock excludes other sessions. Nil
	// proves nothing.
	Exclusion ExclusionCheck
	// Separate takes a lock that a session of the service's own holds.
	Separate SeparateLock
}

// Locks reports whether the services take a lock.
func (s Services) Locks() bool {
	return s.Session != nil || s.Separate != nil
}

// SQLSession adapts a database/sql connection to the lock query contract.
// It retains the caller's ownership of the physical connection.
type SQLSession struct{ *sql.Conn }

// QueryRowContext returns the SQL driver's row through the shared query contract.
func (s SQLSession) QueryRowContext(ctx context.Context, query string, args ...any) dataext.Row {
	return s.Conn.QueryRowContext(ctx, query, args...)
}
