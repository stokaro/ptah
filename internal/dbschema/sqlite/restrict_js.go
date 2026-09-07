//go:build js

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

// attachedDatabaseLimiter is how a js/wasm host reaches SQLITE_LIMIT_ATTACHED.
//
// Everywhere else the linked driver exposes that limit and a build-tagged
// function can call it. A js build links no driver -- see the package doc --
// so which engine is present is a property of the host rather than of the
// platform, and the call has to arrive from the host at run time.
var (
	limiterMu               sync.RWMutex
	attachedDatabaseLimiter func(context.Context, *sql.Conn) error
)

// SetAttachedDatabaseLimiter installs the host's implementation of
// "set SQLITE_LIMIT_ATTACHED to 0 on this pinned session".
//
// Install it before the first connection is opened. Until it is installed,
// RestrictSession fails: see applyAttachedDatabaseLimit.
func SetAttachedDatabaseLimiter(limit func(ctx context.Context, session *sql.Conn) error) {
	limiterMu.Lock()
	defer limiterMu.Unlock()
	attachedDatabaseLimiter = limit
}

// applyAttachedDatabaseLimit refuses rather than degrading when no host has
// installed a limiter. RestrictSession is a security boundary -- it is what
// stops SQL on an untrusted session from reaching another database file -- so
// a build that cannot establish it must say so, not proceed unrestricted.
func applyAttachedDatabaseLimit(ctx context.Context, session *sql.Conn) error {
	limiterMu.RLock()
	limit := attachedDatabaseLimiter
	limiterMu.RUnlock()
	if limit == nil {
		return errors.New(
			"no attached-database limiter installed: a js host must call " +
				"sqlite.SetAttachedDatabaseLimiter before opening a connection")
	}
	return limit(ctx, session)
}
