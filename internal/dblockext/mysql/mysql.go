// Package mysql takes the named lock of MySQL and MariaDB for internal/dblock:
// GET_LOCK, with the wait in whole seconds, and RELEASE_LOCK.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"ptah.run/core/platform"
	"ptah.run/internal/dblockext"
)

// MySQL returns MySQL's lock. GET_LOCK reads a negative timeout as a wait
// without limit.
func MySQL() dblockext.Services {
	return dblockext.Services{
		Dialects: []string{platform.MySQL},
		Session:  getLock{},
	}
}

// MariaDB returns MariaDB's lock. MariaDB refuses GET_LOCK's negative
// timeout, so a wait without limit sends one year, in seconds.
func MariaDB() dblockext.Services {
	return dblockext.Services{
		Dialects: []string{platform.MariaDB},
		Session:  getLock{unboundedWaitSeconds: 31_536_000},
	}
}

// getLock is GET_LOCK on one engine.
type getLock struct {
	// unboundedWaitSeconds is the timeout a wait without limit sends, or zero
	// where the engine reads GET_LOCK's own negative spelling of it.
	unboundedWaitSeconds int64
}

// Acquire asks for the lock once and lets the server wait for it.
func (g getLock) Acquire(ctx context.Context, session dblockext.Session, request dblockext.Request) error {
	var acquired sql.NullInt64
	if err := session.QueryRowContext(
		ctx, "SELECT GET_LOCK(?, ?)", request.Name, g.timeoutSeconds(request.Timeout),
	).Scan(&acquired); err != nil {
		return &dblockext.AmbiguousError{Err: err}
	}
	if !acquired.Valid {
		return fmt.Errorf("GET_LOCK(%q) returned NULL", request.Name)
	}
	if acquired.Int64 == 0 {
		return request.Held
	}
	return nil
}

// Release gives the lock back, and fails when the session did not hold it.
func (getLock) Release(ctx context.Context, session dblockext.Session, request dblockext.Request) error {
	var released sql.NullInt64
	if err := session.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", request.Name).Scan(&released); err != nil {
		return err
	}
	if !released.Valid {
		return errors.New("mysql advisory lock was not held")
	}
	if released.Int64 == 0 {
		return errors.New("mysql advisory lock was not released")
	}
	return nil
}

// timeoutSeconds is the GET_LOCK timeout a wait of timeout sends: a positive
// one rounded up to whole seconds, zero for a negative one, which does not
// wait, and for a zero one, which waits without limit, the engine's unbounded
// wait, or GET_LOCK's own -1 where the engine takes it.
func (g getLock) timeoutSeconds(timeout time.Duration) float64 {
	if timeout > 0 {
		return math.Ceil(timeout.Seconds())
	}
	if timeout < 0 {
		return 0
	}
	if g.unboundedWaitSeconds > 0 {
		return float64(g.unboundedWaitSeconds)
	}
	return -1
}
