// Package sqlserver takes SQL Server's application lock for internal/dblock:
// sp_getapplock with a session owner, with the wait in milliseconds, and
// sp_releaseapplock.
package sqlserver

import (
	"context"
	"fmt"
	"math"
	"time"

	"ptah.run/core/platform"
	"ptah.run/internal/dblockext"
)

// Services returns SQL Server's lock.
func Services() dblockext.Services {
	return dblockext.Services{
		Dialects: []string{platform.SQLServer},
		Session:  appLock{},
	}
}

type appLock struct{}

// Acquire asks for an exclusive lock the session owns and lets the server
// wait for it.
func (appLock) Acquire(ctx context.Context, session dblockext.Session, request dblockext.Request) error {
	var result int
	if err := session.QueryRowContext(ctx, `
DECLARE @result INT;
EXEC @result = sys.sp_getapplock
    @Resource = @p1,
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = @p2;
SELECT @result;`, request.Name, timeoutMilliseconds(request.Timeout)).Scan(&result); err != nil {
		return &dblockext.AmbiguousError{Err: err}
	}
	if result >= 0 {
		return nil
	}
	if result == -1 {
		return request.Held
	}
	return fmt.Errorf("sqlserver sp_getapplock(%q) failed with return code %d", request.Name, result)
}

// Release gives the lock back.
func (appLock) Release(ctx context.Context, session dblockext.Session, request dblockext.Request) error {
	var result int
	if err := session.QueryRowContext(ctx, `
DECLARE @result INT;
EXEC @result = sys.sp_releaseapplock
    @Resource = @p1,
    @LockOwner = 'Session';
SELECT @result;`, request.Name).Scan(&result); err != nil {
		return err
	}
	if result < 0 {
		return fmt.Errorf("sqlserver sp_releaseapplock(%q) failed with return code %d", request.Name, result)
	}
	return nil
}

// timeoutMilliseconds is the @LockTimeout a wait of timeout sends: zero for a
// negative one, which does not wait, -1 for a zero one, which waits without
// limit, and a positive one rounded up to whole milliseconds, at most the
// largest INT.
func timeoutMilliseconds(timeout time.Duration) int {
	if timeout < 0 {
		return 0
	}
	if timeout == 0 {
		return -1
	}
	milliseconds := math.Ceil(float64(timeout) / float64(time.Millisecond))
	if milliseconds > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(milliseconds)
}
