package atlasmigrate

import (
	"context"
	"fmt"
	"time"

	"ptah.run/dbschema"
	"ptah.run/internal/devlock"
)

type devDatabaseLock struct {
	lock *devlock.Lock
}

func acquireDevDatabaseLock(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	timeout time.Duration,
) (*devDatabaseLock, error) {
	lock, err := devlock.Acquire(ctx, conn, timeout)
	if err != nil {
		return nil, fmt.Errorf("acquire migrate diff dev database lock: %w", err)
	}
	return &devDatabaseLock{lock: lock}, nil
}

// guard returns a context that ends when the dev database's lock is lost,
// and settle, which returns the error of the work that ran under it; see
// [devlock.Lock.Guard].
func (l *devDatabaseLock) guard(ctx context.Context) (context.Context, func(error) error) {
	return l.lock.Guard(ctx)
}

func (l *devDatabaseLock) release() error {
	if l == nil {
		return nil
	}
	return l.lock.Release()
}
