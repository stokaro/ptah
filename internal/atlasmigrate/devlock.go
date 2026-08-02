package atlasmigrate

import (
	"context"
	"fmt"
	"time"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/devlock"
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

func (l *devDatabaseLock) release() error {
	if l == nil {
		return nil
	}
	return l.lock.Release()
}
