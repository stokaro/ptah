// Package pglitelock serializes schema changes against a local PGlite server.
// Its clients share one backend session, so PostgreSQL advisory locks are
// reentrant across those clients and cannot prove exclusion.
package pglitelock

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ErrHeld reports that the same local PGlite lock is already held.
var ErrHeld = errors.New("lock is held")

// Lock is a process-level file lock held for one PGlite database operation.
type Lock struct{ file *os.File }

// Acquire takes a file lock keyed by the PGlite cluster, database, and lock
// name. A negative timeout tries once; zero waits until ctx ends.
func Acquire(ctx context.Context, cluster, database, name string, timeout time.Duration) (*Lock, error) {
	if cluster == "" || database == "" || name == "" {
		return nil, errors.New("PGlite lock identity is incomplete")
	}
	digest := sha256.Sum256([]byte(cluster + "\x00" + database + "\x00" + name))
	path := filepath.Join(os.TempDir(), "ptah-pglite-locks", fmt.Sprintf("%x.lock", digest))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	started := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return nil, err
		}
		locked, err := tryLock(file)
		if err != nil {
			return nil, errors.Join(err, file.Close())
		}
		if locked {
			if err := ctx.Err(); err != nil {
				return nil, errors.Join(err, unlock(file), file.Close())
			}
			return &Lock{file: file}, nil
		}
		_ = file.Close()
		if timeout < 0 || timeout > 0 && time.Since(started) >= timeout {
			return nil, ErrHeld
		}
		wait := 25 * time.Millisecond
		if timeout > 0 {
			wait = min(wait, timeout-time.Since(started))
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Release unlocks the file and closes its handle.
func (lock *Lock) Release() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	file := lock.file
	lock.file = nil
	return errors.Join(unlock(file), file.Close())
}
