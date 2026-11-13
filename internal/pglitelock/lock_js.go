//go:build js

// Package pglitelock serializes operations on the in-process browser PGlite.
package pglitelock

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrHeld reports that the same PGlite lock is already held.
var ErrHeld = errors.New("lock is held")

var held = struct {
	sync.Mutex
	keys map[string]bool
}{keys: make(map[string]bool)}

// Lock holds one in-process PGlite operation lock.
type Lock struct{ key string }

// Acquire serializes operations in one browser runtime. Each browser PGlite
// instance lives in its own runtime, so no filesystem lock is available or
// needed there.
func Acquire(ctx context.Context, cluster, database, name string, timeout time.Duration) (*Lock, error) {
	if cluster == "" || database == "" || name == "" {
		return nil, errors.New("PGlite lock identity is incomplete")
	}
	key := cluster + "\x00" + database + "\x00" + name
	started := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		held.Lock()
		if !held.keys[key] {
			held.keys[key] = true
			held.Unlock()
			lock := &Lock{key: key}
			if err := ctx.Err(); err != nil {
				_ = lock.Release()
				return nil, err
			}
			return lock, nil
		}
		held.Unlock()
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

// Release unlocks the operation. Repeating it has no effect.
func (lock *Lock) Release() error {
	if lock == nil {
		return nil
	}
	held.Lock()
	defer held.Unlock()
	delete(held.keys, lock.key)
	lock.key = ""
	return nil
}
