package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dblock"
)

// ApplyLockName is the session advisory lock name that serializes concurrent
// Atlas schema apply runs against one target database. It is deliberately
// distinct from the migrator's "ptah_migrate" lock: declarative applies and
// versioned migration runs are separate workflows with separate lock scopes.
const ApplyLockName = "ptah_schema_apply"

// ParseApplyLockTimeout parses the Atlas `--lock-timeout` flag for schema
// apply. Empty means wait indefinitely, matching `atlas migrate apply`
// handling in Ptah; explicit values must be positive Go durations.
func ParseApplyLockTimeout(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid --lock-timeout: %w", err)
	}
	if duration <= 0 {
		return 0, errors.New("invalid --lock-timeout: must be greater than zero")
	}
	return duration, nil
}

// IsLockTimeout reports whether err wraps a schema apply lock timeout.
func IsLockTimeout(err error) bool {
	return dblock.IsTimeout(err)
}

// ApplyLock is a held schema apply lock. On dialects without advisory-lock
// semantics it is an explicit no-op reported by [ApplyLock.Supported], so the
// caller can surface the capability decision instead of failing.
type ApplyLock struct {
	lock *dblock.Lock
}

// AcquireApplyLock takes the dialect-specific session advisory lock that
// serializes schema apply runs on conn's database. It must be held before the
// target inspection and planning that the apply serializes. A zero timeout
// waits indefinitely; context cancellation always interrupts the wait, and an
// elapsed timeout surfaces as a wrapped [dblock.TimeoutError] recognized by
// [IsLockTimeout].
//
// An empty or whitespace-only name selects [ApplyLockName]. Naming the lock is
// how a caller coordinates with a different tool on the same database: two
// runners serialize only when they name the same lock, so passing a name is
// also how a caller opts OUT of serializing against Ptah's default.
func AcquireApplyLock(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name string,
	timeout time.Duration,
) (*ApplyLock, error) {
	if conn == nil {
		return nil, errors.New("schema apply locking requires database connection")
	}
	lock, err := dblock.Acquire(ctx, conn, EffectiveApplyLockName(name), timeout)
	if err != nil {
		return nil, fmt.Errorf("acquire schema apply lock: %w", err)
	}
	return &ApplyLock{lock: lock}, nil
}

// EffectiveApplyLockName resolves the schema apply lock name a request selects:
// the trimmed name, or [ApplyLockName] when none was given.
func EffectiveApplyLockName(name string) string {
	if trimmed := strings.TrimSpace(name); trimmed != "" {
		return trimmed
	}
	return ApplyLockName
}

// Supported reports whether the lock is backed by a real database lock. It is
// false on dialects without advisory-lock semantics (SQLite, ClickHouse,
// CockroachDB, YugabyteDB, and Spanner), where the apply proceeds unlocked.
func (l *ApplyLock) Supported() bool {
	return l != nil && l.lock.Supported()
}

// Name returns the advisory lock name this lock was acquired under. It is
// recorded on the no-op path too, so a caller on a dialect without advisory
// locks can still name the lock it would have taken. A nil lock — which is
// what a skipped acquisition leaves behind — reports the empty string.
func (l *ApplyLock) Name() string {
	if l == nil {
		return ""
	}
	return l.lock.Name()
}

// Release frees the schema apply lock on its own bounded background context,
// so the lock is released on every exit path including cancellation, when the
// command context is already dead. Releasing a nil lock is a no-op.
func (l *ApplyLock) Release() error {
	if l == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), dblock.DefaultReleaseTimeout)
	defer cancel()
	return l.lock.Release(ctx)
}
