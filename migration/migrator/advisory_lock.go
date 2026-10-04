package migrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
)

const migrationAdvisoryLockName = "ptah_migrate"
const migrationAdvisoryUnlockTimeout = dblock.DefaultReleaseTimeout

// MigrationLockTimeoutError reports that another runner held the migration
// advisory lock longer than this migrator was configured to wait, or held it
// at all when the migrator was configured not to wait. Timeout is the
// configured value, negative in the second case.
type MigrationLockTimeoutError struct {
	Dialect string
	Name    string
	Timeout time.Duration
}

func (e *MigrationLockTimeoutError) Error() string {
	if e.Timeout < 0 {
		return fmt.Sprintf("migration lock %q for %s is held by another runner", e.Name, e.Dialect)
	}
	return fmt.Sprintf("timed out acquiring migration lock %q for %s after %s", e.Name, e.Dialect, e.Timeout)
}

// IsMigrationLockTimeout reports whether err wraps a migration lock timeout.
func IsMigrationLockTimeout(err error) bool {
	var target *MigrationLockTimeoutError
	return errors.As(err, &target)
}

// WithMigrationLockTimeout returns a copy of the migrator that limits how long
// it waits for the session-level migration advisory lock. Zero means wait
// indefinitely. A negative timeout means do not wait: the lock is tried once,
// and a lock another runner holds is a [MigrationLockTimeoutError] at once.
func (m *Migrator) WithMigrationLockTimeout(timeout time.Duration) *Migrator {
	tmp := *m
	tmp.migrationLockTimeout = timeout
	return &tmp
}

// WithMigrationLockName returns a copy of the migrator that uses name for the
// session-level migration advisory lock. Empty or whitespace-only names keep
// the default lock name.
func (m *Migrator) WithMigrationLockName(name string) *Migrator {
	tmp := *m
	tmp.migrationLockName = normalizeMigrationLockName(name)
	return &tmp
}

// WithoutMigrationLock returns a copy of the migrator that runs without the
// session-level migration advisory lock: no lock is requested, no wait is
// observed, and concurrent runners are not serialized against each other.
//
// This is a deliberate loss of safety, not a capability decision like the
// no-op lock returned on dialects that have no advisory locks — there the
// database cannot serialize, here the caller has asked not to. Callers must
// surface the choice; the migrator does not warn on its own.
func (m *Migrator) WithoutMigrationLock() *Migrator {
	tmp := *m
	tmp.migrationLockSkipped = true
	return &tmp
}

// MigrationLockName returns the advisory lock name this migrator acquires,
// after the trimming and defaulting [Migrator.WithMigrationLockName] applies.
// It reports the name even when locking is skipped, so a caller can name the
// lock it chose not to take.
func (m *Migrator) MigrationLockName() string {
	return m.effectiveMigrationLockName()
}

// MigrationLockSkipped reports whether [Migrator.WithoutMigrationLock] turned
// the session-level migration advisory lock off.
func (m *Migrator) MigrationLockSkipped() bool {
	return m != nil && m.migrationLockSkipped
}

func (m *Migrator) withMigrationLock(ctx context.Context, operation string, fn func(context.Context) error) error {
	if m.conn == nil || m.conn.Writer().IsDryRun() || m.migrationLockSkipped {
		return fn(ctx)
	}

	dialect := m.conn.Info().Dialect
	lockName := m.effectiveMigrationLockName()
	startedAt := time.Now()
	observer := m.migrationObserver()
	lockCtx, span := observer.StartSpan(ctx, "ptah.lock.acquire",
		attr("db.system", dialect),
		attr("migration.operation", operation),
		attr("lock.name", lockName),
		attr("lock.timeout_ms", m.migrationLockTimeout.Milliseconds()),
	)
	lock, err := acquireMigrationLock(ctx, m.conn, lockName, m.migrationLockTimeout)
	wait := time.Since(startedAt)
	span.SetAttributes(attr("lock.wait_ms", wait.Milliseconds()))
	span.End(err)
	if root := rootSpanFromContext(ctx); root != nil {
		root.SetAttributes(attr("lock.wait_ms", wait.Milliseconds()))
	}
	observer.RecordDuration(lockCtx, "ptah_migration_lock_wait_seconds", wait,
		attr("db.system", dialect),
		attr("migration.operation", operation),
	)
	if err != nil {
		return fmt.Errorf("failed to acquire migration lock for %s: %w", operation, err)
	}

	guarded, stopGuard := lock.Guard(ctx)
	runErr := fn(withHeldMigrationLock(guarded, lock))
	// Read before the release, which is not a loss and hides one.
	lostErr := lock.Err()
	stopGuard()
	releaseCtx, cancel := context.WithTimeout(context.Background(), migrationAdvisoryUnlockTimeout)
	defer cancel()
	releaseErr := lock.Release(releaseCtx)
	return migrationLockOutcome(operation, runErr, lostErr, releaseErr)
}

// migrationLockOutcome is the error of a run that held the migration lock.
//
// A lost lock is the run's failure whatever else happened: another runner may
// have taken the lock and gone on, so a run that finished its own statements
// did not finish them alone. A failed release is the run's failure too, since
// a lock the server may still hold keeps every other runner waiting until the
// session times out.
func migrationLockOutcome(operation string, runErr, lostErr, releaseErr error) error {
	if lostErr != nil {
		lostErr = fmt.Errorf("migration lock for %s: %w; the run stopped there, and the revision table "+
			"records what it committed", operation, lostErr)
		if runErr != nil && !dblock.IsLost(runErr) {
			lostErr = fmt.Errorf("%w (the run reported: %v)", lostErr, runErr)
		}
		runErr = lostErr
	}
	switch {
	case releaseErr == nil:
		return runErr
	case runErr == nil:
		return fmt.Errorf("failed to release migration lock for %s: %w", operation, releaseErr)
	default:
		return fmt.Errorf("%w; additionally failed to release migration lock: %v", runErr, releaseErr)
	}
}

// heldMigrationLockKey stores the migration lock a run holds in its context.
type heldMigrationLockKey struct{}

// heldLock is what the run asks of the migration lock it holds.
type heldLock interface {
	Err() error
}

func withHeldMigrationLock(ctx context.Context, lock heldLock) context.Context {
	return context.WithValue(ctx, heldMigrationLockKey{}, lock)
}

// migrationLockLost returns an error once the migration lock the run holds
// has been taken away, and nil while it is held or when the run holds none.
// The run asks before each statement it runs and before each commit, so a
// run that lost its lock starts nothing more and commits nothing more.
func migrationLockLost(ctx context.Context) error {
	lock, ok := ctx.Value(heldMigrationLockKey{}).(heldLock)
	if !ok {
		return nil
	}
	return lock.Err()
}

// acquireMigrationLock takes the shared session advisory lock through
// internal/dblock and converts its timeout error into the migrator's typed
// [MigrationLockTimeoutError], preserving the historical error text.
func acquireMigrationLock(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name string,
	timeout time.Duration,
) (*dblock.Lock, error) {
	lock, err := dblock.Acquire(ctx, conn, normalizeMigrationLockName(name), timeout)
	if timeoutErr, ok := errors.AsType[*dblock.TimeoutError](err); ok {
		return nil, &MigrationLockTimeoutError{
			Dialect: timeoutErr.Dialect,
			Name:    timeoutErr.Name,
			Timeout: timeoutErr.Timeout,
		}
	}
	return lock, err
}

func (m *Migrator) effectiveMigrationLockName() string {
	return normalizeMigrationLockName(m.migrationLockName)
}

func normalizeMigrationLockName(name string) string {
	if trimmed := strings.TrimSpace(name); trimmed != "" {
		return trimmed
	}
	return migrationAdvisoryLockName
}

func postgresMigrationLockKey(name string) int64 {
	return dblock.PostgresKey(normalizeMigrationLockName(name))
}
