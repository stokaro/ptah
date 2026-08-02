package migrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dblock"
)

const migrationAdvisoryLockName = "ptah_migrate"
const migrationAdvisoryUnlockTimeout = dblock.DefaultReleaseTimeout

// MigrationLockTimeoutError reports that another runner held the migration
// advisory lock longer than this migrator was configured to wait.
type MigrationLockTimeoutError struct {
	Dialect string
	Name    string
	Timeout time.Duration
}

func (e *MigrationLockTimeoutError) Error() string {
	return fmt.Sprintf("timed out acquiring migration lock %q for %s after %s", e.Name, e.Dialect, e.Timeout)
}

// IsMigrationLockTimeout reports whether err wraps a migration lock timeout.
func IsMigrationLockTimeout(err error) bool {
	var target *MigrationLockTimeoutError
	return errors.As(err, &target)
}

// ParseMigrationLockTimeout parses the session-level advisory lock timeout.
// Empty means wait indefinitely.
func ParseMigrationLockTimeout(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	duration, err := parsePositiveDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid migration lock timeout: %w", err)
	}
	return duration, nil
}

// WithMigrationLockTimeout returns a copy of the migrator that limits how long
// it waits for the session-level migration advisory lock. Zero means wait
// indefinitely.
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

func (m *Migrator) withMigrationLock(ctx context.Context, operation string, fn func(context.Context) error) error {
	if m.conn == nil || m.conn.Writer().IsDryRun() {
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
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), migrationAdvisoryUnlockTimeout)
		defer cancel()
		if err := lock.Release(releaseCtx); err != nil {
			m.logger.Warn("failed to release migration lock", "operation", operation, "error", err)
		}
	}()

	return fn(ctx)
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
	var timeoutErr *dblock.TimeoutError
	if errors.As(err, &timeoutErr) {
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
