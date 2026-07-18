package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/stokaro/ptah/dbschema"
)

const migrationAdvisoryLockName = "ptah_migrate"
const migrationAdvisoryUnlockTimeout = 10 * time.Second
const mariaDBDefaultAdvisoryLockTimeoutSeconds = 31_536_000

// postgresMigrationAdvisoryLockKey is the stable FNV-1a 64-bit hash of
// migrationAdvisoryLockName, stored as a signed value for pg_advisory_lock.
const postgresMigrationAdvisoryLockKey int64 = -7752083082818440098

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

func (m *Migrator) withMigrationLock(ctx context.Context, operation string, fn func(context.Context) error) error {
	if m.conn == nil || m.conn.Writer().IsDryRun() {
		return fn(ctx)
	}

	lock, err := acquireMigrationLock(ctx, m.conn, m.migrationLockTimeout)
	if err != nil {
		return fmt.Errorf("failed to acquire migration lock for %s: %w", operation, err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), migrationAdvisoryUnlockTimeout)
		defer cancel()
		if err := lock.release(releaseCtx); err != nil {
			m.logger.Warn("failed to release migration lock", "operation", operation, "error", err)
		}
	}()

	return fn(ctx)
}

type migrationLock struct {
	conn        *sql.Conn
	releaseFunc func(context.Context) error
}

func acquireMigrationLock(ctx context.Context, conn *dbschema.DatabaseConnection, timeout time.Duration) (*migrationLock, error) {
	switch conn.Info().Dialect {
	case "postgres", "mysql", "mariadb":
	default:
		return &migrationLock{}, nil
	}

	session, err := conn.Conn(ctx)
	if err != nil {
		return nil, err
	}

	lock := &migrationLock{conn: session}
	var acquireErr error
	switch conn.Info().Dialect {
	case "postgres":
		lock.releaseFunc = releasePostgresMigrationLock(session)
		acquireErr = acquirePostgresMigrationLock(ctx, session, timeout)
	case "mysql", "mariadb":
		lock.releaseFunc = releaseMySQLMigrationLock(session)
		acquireErr = acquireMySQLMigrationLock(ctx, session, conn.Info().Dialect, timeout)
	}
	if acquireErr != nil {
		_ = session.Close()
		return nil, acquireErr
	}
	return lock, nil
}

func (l *migrationLock) release(ctx context.Context) error {
	if l.conn == nil {
		return nil
	}
	defer l.conn.Close()
	return l.releaseFunc(ctx)
}

func acquirePostgresMigrationLock(ctx context.Context, conn *sql.Conn, timeout time.Duration) error {
	lockCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if _, err := conn.ExecContext(lockCtx, "SELECT pg_advisory_lock($1)", postgresMigrationLockKey()); err != nil {
		if timeout > 0 && errors.Is(lockCtx.Err(), context.DeadlineExceeded) {
			return &MigrationLockTimeoutError{Dialect: "postgres", Name: migrationAdvisoryLockName, Timeout: timeout}
		}
		return err
	}
	return nil
}

func releasePostgresMigrationLock(conn *sql.Conn) func(context.Context) error {
	return func(ctx context.Context) error {
		var released bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", postgresMigrationLockKey()).Scan(&released); err != nil {
			return err
		}
		if !released {
			return fmt.Errorf("postgres migration advisory lock was not held")
		}
		return nil
	}
}

func acquireMySQLMigrationLock(ctx context.Context, conn *sql.Conn, dialect string, timeout time.Duration) error {
	timeoutSeconds := mySQLMigrationLockTimeoutSeconds(dialect, timeout)

	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", migrationAdvisoryLockName, timeoutSeconds).Scan(&acquired); err != nil {
		return err
	}
	if !acquired.Valid {
		return fmt.Errorf("GET_LOCK(%q) returned NULL", migrationAdvisoryLockName)
	}
	if acquired.Int64 == 0 {
		return &MigrationLockTimeoutError{Dialect: dialect, Name: migrationAdvisoryLockName, Timeout: timeout}
	}
	return nil
}

func releaseMySQLMigrationLock(conn *sql.Conn) func(context.Context) error {
	return func(ctx context.Context) error {
		var released sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", migrationAdvisoryLockName).Scan(&released); err != nil {
			return err
		}
		if !released.Valid {
			return fmt.Errorf("mysql migration advisory lock was not held")
		}
		if released.Int64 == 0 {
			return fmt.Errorf("mysql migration advisory lock was not released")
		}
		return nil
	}
}

func mySQLMigrationLockTimeoutSeconds(dialect string, timeout time.Duration) float64 {
	if timeout > 0 {
		return math.Ceil(timeout.Seconds())
	}
	if dialect == "mariadb" {
		return mariaDBDefaultAdvisoryLockTimeoutSeconds
	}
	return -1
}

func postgresMigrationLockKey() int64 {
	return postgresMigrationAdvisoryLockKey
}
