package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/stokaro/ptah/core/platform"
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

	dialect := m.conn.Info().Dialect
	startedAt := time.Now()
	observer := m.migrationObserver()
	lockCtx, span := observer.StartSpan(ctx, "ptah.lock.acquire",
		attr("db.system", dialect),
		attr("migration.operation", operation),
		attr("lock.name", migrationAdvisoryLockName),
		attr("lock.timeout_ms", m.migrationLockTimeout.Milliseconds()),
	)
	lock, err := acquireMigrationLock(ctx, m.conn, m.migrationLockTimeout)
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
	dialect := conn.Info().Dialect
	switch dialect {
	case platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLServer:
	default:
		return &migrationLock{}, nil
	}

	session, err := conn.Conn(ctx)
	if err != nil {
		return nil, err
	}

	lock := &migrationLock{conn: session}
	var acquireErr error
	switch dialect {
	case platform.Postgres:
		lock.releaseFunc = releasePostgresMigrationLock(session)
		acquireErr = acquirePostgresMigrationLock(ctx, session, timeout)
	case platform.MySQL, platform.MariaDB:
		lock.releaseFunc = releaseMySQLMigrationLock(session)
		acquireErr = acquireMySQLMigrationLock(ctx, session, dialect, timeout)
	case platform.SQLServer:
		lock.releaseFunc = releaseSQLServerMigrationLock(session)
		acquireErr = acquireSQLServerMigrationLock(ctx, session, timeout)
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

func acquireSQLServerMigrationLock(ctx context.Context, conn *sql.Conn, timeout time.Duration) error {
	timeoutMilliseconds := sqlServerMigrationLockTimeoutMilliseconds(timeout)

	var result int
	if err := conn.QueryRowContext(ctx, `
DECLARE @result INT;
EXEC @result = sys.sp_getapplock
    @Resource = @p1,
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = @p2;
SELECT @result;`, migrationAdvisoryLockName, timeoutMilliseconds).Scan(&result); err != nil {
		return err
	}
	if result >= 0 {
		return nil
	}
	if result == -1 {
		return &MigrationLockTimeoutError{Dialect: platform.SQLServer, Name: migrationAdvisoryLockName, Timeout: timeout}
	}
	return fmt.Errorf("sqlserver sp_getapplock(%q) failed with return code %d", migrationAdvisoryLockName, result)
}

func releaseSQLServerMigrationLock(conn *sql.Conn) func(context.Context) error {
	return func(ctx context.Context) error {
		var result int
		if err := conn.QueryRowContext(ctx, `
DECLARE @result INT;
EXEC @result = sys.sp_releaseapplock
    @Resource = @p1,
    @LockOwner = 'Session';
SELECT @result;`, migrationAdvisoryLockName).Scan(&result); err != nil {
			return err
		}
		if result < 0 {
			return fmt.Errorf("sqlserver sp_releaseapplock(%q) failed with return code %d", migrationAdvisoryLockName, result)
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

func sqlServerMigrationLockTimeoutMilliseconds(timeout time.Duration) int {
	if timeout <= 0 {
		return -1
	}
	milliseconds := math.Ceil(float64(timeout) / float64(time.Millisecond))
	if milliseconds > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(milliseconds)
}

func postgresMigrationLockKey() int64 {
	return postgresMigrationAdvisoryLockKey
}
