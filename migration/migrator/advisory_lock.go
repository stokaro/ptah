package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
)

const migrationAdvisoryLockName = "ptah_migrate"
const migrationAdvisoryUnlockTimeout = 10 * time.Second
const mariaDBDefaultAdvisoryLockTimeoutSeconds = 31_536_000

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

func acquireMigrationLock(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name string,
	timeout time.Duration,
) (*migrationLock, error) {
	name = normalizeMigrationLockName(name)
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
		lock.releaseFunc = releasePostgresMigrationLock(session, name)
		acquireErr = acquirePostgresMigrationLock(ctx, session, name, timeout)
	case platform.MySQL, platform.MariaDB:
		lock.releaseFunc = releaseMySQLMigrationLock(session, name)
		acquireErr = acquireMySQLMigrationLock(ctx, session, dialect, name, timeout)
	case platform.SQLServer:
		lock.releaseFunc = releaseSQLServerMigrationLock(session, name)
		acquireErr = acquireSQLServerMigrationLock(ctx, session, name, timeout)
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

func acquirePostgresMigrationLock(ctx context.Context, conn *sql.Conn, name string, timeout time.Duration) error {
	lockCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if _, err := conn.ExecContext(lockCtx, "SELECT pg_advisory_lock($1)", postgresMigrationLockKey(name)); err != nil {
		if timeout > 0 && errors.Is(lockCtx.Err(), context.DeadlineExceeded) {
			return &MigrationLockTimeoutError{Dialect: "postgres", Name: name, Timeout: timeout}
		}
		return err
	}
	return nil
}

func releasePostgresMigrationLock(conn *sql.Conn, name string) func(context.Context) error {
	return func(ctx context.Context) error {
		var released bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", postgresMigrationLockKey(name)).Scan(&released); err != nil {
			return err
		}
		if !released {
			return fmt.Errorf("postgres migration advisory lock was not held")
		}
		return nil
	}
}

func acquireMySQLMigrationLock(ctx context.Context, conn *sql.Conn, dialect string, name string, timeout time.Duration) error {
	timeoutSeconds := mySQLMigrationLockTimeoutSeconds(dialect, timeout)

	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, timeoutSeconds).Scan(&acquired); err != nil {
		return err
	}
	if !acquired.Valid {
		return fmt.Errorf("GET_LOCK(%q) returned NULL", name)
	}
	if acquired.Int64 == 0 {
		return &MigrationLockTimeoutError{Dialect: dialect, Name: name, Timeout: timeout}
	}
	return nil
}

func releaseMySQLMigrationLock(conn *sql.Conn, name string) func(context.Context) error {
	return func(ctx context.Context) error {
		var released sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", name).Scan(&released); err != nil {
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

func acquireSQLServerMigrationLock(ctx context.Context, conn *sql.Conn, name string, timeout time.Duration) error {
	timeoutMilliseconds := sqlServerMigrationLockTimeoutMilliseconds(timeout)

	var result int
	if err := conn.QueryRowContext(ctx, `
DECLARE @result INT;
EXEC @result = sys.sp_getapplock
    @Resource = @p1,
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = @p2;
SELECT @result;`, name, timeoutMilliseconds).Scan(&result); err != nil {
		return err
	}
	if result >= 0 {
		return nil
	}
	if result == -1 {
		return &MigrationLockTimeoutError{Dialect: platform.SQLServer, Name: name, Timeout: timeout}
	}
	return fmt.Errorf("sqlserver sp_getapplock(%q) failed with return code %d", name, result)
}

func releaseSQLServerMigrationLock(conn *sql.Conn, name string) func(context.Context) error {
	return func(ctx context.Context) error {
		var result int
		if err := conn.QueryRowContext(ctx, `
DECLARE @result INT;
EXEC @result = sys.sp_releaseapplock
    @Resource = @p1,
    @LockOwner = 'Session';
SELECT @result;`, name).Scan(&result); err != nil {
			return err
		}
		if result < 0 {
			return fmt.Errorf("sqlserver sp_releaseapplock(%q) failed with return code %d", name, result)
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
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(normalizeMigrationLockName(name)))
	return int64(hash.Sum32())
}
