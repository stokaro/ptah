// Package dblock acquires dialect-aware, session-scoped database advisory
// locks used to serialize schema-mutating operations such as migration runs
// and Atlas schema apply. Dialects without advisory-lock semantics acquire an
// explicit no-op lock, so callers make a capability decision through
// [Lock.Supported] instead of failing or silently diverging per dialect.
package dblock

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

// mariaDBDefaultTimeoutSeconds stands in for an infinite GET_LOCK wait on
// MariaDB, which rejects the negative timeout MySQL uses for "wait forever".
const mariaDBDefaultTimeoutSeconds = 31_536_000

// DefaultReleaseTimeout bounds the deferred release of an advisory lock so a
// canceled command still returns its dedicated session connection promptly.
const DefaultReleaseTimeout = 10 * time.Second

// TimeoutError reports that another session held the advisory lock longer
// than the caller was configured to wait.
type TimeoutError struct {
	Dialect string
	Name    string
	Timeout time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("timed out acquiring advisory lock %q on %s after %s", e.Name, e.Dialect, e.Timeout)
}

// IsTimeout reports whether err wraps an advisory lock timeout.
func IsTimeout(err error) bool {
	var target *TimeoutError
	return errors.As(err, &target)
}

// Supported reports whether dialect has session-scoped advisory-lock
// semantics. [Acquire] returns a no-op lock on unsupported dialects.
func Supported(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLServer:
		return true
	default:
		return false
	}
}

// Lock is a held session-scoped advisory lock. The zero value is the no-op
// lock acquired on dialects without advisory-lock semantics; releasing it
// never touches the database.
type Lock struct {
	conn    *sql.Conn
	release func(context.Context) error
}

// Supported reports whether the lock is backed by a real database lock, as
// opposed to the no-op lock acquired on dialects without advisory locks.
func (l *Lock) Supported() bool {
	return l != nil && l.conn != nil
}

// Release releases the advisory lock and closes its dedicated session
// connection. Releasing a nil or no-op lock is a no-op.
func (l *Lock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	defer l.conn.Close()
	return l.release(ctx)
}

// Acquire takes the dialect-specific session advisory lock named name on a
// dedicated connection from conn's pool. A zero timeout waits indefinitely;
// context cancellation always interrupts the wait. Elapsed timeouts surface
// as a [TimeoutError]. On dialects without advisory-lock semantics Acquire
// returns a no-op lock, so callers decide whether that is acceptable through
// [Lock.Supported].
func Acquire(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name string,
	timeout time.Duration,
) (*Lock, error) {
	if conn == nil {
		return nil, errors.New("advisory locking requires database connection")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("advisory lock name must not be empty")
	}
	dialect := conn.Info().Dialect
	if !Supported(dialect) {
		return &Lock{}, nil
	}

	session, err := conn.Conn(ctx)
	if err != nil {
		return nil, err
	}

	lock := &Lock{conn: session}
	var acquireErr error
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres:
		lock.release = releasePostgresLock(session, name)
		acquireErr = acquirePostgresLock(ctx, session, name, timeout)
	case platform.MySQL, platform.MariaDB:
		lock.release = releaseMySQLLock(session, name)
		acquireErr = acquireMySQLLock(ctx, session, dialect, name, timeout)
	case platform.SQLServer:
		lock.release = releaseSQLServerLock(session, name)
		acquireErr = acquireSQLServerLock(ctx, session, name, timeout)
	}
	if acquireErr != nil {
		_ = session.Close()
		return nil, acquireErr
	}
	return lock, nil
}

// PostgresKey returns the 64-bit pg_advisory_lock key derived from name.
func PostgresKey(name string) int64 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(name))
	return int64(hash.Sum32())
}

func acquirePostgresLock(ctx context.Context, conn *sql.Conn, name string, timeout time.Duration) error {
	lockCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if _, err := conn.ExecContext(lockCtx, "SELECT pg_advisory_lock($1)", PostgresKey(name)); err != nil {
		if timeout > 0 && errors.Is(lockCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return &TimeoutError{Dialect: platform.Postgres, Name: name, Timeout: timeout}
		}
		return err
	}
	return nil
}

func releasePostgresLock(conn *sql.Conn, name string) func(context.Context) error {
	return func(ctx context.Context) error {
		var released bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", PostgresKey(name)).Scan(&released); err != nil {
			return err
		}
		if !released {
			return fmt.Errorf("postgres advisory lock was not held")
		}
		return nil
	}
}

func acquireMySQLLock(ctx context.Context, conn *sql.Conn, dialect string, name string, timeout time.Duration) error {
	timeoutSeconds := mySQLLockTimeoutSeconds(dialect, timeout)

	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, timeoutSeconds).Scan(&acquired); err != nil {
		return err
	}
	if !acquired.Valid {
		return fmt.Errorf("GET_LOCK(%q) returned NULL", name)
	}
	if acquired.Int64 == 0 {
		return &TimeoutError{Dialect: dialect, Name: name, Timeout: timeout}
	}
	return nil
}

func releaseMySQLLock(conn *sql.Conn, name string) func(context.Context) error {
	return func(ctx context.Context) error {
		var released sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", name).Scan(&released); err != nil {
			return err
		}
		if !released.Valid {
			return fmt.Errorf("mysql advisory lock was not held")
		}
		if released.Int64 == 0 {
			return fmt.Errorf("mysql advisory lock was not released")
		}
		return nil
	}
}

func acquireSQLServerLock(ctx context.Context, conn *sql.Conn, name string, timeout time.Duration) error {
	timeoutMilliseconds := sqlServerLockTimeoutMilliseconds(timeout)

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
		return &TimeoutError{Dialect: platform.SQLServer, Name: name, Timeout: timeout}
	}
	return fmt.Errorf("sqlserver sp_getapplock(%q) failed with return code %d", name, result)
}

func releaseSQLServerLock(conn *sql.Conn, name string) func(context.Context) error {
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

func mySQLLockTimeoutSeconds(dialect string, timeout time.Duration) float64 {
	if timeout > 0 {
		return math.Ceil(timeout.Seconds())
	}
	if platform.NormalizeDialect(dialect) == platform.MariaDB {
		return mariaDBDefaultTimeoutSeconds
	}
	return -1
}

func sqlServerLockTimeoutMilliseconds(timeout time.Duration) int {
	if timeout <= 0 {
		return -1
	}
	milliseconds := math.Ceil(float64(timeout) / float64(time.Millisecond))
	if milliseconds > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(milliseconds)
}
