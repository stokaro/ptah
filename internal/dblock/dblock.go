// Package dblock acquires dialect-aware, session-scoped database advisory
// locks used to serialize schema-mutating operations such as migration runs
// and Atlas schema apply. Dialects without advisory-lock semantics acquire an
// explicit no-op lock, so callers make a capability decision through
// [Lock.Supported] instead of failing or silently diverging per dialect.
package dblock

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/envbool"
)

const (
	// mariaDBDefaultTimeoutSeconds stands in for an infinite GET_LOCK wait on
	// MariaDB, which rejects the negative timeout MySQL uses for "wait forever".
	mariaDBDefaultTimeoutSeconds = 31_536_000
	postgresLockPollInterval     = 25 * time.Millisecond
)

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
	case platform.Postgres,
		platform.YugabyteDB,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLServer:
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
	name    string
	release func(context.Context) error
}

type ambiguousAcquisitionError struct {
	err error
}

func (e *ambiguousAcquisitionError) Error() string {
	return e.err.Error()
}

func (e *ambiguousAcquisitionError) Unwrap() error {
	return e.err
}

// Supported reports whether the lock is backed by a real database lock, as
// opposed to the no-op lock acquired on dialects without advisory locks.
func (l *Lock) Supported() bool {
	return l != nil && l.conn != nil
}

// Name returns the advisory lock name this lock was acquired under, after the
// trimming [Acquire] applies. It is recorded on the no-op path too, so a
// caller on a dialect without advisory locks can still report which lock it
// would have taken. A nil lock reports the empty string.
//
// This is the value the dialect-specific acquisition actually used: the
// PostgreSQL-family key is [PostgresKey] of it, and MySQL, MariaDB and
// SQL Server pass it to the server verbatim.
func (l *Lock) Name() string {
	if l == nil {
		return ""
	}
	return l.name
}

// Release releases the advisory lock and closes its dedicated session
// connection. Releasing a nil or no-op lock is a no-op.
func (l *Lock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	conn := l.conn
	l.conn = nil
	releaseErr := l.release(ctx)
	if releaseErr != nil {
		return errors.Join(releaseErr, discardConnection(conn))
	}
	return conn.Close()
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
	dialect := platform.NormalizeDialect(conn.Info().Dialect)
	if !Supported(dialect) {
		return &Lock{name: name}, nil
	}

	session, err := conn.Conn(ctx)
	if err != nil {
		return nil, err
	}

	lock := &Lock{conn: session, name: name}
	var acquireErr error
	switch dialect {
	case platform.Postgres, platform.YugabyteDB:
		lock.release = releasePostgresLock(session, dialect, name)
		acquireErr = acquirePostgresLock(ctx, session, dialect, name, timeout)
		if acquireErr == nil {
			acquireErr = verifyPostgresLockExcludes(ctx, conn, dialect, name)
		}
	case platform.MySQL, platform.MariaDB:
		lock.release = releaseMySQLLock(session, name)
		acquireErr = acquireMySQLLock(ctx, session, dialect, name, timeout)
	case platform.SQLServer:
		lock.release = releaseSQLServerLock(session, name)
		acquireErr = acquireSQLServerLock(ctx, session, name, timeout)
	}
	if acquireErr != nil {
		return nil, closeAfterFailedAcquisition(session, acquireErr)
	}
	return lock, nil
}

// AllowUnverifiedLock lets an operator run without the exclusion check below.
//
// It is an opt-out from a refusal, not from the lock: the lock is still taken,
// and what is skipped is the proof that it excludes anybody. Set it only where
// the topology is known and the risk is accepted -- two runs against a
// transaction pooler will both believe they hold it.
var AllowUnverifiedLock = envbool.New("PTAH_ALLOW_UNVERIFIED_MIGRATION_LOCK", false, envbool.Gated)

// connOpener is the part of a pool this check needs: a second connection.
//
// An interface rather than the connection type, so the check can be exercised
// against a fake server. Both *sql.DB and the live connection satisfy it.
type connOpener interface {
	Conn(ctx context.Context) (*sql.Conn, error)
}

// verifyPostgresLockExcludes proves the lock just taken actually excludes
// somebody else, and refuses the run when it does not.
//
// A PostgreSQL advisory lock is SESSION-scoped, and a transaction pooler hands
// a client whichever backend is free between transactions. So a second client
// can be handed the SAME backend the first one locked on, where the lock is
// reentrant and answers true again. Measured through PgBouncer 1.25.2 in
// transaction mode, two independent client connections and the same key:
//
//	first handle         acquired=true  backend=113
//	second handle        acquired=true  backend=113
//	first handle again   acquired=true  backend=113
//
// Every attempt succeeded and every one landed on backend 113. So the lock is
// not merely weakened there -- it excludes nothing at all, and two concurrent
// migration runs would both believe they held it. It fails OPEN, which is why
// this is a refusal rather than a warning (stokaro/ptah#1029).
//
// The check asks the property rather than identifying the proxy. Ptah does not
// need to know what sits in front of the database; it needs to know whether the
// lock it just took keeps anybody out, and that is one query from a second
// connection. On a direct server the second attempt answers false and the check
// costs a round trip.
func verifyPostgresLockExcludes(
	ctx context.Context,
	pool connOpener,
	dialect, name string,
) error {
	allowed, err := AllowUnverifiedLock.Resolve()
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}

	witness, err := pool.Conn(ctx)
	if err != nil {
		// A pool that cannot hand out a second connection cannot answer the
		// question. Refusing on that would fail every single-connection
		// topology, so the lock stands and the caller is not told a story
		// about it.
		return nil
	}
	defer func() { _ = witness.Close() }()

	var acquiredAgain bool
	if err := witness.QueryRowContext(
		ctx,
		"SELECT pg_try_advisory_lock($1)",
		PostgresKey(name),
	).Scan(&acquiredAgain); err != nil {
		// The witness could not ask. That is not evidence of a broken lock.
		return nil
	}
	if !acquiredAgain {
		return nil
	}

	// The witness got the same lock, so nothing is excluded. Give its own claim
	// back before refusing, or the backend keeps a lock nobody will release.
	var released bool
	_ = witness.QueryRowContext(
		context.WithoutCancel(ctx),
		"SELECT pg_advisory_unlock($1)",
		PostgresKey(name),
	).Scan(&released)

	return fmt.Errorf(
		"%s advisory lock %q excludes nothing on this connection: a second connection took the same "+
			"lock, which is what a transaction-pooling proxy such as PgBouncer produces -- it hands both "+
			"clients one backend session, where the lock is reentrant. Two runs would both believe they "+
			"held it. Connect to the database directly for schema-mutating commands, or set %s=1 to accept "+
			"the risk",
		dialect, name, AllowUnverifiedLock.Name())
}

// PostgresKey returns the 64-bit pg_advisory_lock key derived from name.
func PostgresKey(name string) int64 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(name))
	return int64(hash.Sum32())
}

func acquirePostgresLock(
	ctx context.Context,
	conn *sql.Conn,
	dialect, name string,
	timeout time.Duration,
) error {
	lockCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	ticker := time.NewTicker(postgresLockPollInterval)
	defer ticker.Stop()
	for {
		var acquired bool
		if err := conn.QueryRowContext(
			lockCtx,
			"SELECT pg_try_advisory_lock($1)",
			PostgresKey(name),
		).Scan(&acquired); err != nil {
			return &ambiguousAcquisitionError{
				err: postgresLockWaitError(ctx, lockCtx, dialect, name, timeout, err),
			}
		}
		if acquired {
			return nil
		}

		select {
		case <-lockCtx.Done():
			return postgresLockWaitError(
				ctx,
				lockCtx,
				dialect,
				name,
				timeout,
				lockCtx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func postgresLockWaitError(
	ctx,
	lockCtx context.Context,
	dialect,
	name string,
	timeout time.Duration,
	fallback error,
) error {
	if timeout > 0 &&
		errors.Is(lockCtx.Err(), context.DeadlineExceeded) &&
		ctx.Err() == nil {
		return &TimeoutError{Dialect: dialect, Name: name, Timeout: timeout}
	}
	return fallback
}

func releasePostgresLock(conn *sql.Conn, dialect, name string) func(context.Context) error {
	return func(ctx context.Context) error {
		var released bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", PostgresKey(name)).Scan(&released); err != nil {
			return err
		}
		if !released {
			return fmt.Errorf("%s advisory lock was not held", dialect)
		}
		return nil
	}
}

func acquireMySQLLock(ctx context.Context, conn *sql.Conn, dialect, name string, timeout time.Duration) error {
	timeoutSeconds := mySQLLockTimeoutSeconds(dialect, timeout)

	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, timeoutSeconds).Scan(&acquired); err != nil {
		return &ambiguousAcquisitionError{err: err}
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
		return &ambiguousAcquisitionError{err: err}
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

func closeAfterFailedAcquisition(conn *sql.Conn, acquireErr error) error {
	if _, ok := errors.AsType[*ambiguousAcquisitionError](acquireErr); ok {
		return errors.Join(acquireErr, discardConnection(conn))
	}
	return errors.Join(acquireErr, conn.Close())
}

func discardConnection(conn *sql.Conn) error {
	discardErr := conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
	if errors.Is(discardErr, driver.ErrBadConn) {
		discardErr = nil
	}
	closeErr := conn.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	return errors.Join(discardErr, closeErr)
}
