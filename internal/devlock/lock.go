// Package devlock serializes destructive replay work by disposable database
// realm.
package devlock

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dblock"
)

const (
	lockRetryInterval     = 25 * time.Millisecond
	defaultReleaseTimeout = 10 * time.Second
)

var errLocked = errors.New("dev database realm is locked")

// Lock is an exclusive lock for one disposable database realm.
type Lock struct {
	advisory *dblock.Lock
	file     *os.File
}

// SameRealm reports whether two live connections select the same destructive
// database realm. Network endpoints are intentionally excluded from the
// identity: aliases and replicated members cannot be proven independent before
// cleanup, so equal live database/catalog names fail closed across hosts. A
// YDB database is named by its path and the moment its server created it, so
// two servers that each serve /local are two realms; see ydbIdentity.
//
// A connection to a whole MySQL or MariaDB server holds every database on it,
// so when either side is one, the two are compared by the server they reached;
// see mysqlServerIdentity. The verbs that take a dev server refuse a dev server
// beside one database, and a dev database beside a server, before they get
// here, so this comparison is what keeps a mixed pair from reading as distinct
// on a path that does not.
func SameRealm(
	ctx context.Context,
	left, right *dbschema.DatabaseConnection,
) (bool, error) {
	if left == nil || right == nil {
		return false, errors.New("compare dev database realms requires two database connections")
	}
	leftDialect := platform.NormalizeDialect(left.Info().Dialect)
	rightDialect := platform.NormalizeDialect(right.Info().Dialect)
	if leftDialect != rightDialect {
		return false, nil
	}
	// A whole MySQL-family server holds every database on it, so a server and
	// any database on the same server are one realm: compare the servers.
	if isMySQLFamily(leftDialect) && (left.Info().WholeServer || right.Info().WholeServer) {
		leftServer, err := mysqlServerIdentity(ctx, left, leftDialect)
		if err != nil {
			return false, err
		}
		rightServer, err := mysqlServerIdentity(ctx, right, rightDialect)
		if err != nil {
			return false, err
		}
		return leftServer == rightServer, nil
	}
	leftIdentity, err := realmIdentity(ctx, left, leftDialect)
	if err != nil {
		return false, err
	}
	rightIdentity, err := realmIdentity(ctx, right, rightDialect)
	if err != nil {
		return false, err
	}
	return leftIdentity == rightIdentity, nil
}

// Acquire locks the selected disposable database realm. A zero timeout waits
// until ctx is canceled, and a negative one does not wait: a realm another
// replay holds is refused at once, as [dblock.NoWait] is.
func Acquire(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	timeout time.Duration,
) (*Lock, error) {
	if conn == nil {
		return nil, errors.New("dev database locking requires a database connection")
	}
	dialect := platform.NormalizeDialect(conn.Info().Dialect)
	switch dialect {
	case platform.ClickHouse, platform.CockroachDB:
		if err := validateLocalFileLockURL(dialect, conn.Info().URL); err != nil {
			return nil, err
		}
	}
	identity, err := realmIdentity(ctx, conn, dialect)
	if err != nil {
		return nil, err
	}
	lockName := fmt.Sprintf("ptah-dev-replay:%s:%s", dialect, identity)
	if dblock.Supported(dialect) {
		advisory, err := dblock.Acquire(ctx, conn, lockName, timeout)
		if err != nil {
			return nil, fmt.Errorf("acquire dev database realm lock: %w", err)
		}
		lock, err := finishAcquire(ctx, &Lock{advisory: advisory})
		if err != nil {
			return nil, fmt.Errorf("acquire dev database realm lock: %w", err)
		}
		return lock, nil
	}
	switch dialect {
	case platform.SQLite, platform.ClickHouse, platform.CockroachDB:
		file, err := acquireFile(ctx, localLockPath(lockName), timeout)
		if err != nil {
			return nil, fmt.Errorf("acquire %s dev database realm lock: %w", dialect, err)
		}
		lock, err := finishAcquire(ctx, &Lock{file: file})
		if err != nil {
			return nil, fmt.Errorf("acquire %s dev database realm lock: %w", dialect, err)
		}
		return lock, nil
	default:
		return nil, fmt.Errorf(
			"%s replay cannot safely serialize destructive dev database use",
			dialect,
		)
	}
}

// Release releases the realm lock. It uses a bounded background context so a
// canceled replay does not leak a server advisory lock.
func (l *Lock) Release() error {
	if l == nil {
		return nil
	}
	var advisoryErr error
	if l.advisory != nil {
		ctx, cancel := context.WithTimeout(context.Background(), defaultReleaseTimeout)
		advisoryErr = l.advisory.Release(ctx)
		cancel()
	}
	var fileErr error
	if l.file != nil {
		fileErr = errors.Join(unlockFile(l.file), l.file.Close())
	}
	return errors.Join(advisoryErr, fileErr)
}

func finishAcquire(ctx context.Context, lock *Lock) (*Lock, error) {
	if err := ctx.Err(); err != nil {
		releaseErr := lock.Release()
		return nil, errors.Join(err, releaseErr)
	}
	return lock, nil
}

func realmIdentity(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
) (string, error) {
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return selectedDatabase(ctx, conn, dialect, "SELECT current_database()")
	case platform.SQLServer:
		return selectedDatabase(ctx, conn, dialect, "SELECT DB_NAME()")
	case platform.MySQL, platform.MariaDB:
		if conn.Info().WholeServer {
			return mysqlServerIdentity(ctx, conn, dialect)
		}
		return selectedDatabase(ctx, conn, dialect, "SELECT DATABASE()")
	case platform.ClickHouse:
		return selectedDatabase(ctx, conn, dialect, "SELECT currentDatabase()")
	case platform.SQLite:
		return sqliteIdentity(ctx, conn)
	case platform.YDB:
		return ydbIdentity(ctx, conn)
	default:
		return "", fmt.Errorf("unsupported dev database lock dialect %q", dialect)
	}
}

// realmIdentifier is what a YDB connection's writer says about the dev
// database its root is.
type realmIdentifier interface {
	RealmIdentity(ctx context.Context) (string, error)
}

// ydbIdentity names the dev database a YDB connection treats as its database:
// the database, told apart from one of the same path on another server by the
// moment its server created it, and the dev realm in it the URL named. Two
// realms of one database are two realms, and so are a realm and the database
// that holds it, whose reads and resets leave the realms alone.
func ydbIdentity(ctx context.Context, conn *dbschema.DatabaseConnection) (string, error) {
	identifier, ok := conn.SchemaWriter().(realmIdentifier)
	if !ok {
		return "", fmt.Errorf("resolve ydb dev database realm: the writer %T cannot name its realm", conn.SchemaWriter())
	}
	identity, err := identifier.RealmIdentity(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve ydb dev database realm: %w", err)
	}
	return identity, nil
}

// mysqlServerIdentity names the server a MySQL-family connection reached,
// whichever database it selected: the server's UUID on MySQL, and on MariaDB,
// which has none, its host name, port and data directory. A connection to a
// whole server has no database to name its realm by, and a server reached
// through another spelling of its address, or through a socket, answers the
// same identity (stokaro/ptah#3789).
//
// The identity is a digest, `@` and 32 hexadecimal digits, because it names
// the realm's advisory lock and the server refuses a lock name over 64
// characters; a data directory can be any length. A database name cannot
// start with `@` unquoted, and a digest collision would only make two servers
// share a lock.
func mysqlServerIdentity(ctx context.Context, conn *dbschema.DatabaseConnection, dialect string) (string, error) {
	query := "SELECT @@server_uuid"
	if dialect == platform.MariaDB {
		query = "SELECT CONCAT_WS(':', @@hostname, @@port, @@datadir)"
	}
	var identity sql.NullString
	if err := conn.QueryRowContext(ctx, query).Scan(&identity); err != nil {
		return "", fmt.Errorf("resolve %s dev server identity: %w", dialect, err)
	}
	if !identity.Valid || strings.TrimSpace(identity.String) == "" {
		return "", fmt.Errorf("%s dev server reported no identity", dialect)
	}
	digest := sha256.Sum256([]byte(dialect + "\x00" + identity.String))
	return "@" + hex.EncodeToString(digest[:16]), nil
}

// isMySQLFamily reports whether dialect is MySQL or MariaDB.
func isMySQLFamily(dialect string) bool {
	return dialect == platform.MySQL || dialect == platform.MariaDB
}

func selectedDatabase(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect, query string,
) (string, error) {
	var database sql.NullString
	if err := conn.QueryRowContext(ctx, query).Scan(&database); err != nil {
		return "", fmt.Errorf("resolve %s dev database realm: %w", dialect, err)
	}
	if !database.Valid || strings.TrimSpace(database.String) == "" {
		return "", fmt.Errorf("%s dev database realm has no selected database", dialect)
	}
	return database.String, nil
}

func sqliteIdentity(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) (string, error) {
	var path string
	if err := conn.QueryRowContext(
		ctx,
		"SELECT file FROM pragma_database_list WHERE name = 'main'",
	).Scan(&path); err != nil {
		return "", fmt.Errorf("resolve sqlite dev database realm: %w", err)
	}
	if strings.TrimSpace(path) == "" {
		return ":memory:", nil
	}
	identity, err := filesystemIdentity(path)
	if err != nil {
		return "", fmt.Errorf("resolve sqlite dev database file identity: %w", err)
	}
	return identity, nil
}

func validateLocalFileLockURL(dialect, rawURL string) error {
	parsed, err := atlasurl.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse %s dev database URL for local locking: %w", dialect, err)
	}
	hostname := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if hostname == "" || hostname == "localhost" {
		return nil
	}
	ip := net.ParseIP(hostname)
	if ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf(
		"%s replay cannot safely serialize non-local dev database host %q with a local file lock",
		dialect,
		parsed.Hostname(),
	)
}

func localLockPath(identity string) string {
	hash := sha256.Sum256([]byte(identity))
	return filepath.Join(os.TempDir(), "ptah-dev-replay-locks", fmt.Sprintf("%x.lock", hash))
}

func acquireFile(ctx context.Context, path string, timeout time.Duration) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	startedAt := time.Now()
	for {
		file, err := tryAcquireFile(path)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, errLocked) {
			return nil, err
		}
		if timeout < 0 {
			return nil, errors.New("lock is held by another process")
		}
		if timeout > 0 && time.Since(startedAt) >= timeout {
			return nil, fmt.Errorf("lock timeout after %s", timeout)
		}
		if err := waitForRetry(ctx, startedAt, timeout); err != nil {
			return nil, err
		}
	}
}

func tryAcquireFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	locked, err := tryLockFile(file)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if !locked {
		return nil, errors.Join(errLocked, file.Close())
	}
	return file, nil
}

func waitForRetry(ctx context.Context, startedAt time.Time, timeout time.Duration) error {
	wait := lockRetryInterval
	if timeout > 0 {
		remaining := timeout - time.Since(startedAt)
		if remaining <= 0 {
			return nil
		}
		wait = min(wait, remaining)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
