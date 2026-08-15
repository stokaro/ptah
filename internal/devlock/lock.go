// Package devlock serializes destructive replay work by disposable database
// realm.
package devlock

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/dblock"
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
// cleanup, so equal live database/catalog names fail closed across hosts.
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
// until ctx is canceled.
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
		return selectedDatabase(ctx, conn, dialect, "SELECT DATABASE()")
	case platform.ClickHouse:
		return selectedDatabase(ctx, conn, dialect, "SELECT currentDatabase()")
	case platform.SQLite:
		return sqliteIdentity(ctx, conn)
	default:
		return "", fmt.Errorf("unsupported dev database lock dialect %q", dialect)
	}
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
