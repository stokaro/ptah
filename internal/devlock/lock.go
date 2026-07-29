// Package devlock serializes destructive replay work by disposable database
// realm.
package devlock

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/dblock"
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
		return &Lock{advisory: advisory}, nil
	}
	switch dialect {
	case platform.SQLite, platform.ClickHouse, platform.CockroachDB:
		file, err := acquireFile(ctx, localLockPath(lockName), timeout)
		if err != nil {
			return nil, fmt.Errorf("acquire %s dev database realm lock: %w", dialect, err)
		}
		return &Lock{file: file}, nil
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

func realmIdentity(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
) (string, error) {
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		var database string
		if err := conn.QueryRowContext(ctx, "SELECT current_database()").Scan(&database); err != nil {
			return "", fmt.Errorf("resolve %s dev database realm: %w", dialect, err)
		}
		return database, nil
	case platform.SQLServer:
		var database string
		if err := conn.QueryRowContext(ctx, "SELECT DB_NAME()").Scan(&database); err != nil {
			return "", fmt.Errorf("resolve sqlserver dev database realm: %w", err)
		}
		return database, nil
	case platform.MySQL, platform.MariaDB, platform.ClickHouse:
		database := strings.TrimSpace(conn.Info().Schema)
		if database == "" {
			return "", fmt.Errorf("%s dev database realm has no selected database", dialect)
		}
		return database, nil
	case platform.SQLite:
		return sqliteIdentity(conn.Info().URL)
	default:
		return "", fmt.Errorf("unsupported dev database lock dialect %q", dialect)
	}
}

func sqliteIdentity(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	path := parsed.Path
	if parsed.Opaque != "" {
		path = parsed.Opaque
	}
	if parsed.Host != "" {
		path = parsed.Host + path
	}
	if strings.Contains(path, ":memory:") || strings.HasPrefix(path, "file:") {
		return path, nil
	}
	return filepath.Abs(filepath.Clean(path))
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
