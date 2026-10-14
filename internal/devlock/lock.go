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
	"sync/atomic"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dblock"
	"ptah.run/internal/devclean"
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
	// dialect and realm name the dev database the lock serializes, for the
	// error of a run that lost it.
	dialect, realm string
}

// SameRealm reports whether two live connections select the same destructive
// database realm. Network endpoints are intentionally excluded from the
// identity: aliases and replicated members cannot be proven independent before
// cleanup, so equal live database/catalog names fail closed across hosts. A
// YDB database is named by its path and the moment its server created it, so
// two servers that each serve /local are two realms; see writerIdentity.
//
// A connection to a whole MySQL or MariaDB server holds every database on it,
// so when either side is one, the two are compared by the server they reached;
// see serverIdentity. The verbs that take a dev server refuse a dev server
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
	realm := devRealm(leftDialect)
	// A whole server holds every database on it, so a server and any
	// database on the same server are one realm: compare the servers.
	if realm.ServerQuery != "" && (left.Info().WholeServer || right.Info().WholeServer) {
		leftServer, err := serverIdentity(ctx, left, leftDialect, realm)
		if err != nil {
			return false, err
		}
		rightServer, err := serverIdentity(ctx, right, rightDialect, realm)
		if err != nil {
			return false, err
		}
		return leftServer == rightServer, nil
	}
	leftIdentity, err := realmIdentity(ctx, left, leftDialect, realm)
	if err != nil {
		return false, err
	}
	rightIdentity, err := realmIdentity(ctx, right, rightDialect, realm)
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
	realm := devRealm(dialect)
	if realm.FileLock == fileLockLoopback {
		if err := validateLocalFileLockURL(dialect, conn.Info().URL); err != nil {
			return nil, err
		}
	}
	identity, err := realmIdentity(ctx, conn, dialect, realm)
	if err != nil {
		return nil, err
	}
	lockName := fmt.Sprintf("ptah-dev-replay:%s:%s", dialect, identity)
	if dblock.Supported(dialect) {
		advisory, err := dblock.Acquire(ctx, conn, lockName, timeout)
		if err != nil {
			return nil, fmt.Errorf("acquire dev database realm lock: %w", err)
		}
		lock, err := finishAcquire(ctx, &Lock{advisory: advisory, dialect: dialect, realm: identity})
		if err != nil {
			return nil, fmt.Errorf("acquire dev database realm lock: %w", err)
		}
		return lock, nil
	}
	switch realm.FileLock {
	case fileLockLocal, fileLockLoopback:
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

// Guard returns a context derived from ctx that ends when the realm's lock is
// lost, and settle, which stops watching and returns the error of the work
// that ran under the context: err while the lock held, and the loss otherwise
// (see [dblock.Lock.Settle]). Call settle once the work is done and before
// [Lock.Release].
//
// A server lock lives on a session of its own while the work runs on other
// connections, so the end of that session reaches the work through this
// context alone; another replay may take the realm then. A file lock is never
// lost, and its context ends only with ctx.
//
// A cleanup of the dev database that runs under the context asks [MayClean]
// first. After a loss it does not run, and the error settle returns says that
// the dev database was left as it was, which one, and how to empty it.
func (l *Lock) Guard(ctx context.Context) (guarded context.Context, settle func(err error) error) {
	var advisory *dblock.Lock
	if l != nil {
		advisory = l.advisory
	}
	guarded, stop := advisory.Guard(ctx)
	state := &guardState{lock: advisory}
	guarded = context.WithValue(guarded, guardStateKey{}, state)
	return guarded, func(err error) error {
		defer stop()
		settled := advisory.Settle("dev database lock", "the work on the dev database", err)
		if state.skipped.Load() && dblock.IsLost(settled) {
			return fmt.Errorf("%w; %s", settled, l.leftInPlace())
		}
		return settled
	}
}

// guardStateKey carries the [guardState] of a context [Lock.Guard] returned.
type guardStateKey struct{}

// guardState is what a guarded context knows about its lock: the lock, and
// whether a cleanup was skipped because the lock was lost.
type guardState struct {
	lock    *dblock.Lock
	skipped atomic.Bool
}

// MayClean reports whether a cleanup of the dev database may run under ctx: it
// may unless ctx came from [Lock.Guard] and the realm's lock has been lost,
// which it asks the lock itself rather than the context, so a loss the context
// has not heard of yet still counts.
//
// After a loss another run may hold the realm, and a cleanup would drop what
// that run put there, silently. Left alone, the realm keeps this run's
// objects, and the next run that claims it refuses it as not clean, loudly.
// A cleanup MayClean refuses is recorded, and the guard's settle reports it.
func MayClean(ctx context.Context) bool {
	state, ok := ctx.Value(guardStateKey{}).(*guardState)
	if !ok || state.lock.Err() == nil {
		return true
	}
	state.skipped.Store(true)
	return false
}

// leftInPlace says what a run that lost the lock left behind, and how to empty
// it: the claim the next run makes refuses it with the remedy
// [devclean.NotCleanRemedy] names.
func (l *Lock) leftInPlace() string {
	return fmt.Sprintf("the %s dev database %s was left as it was, since another run may be using it now: "+
		"empty it by hand once no run uses it (ptah db drop-all empties a database), or %s",
		l.dialect, l.realm, devclean.NotCleanRemedy)
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

// The values of descriptor.DevRealm.Identity this package reads.
const (
	identityQuery  = "query"
	identityFile   = "file"
	identityWriter = "writer"
)

// The values of descriptor.DevRealm.FileLock this package reads.
const (
	fileLockLocal    = "local"
	fileLockLoopback = "loopback"
)

// devRealm is what the descriptor of dialect states about its dev
// databases, and the zero value for a dialect no descriptor describes.
func devRealm(dialect string) descriptor.DevRealm {
	described, _ := platform.Lookup(dialect)
	return described.DevRealm
}

// realmIdentity names the dev database conn selected, the way realm says the
// engine names one. A connection that holds a whole server is named by the
// server.
func realmIdentity(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
	realm descriptor.DevRealm,
) (string, error) {
	if realm.ServerQuery != "" && conn.Info().WholeServer {
		return serverIdentity(ctx, conn, dialect, realm)
	}
	switch realm.Identity {
	case identityQuery:
		return selectedDatabase(ctx, conn, dialect, realm.Query)
	case identityFile:
		return fileIdentity(ctx, conn, dialect, realm.Query)
	case identityWriter:
		return writerIdentity(ctx, conn, dialect)
	default:
		return "", fmt.Errorf("unsupported dev database lock dialect %q", dialect)
	}
}

// realmIdentifier is what a connection's writer says about the dev database
// its root is, for an engine whose dev database the writer names.
type realmIdentifier interface {
	RealmIdentity(ctx context.Context) (string, error)
}

// writerIdentity names the dev database a connection treats as its database
// by asking the connection's writer. On YDB that is the database, told apart
// from one of the same path on another server by the moment its server
// created it, and the dev realm in it the URL named. Two realms of one
// database are two realms, and so are a realm and the database that holds it,
// whose reads and resets leave the realms alone.
func writerIdentity(ctx context.Context, conn *dbschema.DatabaseConnection, dialect string) (string, error) {
	identifier, ok := conn.SchemaWriter().(realmIdentifier)
	if !ok {
		return "", fmt.Errorf("resolve %s dev database realm: the writer %T cannot name its realm", dialect, conn.SchemaWriter())
	}
	identity, err := identifier.RealmIdentity(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve %s dev database realm: %w", dialect, err)
	}
	return identity, nil
}

// serverIdentity names the server a connection reached, whichever database it
// selected, with the realm's ServerQuery: the server's UUID on MySQL, and on
// MariaDB, which has none, its host name, port and data directory. A
// connection to a whole server has no database to name its realm by, and a
// server reached through another spelling of its address, or through a
// socket, answers the same identity (stokaro/ptah#3789).
//
// The identity is a digest, `@` and 32 hexadecimal digits, because it names
// the realm's advisory lock and the server refuses a lock name over 64
// characters; a data directory can be any length. A database name cannot
// start with `@` unquoted, and a digest collision would only make two servers
// share a lock.
func serverIdentity(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
	realm descriptor.DevRealm,
) (string, error) {
	var identity sql.NullString
	if err := conn.QueryRowContext(ctx, realm.ServerQuery).Scan(&identity); err != nil {
		return "", fmt.Errorf("resolve %s dev server identity: %w", dialect, err)
	}
	if !identity.Valid || strings.TrimSpace(identity.String) == "" {
		return "", fmt.Errorf("%s dev server reported no identity", dialect)
	}
	digest := sha256.Sum256([]byte(dialect + "\x00" + identity.String))
	return "@" + hex.EncodeToString(digest[:16]), nil
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

// fileIdentity names the file that holds the dev database, whose path query
// selects, by its identity on disk, so two spellings of one path name one
// realm. An empty path is an in-memory database.
func fileIdentity(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect, query string,
) (string, error) {
	var path string
	if err := conn.QueryRowContext(ctx, query).Scan(&path); err != nil {
		return "", fmt.Errorf("resolve %s dev database realm: %w", dialect, err)
	}
	if strings.TrimSpace(path) == "" {
		return ":memory:", nil
	}
	identity, err := filesystemIdentity(path)
	if err != nil {
		return "", fmt.Errorf("resolve %s dev database file identity: %w", dialect, err)
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
