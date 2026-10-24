// Package dblock acquires dialect-aware, session-scoped database advisory
// locks used to serialize schema-mutating operations such as migration runs
// and Atlas schema apply.
//
// An engine's lock service (internal/dblockext) supplies the statements or SDK
// calls that take and give back its lock, and dblock owns the rest: the
// session the lock lives on, the watch that reports a lost lock, the timeout
// and loss errors, and what happens to a session after a failed acquisition.
// The services of the engines Ptah ships are listed by
// internal/dblockext/builtin, and [Register] adds those of an engine it does
// not ship. A dialect without a lock service acquires an explicit no-op lock,
// so callers make a capability decision through [Lock.Supported] instead of
// failing or silently diverging per dialect.
package dblock

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"time"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dblockext"
	"ptah.run/internal/dblockext/builtin"
	"ptah.run/internal/envbool"
)

// DefaultReleaseTimeout bounds the deferred release of an advisory lock so a
// canceled command still returns its dedicated session connection promptly.
const DefaultReleaseTimeout = 10 * time.Second

// NoWait is a timeout that does not wait at all: the lock is tried once, and a
// lock another session holds is a [TimeoutError] at once. Every negative
// timeout means the same; this is the one to spell.
//
// It is the Atlas-compatible surface's reading of `--lock-timeout 0` and of a
// negative value: measured on the pinned Atlas community binary v1.3.0 on
// PostgreSQL, `migrate apply --lock-timeout 0` and `--lock-timeout -1s` against
// a held lock both fail at once, where `2s` fails after two seconds.
const NoWait time.Duration = -1

// TimeoutError reports that another session held the advisory lock longer
// than the caller was configured to wait, or held it at all under [NoWait].
type TimeoutError struct {
	Dialect string
	Name    string
	Timeout time.Duration
}

func (e *TimeoutError) Error() string {
	if e.Timeout < 0 {
		return fmt.Sprintf("advisory lock %q on %s is held by another session", e.Name, e.Dialect)
	}
	return fmt.Sprintf("timed out acquiring advisory lock %q on %s after %s", e.Name, e.Dialect, e.Timeout)
}

// IsTimeout reports whether err wraps an advisory lock timeout.
func IsTimeout(err error) bool {
	var target *TimeoutError
	return errors.As(err, &target)
}

// ErrInvalidRegistration identifies lock services [Register] refuses.
var ErrInvalidRegistration = errors.New("invalid lock service registration")

// registered holds the lock services [Register] added. The built-in ones are
// never in it.
var registered = struct {
	mu       sync.RWMutex
	services map[string]dblockext.Services
}{services: make(map[string]dblockext.Services)}

// Register adds the lock services of dialects Ptah does not ship, for the
// composition root of a program that selects such a dialect. Each dialect is
// the canonical name of a dialect the process registry describes
// (platform.Register), and no other lock service serves it. Each Services
// value takes a lock, through Session or Separate and not both, and an
// Exclusion check goes with a Session lock only. Every refusal wraps
// [ErrInvalidRegistration], and on a refusal nothing is added.
//
// Registration only adds, and lasts for the rest of the process. Register is
// safe for concurrent use, with itself and with every reader.
func Register(services ...dblockext.Services) error {
	registered.mu.Lock()
	defer registered.mu.Unlock()
	added := make(map[string]dblockext.Services)
	for _, served := range services {
		if err := validServices(served); err != nil {
			return err
		}
		for _, dialect := range served.Dialects {
			if dialect == "" || platform.NormalizeDialect(dialect) != dialect {
				return fmt.Errorf("%w: %q is not the canonical name of a described dialect",
					ErrInvalidRegistration, dialect)
			}
			_, again := added[dialect]
			_, taken := registered.services[dialect]
			if again || taken || builtin.Lookup(dialect).Locks() {
				return fmt.Errorf("%w: dialect %q already has a lock service", ErrInvalidRegistration, dialect)
			}
			added[dialect] = served
		}
	}
	maps.Copy(registered.services, added)
	return nil
}

// validServices refuses services no lock can be taken through.
func validServices(served dblockext.Services) error {
	switch {
	case len(served.Dialects) == 0:
		return fmt.Errorf("%w: the services name no dialect", ErrInvalidRegistration)
	case !served.Locks():
		return fmt.Errorf("%w: the services of %q take no lock", ErrInvalidRegistration, served.Dialects)
	case served.Session != nil && served.Separate != nil:
		return fmt.Errorf("%w: the services of %q take a session lock and a separate one",
			ErrInvalidRegistration, served.Dialects)
	case served.Exclusion != nil && served.Session == nil:
		return fmt.Errorf("%w: the services of %q check the exclusion of a lock no session holds",
			ErrInvalidRegistration, served.Dialects)
	default:
		return nil
	}
}

// servicesOf returns the lock services of the dialect a spelling selects: a
// built-in one, one [Register] added, or the zero Services, which take no lock.
// It is the one answer to whether a dialect locks: [Supported] and
// [SupportedDialects] ask it before anything is connected, and the
// acquisition takes the lock through what it returns.
func servicesOf(dialect string) dblockext.Services {
	canonical := platform.NormalizeDialect(dialect)
	if served := builtin.Lookup(canonical); served.Locks() {
		return served
	}
	registered.mu.RLock()
	defer registered.mu.RUnlock()
	return registered.services[canonical]
}

// Supported reports whether dialect has session-scoped advisory-lock
// semantics. [Acquire] returns a no-op lock on unsupported dialects.
func Supported(dialect string) bool {
	return servicesOf(dialect).Locks()
}

// SupportedDialects returns the canonical dialect names whose targets take a
// session advisory lock, in the order the dialect registry lists them. The
// returned slice is new on every call, so a caller may sort or filter it.
func SupportedDialects() []string {
	var supported []string
	for _, dialect := range platform.Dialects().Dialects() {
		if servicesOf(dialect.Name).Locks() {
			supported = append(supported, dialect.Name)
		}
	}
	return supported
}

// Lock is a held session-scoped advisory lock. The zero value is the no-op
// lock acquired on dialects without advisory-lock semantics; releasing it
// never touches the database.
type Lock struct {
	conn    *sql.Conn
	name    string
	dialect string
	release func(context.Context) error
	// holding says whether a lock the server can take away is still held. It
	// is nil for every other lock.
	holding holding
}

// holding is what a lock the server can take away from its holder offers: a
// channel closed when it is lost, and whether it may still be held.
type holding interface {
	Lost() <-chan struct{}
	Held() bool
}

// LostError reports that the server took a held lock away from its holder,
// so another session may hold it now.
type LostError struct {
	Dialect string
	Name    string
}

func (e *LostError) Error() string {
	return fmt.Sprintf("advisory lock %q on %s was lost while it was held: the server no longer confirms "+
		"that this session owns it, so another session may hold it now", e.Name, e.Dialect)
}

// IsLost reports whether err wraps a [LostError].
func IsLost(err error) bool {
	var target *LostError
	return errors.As(err, &target)
}

// Supported reports whether the lock is backed by a real database lock, as
// opposed to the no-op lock acquired on dialects without advisory locks.
func (l *Lock) Supported() bool {
	return l != nil && l.release != nil
}

// Done returns a channel that is closed when the server takes the lock away
// from its holder. A lock [Acquire] took reports a loss: it belongs to a
// session of its own -- a database session from the pool, or a session the
// engine's lock service opens apart from the pool -- which can end while the
// work it guards goes on over other sessions. A lock [WithLockSession] took returns nil, a channel that
// never closes: the work runs on the session that holds the lock, so the end
// of that session fails the work itself. So does a no-op lock. A release is
// not a loss and closes nothing.
func (l *Lock) Done() <-chan struct{} {
	if l == nil || l.holding == nil {
		return nil
	}
	return l.holding.Lost()
}

// Err returns a [LostError] once the lock has been taken away, and nil while
// it may still be held, after it was released, and for a lock that reports no
// loss (see [Lock.Done]).
//
// The answer is current, not the last one a background check saw. A database
// session that has not answered for a second is asked again before Err
// answers, and a lock held by a session the engine's lock service opens is
// judged by that service's own rule. A holder that was stopped and resumes
// learns of a loss here before it does anything else.
func (l *Lock) Err() error {
	if l == nil || l.holding == nil || l.holding.Held() {
		return nil
	}
	return &LostError{Dialect: l.dialect, Name: l.name}
}

// Guard returns a context derived from ctx that ends, with a [LostError] as
// its cause, when the lock is lost, and a function that stops watching. Work
// run under the context stops with the lock. Call stop once the work is done
// and before releasing the lock. For a lock that reports no loss the context
// ends only with ctx.
func (l *Lock) Guard(ctx context.Context) (context.Context, func()) {
	guarded, cancel := context.WithCancelCause(ctx)
	lost := l.Done()
	if lost == nil {
		return guarded, func() { cancel(nil) }
	}
	stopped := make(chan struct{})
	finished := make(chan struct{})
	done := guarded.Done()
	go func() {
		defer close(finished)
		select {
		case <-lost:
			cancel(&LostError{Dialect: l.dialect, Name: l.name})
		case <-stopped:
		case <-done:
		}
	}()
	return guarded, func() {
		close(stopped)
		<-finished
		cancel(nil)
	}
}

// Settle returns the error of work that ran under a context [Lock.Guard]
// returned. While the lock holds, it is workErr. Once the lock is lost, it is
// the [LostError] as "<subject>: <loss>; <work> stopped there", followed by
// what the work reported when that was something other than the loss or the
// cancellation the loss sent. Settle before releasing the lock: a release is
// not a loss, and a released lock reports none.
func (l *Lock) Settle(subject, work string, workErr error) error {
	lostErr := l.Err()
	if lostErr == nil {
		return workErr
	}
	lostErr = fmt.Errorf("%s: %w; %s stopped there", subject, lostErr, work)
	if workErr != nil && !IsLost(workErr) && !errors.Is(workErr, context.Canceled) {
		return fmt.Errorf("%w (%s reported: %v)", lostErr, work, workErr)
	}
	return lostErr
}

// Name returns the advisory lock name this lock was acquired under, after the
// trimming [Acquire] applies. It is recorded on the no-op path too, so a
// caller on a dialect without advisory locks can still report which lock it
// would have taken. A nil lock reports the empty string.
//
// This is the value the engine's lock service actually received: the service
// passes it to the server verbatim, names the lock's object with it, or
// derives a key from it.
func (l *Lock) Name() string {
	if l == nil {
		return ""
	}
	return l.name
}

// Release releases the advisory lock and closes a session owned by the lock.
// A session-bound lock leaves its borrowed session open for its owner to
// discard. Releasing a nil or no-op lock is a no-op.
func (l *Lock) Release(ctx context.Context) error {
	if l == nil || l.release == nil {
		return nil
	}
	release := l.release
	l.release = nil
	conn := l.conn
	l.conn = nil
	releaseErr := release(ctx)
	switch {
	case conn == nil:
		return releaseErr
	case errors.Is(releaseErr, errSessionEnded):
		// The lock went with its session, so there is nothing to give back,
		// and the connection is not returned to the pool.
		return discardConnection(conn)
	case releaseErr != nil:
		return errors.Join(releaseErr, discardConnection(conn))
	default:
		return conn.Close()
	}
}

// WithLockSession pins one physical database session, acquires name on that
// session, and runs use with the same session. The original pool is retained
// only for the lock service: a second session from it is the witness of an
// exclusion check, and a lock held apart from the pool is taken through it.
// Dialects without advisory-lock semantics still pin the callback session and
// receive an explicit no-op lock.
//
// The lock is released before the pinned session is discarded. Callback and
// release errors are returned separately so a command can preserve the
// callback failure while reporting a secondary cleanup warning. If the
// callback succeeds but release fails, the release failure becomes the run
// error: losing the session must never be reported as a successful operation.
// A zero timeout waits indefinitely and a negative one does not wait (see
// [NoWait]); context cancellation always interrupts the acquisition and the
// callback.
func WithLockSession(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name string,
	timeout time.Duration,
	use func(*dbschema.DatabaseConnection, *Lock) error,
) (runErr, releaseErr error) {
	return withLockSession(ctx, conn, name, timeout, acquireSessionLock, use)
}

// WithPreviewLockSession is [WithLockSession] for a run that writes nothing,
// such as a dry run: it takes the lock only where taking it writes nothing
// either. A lock whose service would first have to create the object it lives
// on, such as YDB's coordination node, is not taken while that object is
// missing, and the run gets a no-op lock: no run that took a lock has used
// that database, so there is nobody to wait for.
func WithPreviewLockSession(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name string,
	timeout time.Duration,
	use func(*dbschema.DatabaseConnection, *Lock) error,
) (runErr, releaseErr error) {
	return withLockSession(ctx, conn, name, timeout, acquirePreviewLock, use)
}

type sessionLockAcquirer func(
	context.Context,
	*dbschema.DatabaseConnection,
	*dbschema.DatabaseConnection,
	string,
	string,
	time.Duration,
) (*Lock, error)

func withLockSession(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name string,
	timeout time.Duration,
	acquire sessionLockAcquirer,
	use func(*dbschema.DatabaseConnection, *Lock) error,
) (runErr, releaseErr error) {
	if conn == nil {
		return errors.New("advisory locking requires database connection"), nil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("advisory lock name must not be empty"), nil
	}
	if use == nil {
		return errors.New("advisory lock session callback is nil"), nil
	}
	dialect := platform.NormalizeDialect(conn.Info().Dialect)

	runErr = conn.WithSession(ctx, func(session *dbschema.DatabaseConnection) (callbackErr error) {
		lock, err := acquire(ctx, session, conn, dialect, name, timeout)
		if err != nil {
			return err
		}
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), DefaultReleaseTimeout)
			defer cancel()
			releaseErr = lock.Release(releaseCtx)
		}()
		return use(session, lock)
	})
	if runErr == nil && releaseErr != nil {
		return fmt.Errorf("advisory lock session failed during release: %w", releaseErr), nil
	}
	return runErr, releaseErr
}

func acquireSessionLock(
	ctx context.Context,
	session,
	witness *dbschema.DatabaseConnection,
	dialect,
	name string,
	timeout time.Duration,
) (*Lock, error) {
	if lock, handled, err := nativeLock(ctx, session, name, timeout); handled {
		return lock, err
	}
	return acquireOnSession(ctx, session, witness, dialect, name, timeout, false)
}

func acquirePreviewLock(
	ctx context.Context,
	session,
	witness *dbschema.DatabaseConnection,
	dialect,
	name string,
	timeout time.Duration,
) (*Lock, error) {
	if lock, handled, err := nativePreviewLock(session, dialect, name); handled {
		return lock, err
	}
	return acquireOnSession(ctx, session, witness, dialect, name, timeout, true)
}

// Connection is the database [Acquire] takes a lock on: a pool that names the
// dialect of the server it reaches and hands out a session of its own.
// *dbschema.DatabaseConnection satisfies it.
type Connection interface {
	Info() catalog.ServerInfo
	Conn(ctx context.Context) (*sql.Conn, error)
}

// Acquire takes the dialect-specific session advisory lock named name on a
// dedicated connection from conn's pool. A zero timeout waits indefinitely and
// a negative one does not wait (see [NoWait]); context cancellation always
// interrupts the wait. Elapsed timeouts surface as a [TimeoutError]. On dialects without advisory-lock semantics Acquire
// returns a no-op lock, so callers decide whether that is acceptable through
// [Lock.Supported].
func Acquire(
	ctx context.Context,
	conn Connection,
	name string,
	timeout time.Duration,
) (*Lock, error) {
	if isNil(conn) {
		return nil, errors.New("advisory locking requires database connection")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("advisory lock name must not be empty")
	}
	if lock, handled, err := nativeLock(ctx, conn, name, timeout); handled {
		return lock, err
	}

	dialect := platform.NormalizeDialect(conn.Info().Dialect)
	services := servicesOf(dialect)
	if !services.Locks() {
		return &Lock{name: name}, nil
	}
	if services.Separate != nil {
		// The lock belongs to a session the service opens itself, so it
		// holds no database session from the pool.
		return acquireOnSession(ctx, nil, conn, dialect, name, timeout, false)
	}

	session, err := conn.Conn(ctx)
	if err != nil {
		return nil, err
	}

	lock, acquireErr := acquireOnSession(ctx, dblockext.SQLSession{Conn: session}, conn, dialect, name, timeout, false)
	if acquireErr != nil {
		return nil, closeAfterFailedAcquisition(session, acquireErr)
	}
	lock.conn = session
	hold := newSessionHold(session.PingContext, lock.release, time.Now)
	lock.release = hold.release
	lock.holding = hold
	return lock, nil
}

// isNil reports whether conn is nil or holds a nil pointer, such as a nil
// *dbschema.DatabaseConnection, which has no Info to read.
func isNil(conn Connection) bool {
	if conn == nil {
		return true
	}
	value := reflect.ValueOf(conn)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

// acquireOnSession takes the lock dialect's service offers: on session when
// it is a session lock, and through witness when the service holds it apart
// from the pool. preview marks a run that writes nothing (see
// [WithPreviewLockSession]).
func acquireOnSession(
	ctx context.Context,
	session dblockext.Session,
	witness dblockext.Pool,
	dialect,
	name string,
	timeout time.Duration,
	preview bool,
) (*Lock, error) {
	lock := &Lock{name: name, dialect: dialect}
	services := servicesOf(dialect)
	request := dblockext.Request{
		Dialect: dialect,
		Name:    name,
		Timeout: timeout,
		Preview: preview,
		Held:    &TimeoutError{Dialect: dialect, Name: name, Timeout: timeout},
	}

	var acquireErr error
	switch {
	case services.Session != nil:
		lock.release = func(ctx context.Context) error { return services.Session.Release(ctx, session, request) }
		acquireErr = services.Session.Acquire(ctx, session, request)
		if acquireErr == nil && services.Exclusion != nil {
			if verifyErr := verifyExcludes(ctx, services.Exclusion, witness, request); verifyErr != nil {
				acquireErr = &dblockext.AmbiguousError{Err: verifyErr}
			}
		}
	case services.Separate != nil:
		var hold dblockext.Hold
		hold, acquireErr = services.Separate.Acquire(ctx, witness, request)
		if hold != nil {
			lock.release = hold.Release
			lock.holding = hold
		}
	}
	if acquireErr != nil {
		return nil, acquireErr
	}
	return lock, nil
}

// AllowUnverifiedLock lets an operator run without an engine's exclusion
// check (dblockext.ExclusionCheck).
//
// It is an opt-out from a refusal, not from the lock: the lock is still taken,
// and what is skipped is the proof that it excludes anybody. Set it only where
// the topology is known and the risk is accepted -- two runs against a
// transaction pooler will both believe they hold it.
var AllowUnverifiedLock = envbool.New("PTAH_ALLOW_UNVERIFIED_MIGRATION_LOCK", false, envbool.Gated)

// verifyExcludes runs check on a lock just taken, and refuses the run when
// the lock excludes nobody, unless [AllowUnverifiedLock] accepts the risk. A
// lock that excludes nobody fails OPEN -- two runs would both believe they
// held it -- which is why this is a refusal rather than a warning
// (stokaro/ptah#1029).
func verifyExcludes(
	ctx context.Context,
	check dblockext.ExclusionCheck,
	pool dblockext.Pool,
	request dblockext.Request,
) error {
	allowed, err := AllowUnverifiedLock.Resolve()
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	if err := check.Excludes(ctx, pool, request); err != nil {
		return fmt.Errorf("%w. Connect to the database directly for schema-mutating commands, or set %s=1 "+
			"to accept the risk", err, AllowUnverifiedLock.Name())
	}
	return nil
}

func closeAfterFailedAcquisition(conn *sql.Conn, acquireErr error) error {
	if _, ok := errors.AsType[*dblockext.AmbiguousError](acquireErr); ok {
		return errors.Join(acquireErr, discardConnection(conn))
	}
	return errors.Join(acquireErr, conn.Close())
}

func discardConnection(conn *sql.Conn) error {
	discardErr := conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
	if errors.Is(discardErr, driver.ErrBadConn) || errors.Is(discardErr, sql.ErrConnDone) {
		// Discarded now, or already gone: database/sql closes a connection
		// whose driver reported it bad, such as a session the server ended.
		discardErr = nil
	}
	closeErr := conn.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	return errors.Join(discardErr, closeErr)
}
