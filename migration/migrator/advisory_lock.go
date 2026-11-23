package migrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	lockbuiltin "ptah.run/internal/dblockext/builtin"
)

const migrationAdvisoryLockName = "ptah_migrate"
const migrationAdvisoryUnlockTimeout = dblock.DefaultReleaseTimeout

// MigrationLockTimeoutError reports that another runner held the migration
// advisory lock longer than this migrator was configured to wait, or held it
// at all when the migrator was configured not to wait. Timeout is the
// configured value, negative in the second case.
type MigrationLockTimeoutError struct {
	Dialect string
	Name    string
	Timeout time.Duration
}

func (e *MigrationLockTimeoutError) Error() string {
	if e.Timeout < 0 {
		return fmt.Sprintf("migration lock %q for %s is held by another runner", e.Name, e.Dialect)
	}
	return fmt.Sprintf("timed out acquiring migration lock %q for %s after %s", e.Name, e.Dialect, e.Timeout)
}

// IsMigrationLockTimeout reports whether err wraps a migration lock timeout.
func IsMigrationLockTimeout(err error) bool {
	var target *MigrationLockTimeoutError
	return errors.As(err, &target)
}

// WithMigrationLockTimeout returns a copy of the migrator that limits how long
// it waits for the session-level migration advisory lock. Zero means wait
// indefinitely. A negative timeout means do not wait: the lock is tried once,
// and a lock another runner holds is a [MigrationLockTimeoutError] at once.
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

// WithoutMigrationLock returns a copy of the migrator that runs without the
// session-level migration advisory lock: no lock is requested, no wait is
// observed, and concurrent runners are not serialized against each other.
//
// This is a deliberate loss of safety, not a capability decision like the
// no-op lock returned on dialects that have no advisory locks — there the
// database cannot serialize, here the caller has asked not to. Callers must
// surface the choice; the migrator does not warn on its own.
func (m *Migrator) WithoutMigrationLock() *Migrator {
	tmp := *m
	tmp.migrationLockSkipped = true
	return &tmp
}

// MigrationLockName returns the advisory lock name this migrator acquires,
// after the trimming and defaulting [Migrator.WithMigrationLockName] applies.
// It reports the name even when locking is skipped, so a caller can name the
// lock it chose not to take.
func (m *Migrator) MigrationLockName() string {
	return m.effectiveMigrationLockName()
}

// MigrationLockSkipped reports whether [Migrator.WithoutMigrationLock] turned
// the session-level migration advisory lock off.
func (m *Migrator) MigrationLockSkipped() bool {
	return m != nil && m.migrationLockSkipped
}

// HoldMigrationLock runs fn while holding the session-level migration advisory
// lock, for a caller whose reads before an operation belong under the same
// lock: a read initializes the revision metadata, so a caller that reads the
// status, decides and then runs holds the lock from the first read.
//
// fn receives the context to run with. An operation of this migrator, or of a
// copy of it with the same lock name, called with that context runs under the
// held lock and takes no second one; the same operation called with another
// context waits for the lock this call holds. On an engine whose lock keeps a
// table, Oracle's, the copy needs the same migrations schema too, an unnamed
// one being the session's current schema: a copy configured for another
// schema takes that schema's lock. The lock is acquired and
// released, and a lost lock is reported, as for [Migrator.MigrateUp]. A
// dry-run writer and [Migrator.WithoutMigrationLock] take no lock, and fn runs
// at once.
func (m *Migrator) HoldMigrationLock(ctx context.Context, operation string, fn func(context.Context) error) error {
	return m.withMigrationLock(ctx, operation, fn)
}

func (m *Migrator) withMigrationLock(ctx context.Context, operation string, fn func(context.Context) error) error {
	if m.conn == nil || m.conn.Writer().IsDryRun() || m.migrationLockSkipped {
		return fn(ctx)
	}

	dialect := m.conn.Info().Dialect
	held := migrationLockHolder{conn: m.conn, name: m.effectiveMigrationLockName(), scope: m.migrationLockScope()}
	lockName := held.name
	if holder, ok := ctx.Value(migrationLockHolderKey{}).(migrationLockHolder); ok && holder == held {
		return fn(ctx)
	}
	if err := m.refuseForeignLockTable(ctx); err != nil {
		return fmt.Errorf("failed to acquire migration lock for %s: %w", operation, err)
	}
	startedAt := time.Now()
	observer := m.migrationObserver()
	lockCtx, span := observer.StartSpan(ctx, "ptah.lock.acquire",
		attr("db.system", dialect),
		attr("migration.operation", operation),
		attr("lock.name", lockName),
		attr("lock.timeout_ms", m.migrationLockTimeout.Milliseconds()),
	)
	lock, err := acquireMigrationLock(ctx, m.conn, lockName, m.migrationLockSchema(), m.migrationLockTimeout)
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
	// Again once the lock is taken, which may have created its table: where a
	// table belongs to the account that created it, the check above and the
	// creation are two round trips, and a table another account placed between
	// them is the one the lock wrote its row to. On Oracle a table belongs to
	// its schema's account, so the check above already answered for a table
	// the lock had yet to create, and this one gives the same answer.
	if err := m.refuseForeignLockTable(ctx); err != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), migrationAdvisoryUnlockTimeout)
		defer cancel()
		return errors.Join(fmt.Errorf("failed to acquire migration lock for %s: %w", operation, err), lock.Release(releaseCtx))
	}

	guarded, stopGuard := lock.Guard(ctx)
	runCtx := withHeldMigrationLock(guarded, lock)
	runCtx = context.WithValue(runCtx, migrationLockHolderKey{}, held)
	if service, err := m.conn.SessionLocker(); err == nil && service != nil {
		runCtx = withMigrationLockSession(runCtx, m.conn)
	}
	runErr := fn(runCtx)
	// Read before the release, which is not a loss and hides one.
	lostErr := lock.Err()
	stopGuard()
	releaseCtx, cancel := context.WithTimeout(context.Background(), migrationAdvisoryUnlockTimeout)
	defer cancel()
	releaseErr := lock.Release(releaseCtx)
	return migrationLockOutcome(operation, runErr, lostErr, releaseErr)
}

// migrationLockOutcome is the error of a run that held the migration lock.
//
// A lost lock is the run's failure whatever else happened: another runner may
// have taken the lock and gone on, so a run that finished its own statements
// did not finish them alone. A failed release is the run's failure too, since
// a lock the server may still hold keeps every other runner waiting until the
// session times out.
func migrationLockOutcome(operation string, runErr, lostErr, releaseErr error) error {
	if lostErr != nil {
		lostErr = fmt.Errorf("migration lock for %s: %w; the run stopped there, and the revision table "+
			"records what it committed", operation, lostErr)
		if runErr != nil && (!dblock.IsLost(runErr) || errors.Is(runErr, ptaherr.ErrOutcomeUnknown)) {
			lostErr = fmt.Errorf("%w (the run reported: %w)", lostErr, runErr)
		}
		runErr = lostErr
	}
	switch {
	case releaseErr == nil:
		return runErr
	case runErr == nil:
		return fmt.Errorf("failed to release migration lock for %s: %w", operation, releaseErr)
	default:
		return fmt.Errorf("%w; additionally failed to release migration lock: %w", runErr, releaseErr)
	}
}

// heldMigrationLockKey stores the migration lock a run holds in its context.
type heldMigrationLockKey struct{}

// heldLock is what the run asks of the migration lock it holds.
type heldLock interface {
	Err() error
}

func withHeldMigrationLock(ctx context.Context, lock heldLock) context.Context {
	return context.WithValue(ctx, heldMigrationLockKey{}, lock)
}

// migrationLockHolderKey stores which lock a run holds, so an operation the
// run calls under it takes no second one. A second acquisition would ask for
// the lock on another session and wait on the run that holds it.
type migrationLockHolderKey struct{}

// migrationLockHolder names a held migration lock: the connection it was
// taken through, its name, and its scope (see [Migrator.migrationLockScope]).
type migrationLockHolder struct {
	conn  *dbschema.DatabaseConnection
	name  string
	scope string
}

// migrationLockSessionKey stores the connection whose own session holds the
// migration lock a provider took for the run.
type migrationLockSessionKey struct{}

// withMigrationLockSession records that conn's own session holds the
// migration lock. A provider's lock guards the writes and commits of the
// session that took it and of no other (see [dbschema.SessionLocker]), so a
// migration body committed on another session commits unguarded: a lock lost
// just before the commit would not stop it.
func withMigrationLockSession(ctx context.Context, conn *dbschema.DatabaseConnection) context.Context {
	return context.WithValue(ctx, migrationLockSessionKey{}, conn)
}

// migrationLockSession returns the connection whose own session holds the
// provider's migration lock, or nil when the run holds no such lock.
func migrationLockSession(ctx context.Context) *dbschema.DatabaseConnection {
	conn, _ := ctx.Value(migrationLockSessionKey{}).(*dbschema.DatabaseConnection)
	return conn
}

// migrationLockLost returns an error once the migration lock the run holds
// has been taken away, and nil while it is held or when the run holds none.
// The run asks before each statement it runs and before each commit, so a
// run that lost its lock starts nothing more and commits nothing more.
func migrationLockLost(ctx context.Context) error {
	lock, ok := ctx.Value(heldMigrationLockKey{}).(heldLock)
	if !ok {
		return nil
	}
	return lock.Err()
}

// acquireMigrationLock takes the shared session advisory lock through
// internal/dblock, keeping a lock table in schema, and converts its timeout
// error into the migrator's typed [MigrationLockTimeoutError], preserving the
// historical error text.
func acquireMigrationLock(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name,
	schema string,
	timeout time.Duration,
) (*dblock.Lock, error) {
	lock, err := dblock.AcquireInSchema(ctx, conn, normalizeMigrationLockName(name), schema, timeout)
	if timeoutErr, ok := errors.AsType[*dblock.TimeoutError](err); ok {
		return nil, &MigrationLockTimeoutError{
			Dialect: timeoutErr.Dialect,
			Name:    timeoutErr.Name,
			Timeout: timeoutErr.Timeout,
		}
	}
	return lock, err
}

// migrationLockSchema is the schema a lock that keeps a table keeps it in: the
// one the revision table is configured into, so that every account recording
// in that table takes one lock, and the session's current schema, where an
// unconfigured revision table lives, when none is configured.
//
// The schema keeps its bytes, as [metadataTableSchemaName] keeps them, so the
// lock and the revision table name one schema.
func (m *Migrator) migrationLockSchema() string {
	if strings.TrimSpace(m.migrationsSchema) == "" {
		return ""
	}
	return m.migrationsSchema
}

// refuseForeignLockTable holds the table the migration lock keeps, on an
// engine whose lock keeps one, to the policy of
// [Migrator.refuseForeignMetadataTable]: the lock writes a row to it, as Ptah
// writes to its other metadata tables, so a table another account owns is
// refused unless [AllowForeignMetadataTableEnvVar] accepts it. On Oracle an
// account owns every table of its schema, so this refuses a lock table in a
// migrations schema another account owns, before the lock creates one there.
func (m *Migrator) refuseForeignLockTable(ctx context.Context) error {
	for _, table := range lockbuiltin.Tables(m.conn.Info().Dialect) {
		if err := m.refuseForeignMetadataTable(ctx, table); err != nil {
			return err
		}
	}
	return nil
}

// migrationLockScope is the schema that is part of the migration lock's
// identity. A lock that keeps a table, as Oracle's does, is a row of the table
// in migrationLockSchema, so a lock of the same name in another schema is
// another lock; an unnamed schema is the session's current one, so naming that
// schema takes the same lock. A lock that keeps no table, such as PostgreSQL's
// advisory lock, is one lock whatever the schema, and its scope is empty.
func (m *Migrator) migrationLockScope() string {
	if len(lockbuiltin.Tables(m.conn.Info().Dialect)) == 0 {
		return ""
	}
	return cmp.Or(m.migrationLockSchema(), m.conn.Info().Schema)
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
