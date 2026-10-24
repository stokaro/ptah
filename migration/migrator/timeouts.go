package migrator

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
	"ptah.run/internal/statementtimeout"
	"ptah.run/migration/migrationfile"
)

type restoreTimeoutsFunc func(context.Context) error

func mergeMigrationTimeouts(defaults, overrides migrationfile.Timeouts) migrationfile.Timeouts {
	merged := defaults
	if overrides.HasLockTimeout {
		merged.LockTimeout = overrides.LockTimeout
		merged.HasLockTimeout = true
	}
	if overrides.HasStatementTimeout {
		merged.StatementTimeout = overrides.StatementTimeout
		merged.HasStatementTimeout = true
	}
	return merged
}

// WithDefaultTimeouts returns a copy of the migrator that applies the provided
// timeouts to migrations that do not override them with file-level directives.
func (m *Migrator) WithDefaultTimeouts(timeouts migrationfile.Timeouts) *Migrator {
	tmp := *m
	tmp.defaultTimeouts = timeouts
	return &tmp
}

// timeoutScope is what a migration's timeouts are attached to: the
// transaction the migration runs in, or the pinned session that runs a
// migration outside one.
type timeoutScope uint8

const (
	// timeoutScopeTransaction bounds the statements of one transaction.
	timeoutScopeTransaction timeoutScope = iota
	// timeoutScopeSession bounds every statement on one pinned session, which
	// is what a no_transaction migration runs on.
	timeoutScopeSession
)

func (m *Migrator) applyTimeoutsWithRestore(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	timeouts migrationfile.Timeouts,
	scope timeoutScope,
) (restoreTimeoutsFunc, error) {
	if timeouts.IsZero() {
		return noopRestoreTimeouts, nil
	}
	if ownerBoundsStatements(conn) {
		// The owner stops each statement itself, from the timeout the
		// statements' context carries; see [withStatementBound].
		return noopRestoreTimeouts, refuseLockTimeoutForOwner(conn.Info().Dialect, timeouts)
	}

	setupStatements, restoreStatements, err := timeoutStatements(
		conn.Info().Dialect, conn.Info().Capabilities, timeouts, scope)
	if err != nil {
		return nil, err
	}

	for _, statement := range setupStatements {
		if err := conn.Writer().ExecuteSQL(ctx, statement); err != nil {
			return nil, fmt.Errorf("failed to apply migration timeout: %w", err)
		}
	}

	return func(ctx context.Context) error {
		for _, statement := range restoreStatements {
			if err := conn.Writer().ExecuteSQL(ctx, statement); err != nil {
				return fmt.Errorf("failed to restore migration timeout: %w", err)
			}
		}
		return nil
	}, nil
}

// refuseTimeoutsTheTargetCannotCarry refuses a migration that declares
// timeouts this target has no statement for, before anything runs.
//
// Whether a target can carry a timeout does not depend on the transaction
// mode, only how it is spelled does, so the question is asked once for every
// mode rather than discovered by the first migration that reaches the server.
func (m *Migrator) refuseTimeoutsTheTargetCannotCarry(migration *Migration, timeouts migrationfile.Timeouts) error {
	if timeouts.IsZero() {
		return nil
	}
	info := m.conn.Info()
	if ownerBoundsStatements(m.conn) {
		if err := refuseLockTimeoutForOwner(info.Dialect, timeouts); err != nil {
			return fmt.Errorf("migration %d declares timeouts: %w", migration.Version, err)
		}
		return nil
	}
	if _, _, err := timeoutStatements(info.Dialect, info.Capabilities, timeouts, timeoutScopeSession); err != nil {
		return fmt.Errorf("migration %d declares timeouts: %w", migration.Version, err)
	}
	return nil
}

// statementBounder is a schema writer whose owner stops a statement at a
// timeout the executor hands it with the statement, rather than through a
// setting the migrator sends first: an executable provider that offers
// units.statement-timeout.
type statementBounder interface {
	BoundsStatements() bool
}

// ownerBoundsStatements reports whether the owner of conn stops each
// statement at the migration's statement timeout itself.
func ownerBoundsStatements(conn *dbschema.DatabaseConnection) bool {
	bounder, ok := conn.SchemaWriter().(statementBounder)
	return ok && bounder.BoundsStatements()
}

// refuseLockTimeoutForOwner refuses a lock timeout on a target whose owner
// stops statements itself: the owner is handed a statement timeout and
// nothing that bounds a lock wait.
func refuseLockTimeoutForOwner(dialect string, timeouts migrationfile.Timeouts) error {
	if !timeouts.HasLockTimeout {
		return nil
	}
	return fmt.Errorf("migration timeouts are not supported for dialect %q: its provider stops a statement at its "+
		"timeout and has no lock wait Ptah bounds, so a lock timeout cannot be honored; remove it", dialect)
}

// withStatementBound returns the context a migration's statements run under
// on conn: ctx with the migration's statement timeout for an owner that stops
// each statement itself, and ctx otherwise.
func withStatementBound(ctx context.Context, conn *dbschema.DatabaseConnection, timeouts migrationfile.Timeouts) context.Context {
	if !timeouts.HasStatementTimeout || !ownerBoundsStatements(conn) {
		return ctx
	}
	return statementtimeout.With(ctx, timeouts.StatementTimeout)
}

// applySessionTimeouts bounds the statements of a migration that runs outside
// a transaction, on the session that runs them.
//
// It has to be the first statement on that session. A resumed attempt replays
// the session state its committed prefix set, and a `SET lock_timeout` the
// file runs itself must land after this one, as it did the first time.
//
// The setting outlives each statement, which is what a transaction-local one
// cannot do here. It does not outlive the migration: the caller restores it
// after the last statement, and the pinned session is discarded afterward
// either way.
//
// The returned context is the one the migration's statements run under. On a
// target that bounds each query itself rather than through a session setting
// (see [boundsEachQuery]) it carries the statement timeout to the executor.
func (m *Migrator) applySessionTimeouts(
	ctx context.Context,
	migration *Migration,
	timeouts migrationfile.Timeouts,
) (context.Context, restoreTimeoutsFunc, error) {
	restore, err := m.applyTimeoutsWithRestore(ctx, m.noTransactionConnection(), timeouts, timeoutScopeSession)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to apply timeouts for migration %d: %w", migration.Version, err)
	}
	if boundsEachQuery(m.connectionDialect()) {
		ctx = withQueryTimeout(ctx, timeouts)
	}
	return withStatementBound(ctx, m.noTransactionConnection(), timeouts), restore, nil
}

func noopRestoreTimeouts(_ context.Context) error {
	return nil
}

func (m *Migrator) restoreTimeouts(ctx context.Context, version int64, restore restoreTimeoutsFunc) error {
	if restore == nil {
		return nil
	}
	if err := restore(ctx); err != nil {
		return fmt.Errorf("failed to restore timeouts for migration %d: %w", version, err)
	}
	return nil
}

func (m *Migrator) restoreTimeoutsAfterFailure(ctx context.Context, version int64, restore restoreTimeoutsFunc, failure error) error {
	if restore == nil {
		return failure
	}
	if err := restore(ctx); err != nil {
		return fmt.Errorf("failed to restore timeouts after migration %d failed: %w (original error: %v)", version, err, failure)
	}
	return failure
}

// timeoutStatements returns the statements that bound a migration on this
// target, and the ones that put the session back afterwards.
//
// The decision is capability.MigrationLockTimeout and
// capability.MigrationStatementTimeout rather than a list of dialect names.
// That list had three entries, and two of the engines it excluded --
// CockroachDB and YugabyteDB -- accept `SET LOCAL statement_timeout` and
// `SET LOCAL lock_timeout` exactly as PostgreSQL does. They speak the
// PostgreSQL wire protocol, so what refused them was Ptah's switch rather than
// the server, on the two deployments where a long lock hurts most
// (stokaro/ptah#1713).
//
// The SPELLING still comes from the dialect, because it differs: PostgreSQL
// sets two transaction-local GUCs, the MySQL family sets and restores two
// session variables, and YDB has no setting at all -- the executor puts a
// deadline on each query instead (see [boundsEachQuery]). A target that
// carries a key and has no spelling here is a programming error rather than
// an unsupported engine, and it says so.
//
// The scope matters only to the PostgreSQL family. `SET LOCAL` outside a
// transaction block is a warning and changes nothing -- measured on PostgreSQL
// 18.6 and CockroachDB v26.3.1 -- so a migration that runs outside a
// transaction takes the session spelling instead, and RESET returns the session
// to the value it started with. The MySQL family's spelling is session-scoped
// already and serves both.
func timeoutStatements(
	dialect string,
	caps capability.Capabilities,
	timeouts migrationfile.Timeouts,
	scope timeoutScope,
) (setupStatements, restoreStatements []string, err error) {
	if err := refuseTimeoutsWithoutCapability(dialect, caps, timeouts); err != nil {
		return nil, nil, err
	}

	spelling := builtin.Lookup(dialect).Timeouts
	if spelling == nil {
		return nil, nil, fmt.Errorf(
			"migration timeouts are declared supported for dialect %q but no statement spelling is "+
				"registered for it",
			dialect)
	}
	spellingScope := migratorext.ScopeTransaction
	if scope == timeoutScopeSession {
		spellingScope = migratorext.ScopeSession
	}
	return spelling.Statements(timeouts, spellingScope)
}

// refuseTimeoutsWithoutCapability refuses a timeout the target has no
// capability for. A target can carry one timeout and not the other, so each
// is asked on its own, and the refusal names the key that is missing.
func refuseTimeoutsWithoutCapability(
	dialect string,
	caps capability.Capabilities,
	timeouts migrationfile.Timeouts,
) error {
	lockMissing := timeouts.HasLockTimeout && !caps.Has(capability.MigrationLockTimeout)
	statementMissing := timeouts.HasStatementTimeout && !caps.Has(capability.MigrationStatementTimeout)
	var reason string
	switch {
	case !caps.Has(capability.MigrationLockTimeout) && !caps.Has(capability.MigrationStatementTimeout) &&
		(lockMissing || statementMissing):
		reason = "this target has no session or transaction timeout Ptah sets and restores around a migration"
	case lockMissing:
		reason = fmt.Sprintf("this target has no lock wait Ptah bounds around a migration (capability %s), "+
			"so a lock timeout cannot be honored; remove it", capability.MigrationLockTimeout)
	case statementMissing:
		reason = fmt.Sprintf("this target has no statement timeout Ptah sets around a migration (capability %s), "+
			"so a statement timeout cannot be honored; remove it", capability.MigrationStatementTimeout)
	default:
		return nil
	}
	return fmt.Errorf("migration timeouts are not supported for dialect %q: %s", dialect, reason)
}
