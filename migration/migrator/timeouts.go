package migrator

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
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
	if _, _, err := timeoutStatements(info.Dialect, info.Capabilities, timeouts, timeoutScopeSession); err != nil {
		return fmt.Errorf("migration %d declares timeouts: %w", migration.Version, err)
	}
	return nil
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
func (m *Migrator) applySessionTimeouts(
	ctx context.Context,
	migration *Migration,
	timeouts migrationfile.Timeouts,
) (restoreTimeoutsFunc, error) {
	restore, err := m.applyTimeoutsWithRestore(ctx, m.noTransactionConnection(), timeouts, timeoutScopeSession)
	if err != nil {
		return nil, fmt.Errorf("failed to apply timeouts for migration %d: %w", migration.Version, err)
	}
	return restore, nil
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
// The decision is capability.MigrationTimeouts rather than a list of dialect
// names. That list had three entries, and two of the engines it excluded --
// CockroachDB and YugabyteDB -- accept `SET LOCAL statement_timeout` and
// `SET LOCAL lock_timeout` exactly as PostgreSQL does. They speak the
// PostgreSQL wire protocol, so what refused them was Ptah's switch rather than
// the server, on the two deployments where a long lock hurts most
// (stokaro/ptah#1713).
//
// The SPELLING still comes from the dialect, because it differs: PostgreSQL
// sets two transaction-local GUCs, and the MySQL family sets and restores two
// session variables. A target that carries the key and has no spelling here is
// a programming error rather than an unsupported engine, and it says so.
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
	normalized := platform.NormalizeDialect(dialect)

	if !caps.Has(capability.MigrationTimeouts) {
		return nil, nil, fmt.Errorf(
			"migration timeouts are not supported for dialect %q: this target has no session or "+
				"transaction timeout Ptah sets and restores around a migration",
			dialect)
	}

	switch normalized {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		if scope == timeoutScopeSession {
			setup, restore := postgresSessionTimeoutStatements(timeouts)
			return setup, restore, nil
		}
		return postgresTimeoutStatements(timeouts), nil, nil
	case platform.MySQL:
		return mysqlTimeoutStatements(timeouts)
	case platform.MariaDB:
		return mariaDBTimeoutStatements(timeouts)
	default:
		return nil, nil, fmt.Errorf(
			"migration timeouts are declared supported for dialect %q but no statement spelling is "+
				"registered for it",
			dialect)
	}
}

func postgresTimeoutStatements(timeouts migrationfile.Timeouts) []string {
	statements := make([]string, 0, 2)
	if timeouts.HasLockTimeout {
		statements = append(statements, "SET LOCAL lock_timeout = '"+durationMillisLiteral(timeouts.LockTimeout)+"'")
	}
	if timeouts.HasStatementTimeout {
		statements = append(statements, "SET LOCAL statement_timeout = '"+durationMillisLiteral(timeouts.StatementTimeout)+"'")
	}
	return statements
}

func postgresSessionTimeoutStatements(timeouts migrationfile.Timeouts) (setupStatements, restoreStatements []string) {
	setup := make([]string, 0, 2)
	restore := make([]string, 0, 2)
	if timeouts.HasLockTimeout {
		setup = append(setup, "SET lock_timeout = '"+durationMillisLiteral(timeouts.LockTimeout)+"'")
		restore = append(restore, "RESET lock_timeout")
	}
	if timeouts.HasStatementTimeout {
		setup = append(setup, "SET statement_timeout = '"+durationMillisLiteral(timeouts.StatementTimeout)+"'")
		restore = append(restore, "RESET statement_timeout")
	}
	return setup, reverseStrings(restore)
}

func mysqlTimeoutStatements(timeouts migrationfile.Timeouts) (setupStatements, restoreStatements []string, err error) {
	setup := make([]string, 0, 4)
	restore := make([]string, 0, 2)
	if timeouts.HasLockTimeout {
		setup = append(setup,
			"SET @ptah_prev_innodb_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout",
			"SET SESSION innodb_lock_wait_timeout = "+strconv.FormatInt(durationSeconds(timeouts.LockTimeout), 10),
		)
		restore = append(restore, "SET SESSION innodb_lock_wait_timeout = @ptah_prev_innodb_lock_wait_timeout")
	}
	if timeouts.HasStatementTimeout {
		setup = append(setup,
			"SET @ptah_prev_max_execution_time = @@SESSION.max_execution_time",
			"SET SESSION max_execution_time = "+strconv.FormatInt(durationMillis(timeouts.StatementTimeout), 10),
		)
		restore = append(restore, "SET SESSION max_execution_time = @ptah_prev_max_execution_time")
	}
	return setup, reverseStrings(restore), nil
}

func mariaDBTimeoutStatements(timeouts migrationfile.Timeouts) (setupStatements, restoreStatements []string, err error) {
	setup := make([]string, 0, 4)
	restore := make([]string, 0, 2)
	if timeouts.HasLockTimeout {
		setup = append(setup,
			"SET @ptah_prev_innodb_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout",
			"SET SESSION innodb_lock_wait_timeout = "+strconv.FormatInt(durationSeconds(timeouts.LockTimeout), 10),
		)
		restore = append(restore, "SET SESSION innodb_lock_wait_timeout = @ptah_prev_innodb_lock_wait_timeout")
	}
	if timeouts.HasStatementTimeout {
		setup = append(setup,
			"SET @ptah_prev_max_statement_time = @@SESSION.max_statement_time",
			"SET SESSION max_statement_time = "+strconv.FormatFloat(timeouts.StatementTimeout.Seconds(), 'f', -1, 64),
		)
		restore = append(restore, "SET SESSION max_statement_time = @ptah_prev_max_statement_time")
	}
	return setup, reverseStrings(restore), nil
}

func reverseStrings(values []string) []string {
	reversed := slices.Clone(values)
	slices.Reverse(reversed)
	return reversed
}

func durationMillisLiteral(duration time.Duration) string {
	return strconv.FormatInt(durationMillis(duration), 10) + "ms"
}

func durationMillis(duration time.Duration) int64 {
	return ceilDurationUnits(duration, time.Millisecond)
}

func durationSeconds(duration time.Duration) int64 {
	return ceilDurationUnits(duration, time.Second)
}

func ceilDurationUnits(duration, unit time.Duration) int64 {
	return int64((duration-1)/unit) + 1
}
