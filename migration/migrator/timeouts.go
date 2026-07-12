package migrator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/platform"
)

const ptahDirectivePrefix = "-- +ptah "

// MigrationTimeouts configures per-migration database safety timeouts.
// Empty values mean no timeout is configured.
type MigrationTimeouts struct {
	LockTimeout         time.Duration
	StatementTimeout    time.Duration
	HasLockTimeout      bool
	HasStatementTimeout bool
}

type restoreTimeoutsFunc func(context.Context) error

// ParseMigrationTimeouts parses CLI timeout values. Empty values are ignored.
func ParseMigrationTimeouts(lockTimeout, statementTimeout string) (MigrationTimeouts, error) {
	var timeouts MigrationTimeouts

	if strings.TrimSpace(lockTimeout) != "" {
		duration, err := parsePositiveDuration(lockTimeout)
		if err != nil {
			return MigrationTimeouts{}, fmt.Errorf("invalid lock timeout: %w", err)
		}
		timeouts.LockTimeout = duration
		timeouts.HasLockTimeout = true
	}

	if strings.TrimSpace(statementTimeout) != "" {
		duration, err := parsePositiveDuration(statementTimeout)
		if err != nil {
			return MigrationTimeouts{}, fmt.Errorf("invalid statement timeout: %w", err)
		}
		timeouts.StatementTimeout = duration
		timeouts.HasStatementTimeout = true
	}

	return timeouts, nil
}

// IsZero reports whether no timeout is configured.
func (t MigrationTimeouts) IsZero() bool {
	return !t.HasLockTimeout && !t.HasStatementTimeout
}

func parseMigrationTimeoutDirectives(sql string) (MigrationTimeouts, error) {
	var timeouts MigrationTimeouts

	for line := range strings.SplitSeq(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, "--") {
			break
		}
		if !strings.HasPrefix(trimmed, ptahDirectivePrefix) {
			continue
		}

		directive := strings.TrimSpace(strings.TrimPrefix(trimmed, ptahDirectivePrefix))
		if directive == "" {
			return MigrationTimeouts{}, fmt.Errorf("empty +ptah directive")
		}

		for field := range strings.FieldsSeq(directive) {
			key, value, ok := strings.Cut(field, "=")
			if !ok {
				return MigrationTimeouts{}, fmt.Errorf("invalid +ptah directive %q", field)
			}
			duration, err := parsePositiveDuration(value)
			if err != nil {
				return MigrationTimeouts{}, fmt.Errorf("invalid +ptah %s value: %w", key, err)
			}

			switch key {
			case "lock_timeout", "lock-timeout":
				timeouts.LockTimeout = duration
				timeouts.HasLockTimeout = true
			case "statement_timeout", "statement-timeout":
				timeouts.StatementTimeout = duration
				timeouts.HasStatementTimeout = true
			default:
				return MigrationTimeouts{}, fmt.Errorf("unknown +ptah directive %q", key)
			}
		}
	}

	return timeouts, nil
}

func parsePositiveDuration(value string) (time.Duration, error) {
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, err
	}
	if duration <= 0 {
		return 0, fmt.Errorf("must be greater than zero")
	}
	return duration, nil
}

func mergeMigrationTimeouts(defaults, overrides MigrationTimeouts) MigrationTimeouts {
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
func (m *Migrator) WithDefaultTimeouts(timeouts MigrationTimeouts) *Migrator {
	tmp := *m
	tmp.defaultTimeouts = timeouts
	return &tmp
}

func (m *Migrator) applyTimeoutsWithRestore(ctx context.Context, timeouts MigrationTimeouts) (restoreTimeoutsFunc, error) {
	if timeouts.IsZero() {
		return noopRestoreTimeouts, nil
	}

	setupStatements, restoreStatements, err := timeoutStatements(m.conn.Info().Dialect, timeouts)
	if err != nil {
		return nil, err
	}

	for _, statement := range setupStatements {
		if err := m.conn.Writer().ExecuteSQL(ctx, statement); err != nil {
			return nil, fmt.Errorf("failed to apply migration timeout: %w", err)
		}
	}

	return func(ctx context.Context) error {
		for _, statement := range restoreStatements {
			if err := m.conn.Writer().ExecuteSQL(ctx, statement); err != nil {
				return fmt.Errorf("failed to restore migration timeout: %w", err)
			}
		}
		return nil
	}, nil
}

func noopRestoreTimeouts(_ context.Context) error {
	return nil
}

func (m *Migrator) restoreTimeouts(ctx context.Context, version int, restore restoreTimeoutsFunc) error {
	if restore == nil {
		return nil
	}
	if err := restore(ctx); err != nil {
		return fmt.Errorf("failed to restore timeouts for migration %d: %w", version, err)
	}
	return nil
}

func (m *Migrator) restoreTimeoutsAfterFailure(ctx context.Context, version int, restore restoreTimeoutsFunc, failure error) error {
	if restore == nil {
		return failure
	}
	if err := restore(ctx); err != nil {
		return fmt.Errorf("failed to restore timeouts after migration %d failed: %w (original error: %v)", version, err, failure)
	}
	return failure
}

func timeoutStatements(dialect string, timeouts MigrationTimeouts) (setupStatements, restoreStatements []string, err error) {
	normalized := platform.NormalizeDialect(dialect)

	switch normalized {
	case platform.Postgres:
		return postgresTimeoutStatements(timeouts), nil, nil
	case platform.MySQL:
		return mysqlTimeoutStatements(timeouts)
	case platform.MariaDB:
		return mariaDBTimeoutStatements(timeouts)
	default:
		return nil, nil, fmt.Errorf("migration timeouts are not supported for dialect %q", dialect)
	}
}

func postgresTimeoutStatements(timeouts MigrationTimeouts) []string {
	statements := make([]string, 0, 2)
	if timeouts.HasLockTimeout {
		statements = append(statements, "SET LOCAL lock_timeout = '"+durationMillisLiteral(timeouts.LockTimeout)+"'")
	}
	if timeouts.HasStatementTimeout {
		statements = append(statements, "SET LOCAL statement_timeout = '"+durationMillisLiteral(timeouts.StatementTimeout)+"'")
	}
	return statements
}

func mysqlTimeoutStatements(timeouts MigrationTimeouts) (setupStatements, restoreStatements []string, err error) {
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

func mariaDBTimeoutStatements(timeouts MigrationTimeouts) (setupStatements, restoreStatements []string, err error) {
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
	reversed := make([]string, len(values))
	for i, value := range values {
		reversed[len(values)-1-i] = value
	}
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
