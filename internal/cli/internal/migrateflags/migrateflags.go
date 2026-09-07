// Package migrateflags parses the CLI flag values the migration commands
// share into migrator types. The vocabulary each parser accepts is the
// migrator's; this package only owns the string-to-type step, so the library
// surface does not have to carry flag parsing no library caller performs.
package migrateflags

import (
	"fmt"
	"strings"
	"time"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// ParseExecOrder parses a CLI/API exec-order value.
func ParseExecOrder(value string) (migrator.ExecOrder, error) {
	switch migrator.ExecOrder(strings.ToLower(strings.TrimSpace(value))) {
	case "", migrator.ExecOrderLinear:
		return migrator.ExecOrderLinear, nil
	case migrator.ExecOrderLinearSkip:
		return migrator.ExecOrderLinearSkip, nil
	case migrator.ExecOrderNonLinear:
		return migrator.ExecOrderNonLinear, nil
	default:
		return "", fmt.Errorf("invalid exec-order %q: expected linear, linear-skip, or non-linear", value)
	}
}

// ParseMigrationLockTimeout parses the session-level advisory lock timeout.
// Empty means wait indefinitely.
func ParseMigrationLockTimeout(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	duration, err := parsePositiveDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid migration lock timeout: %w", err)
	}
	return duration, nil
}

// ParseMigrationTimeouts parses CLI timeout values. Empty values are ignored.
func ParseMigrationTimeouts(lockTimeout, statementTimeout string) (migrationfile.Timeouts, error) {
	var timeouts migrationfile.Timeouts

	if strings.TrimSpace(lockTimeout) != "" {
		duration, err := parsePositiveDuration(lockTimeout)
		if err != nil {
			return migrationfile.Timeouts{}, fmt.Errorf("invalid lock timeout: %w", err)
		}
		timeouts.LockTimeout = duration
		timeouts.HasLockTimeout = true
	}

	if strings.TrimSpace(statementTimeout) != "" {
		duration, err := parsePositiveDuration(statementTimeout)
		if err != nil {
			return migrationfile.Timeouts{}, fmt.Errorf("invalid statement timeout: %w", err)
		}
		timeouts.StatementTimeout = duration
		timeouts.HasStatementTimeout = true
	}

	return timeouts, nil
}

// ParseMigrationTxMode parses the Atlas-compatible migration transaction mode.
func ParseMigrationTxMode(value string) (migrator.MigrationTxMode, error) {
	mode := migrator.MigrationTxMode(strings.ToLower(strings.TrimSpace(value)))
	if mode == "" {
		mode = migrator.MigrationTxModeFile
	}
	switch mode {
	case migrator.MigrationTxModeFile, migrator.MigrationTxModeAll, migrator.MigrationTxModeNone:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid tx-mode %q: expected file, all, or none", value)
	}
}

// ParseRevisionTableFormat normalizes a revision table format value.
func ParseRevisionTableFormat(value string) (migrator.RevisionTableFormat, error) {
	switch migrator.RevisionTableFormat(strings.ToLower(strings.TrimSpace(value))) {
	case "", migrator.RevisionTableFormatPtah:
		return migrator.RevisionTableFormatPtah, nil
	case migrator.RevisionTableFormatAtlas:
		return migrator.RevisionTableFormatAtlas, nil
	default:
		return "", fmt.Errorf("unknown revision table format %q: expected ptah or atlas", value)
	}
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
