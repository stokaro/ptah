// Package migrateflags parses the CLI flag values the migration commands
// share into migrator types. The vocabulary each parser accepts is the
// migrator's; this package only owns the string-to-type step, so the library
// surface does not have to carry flag parsing no library caller performs.
package migrateflags

import (
	"fmt"
	"strings"
	"time"

	"ptah.run/core/lintext"
	"ptah.run/internal/atlasmigrateimport"
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

// RunnerFlag is the flag `migrations lint`, `migrations up` and `migrations
// down` read a [Runner] from, and RunnerUsage its help; `migration.runner` in
// ptah.yaml is its project default.
const (
	RunnerFlag  = "runner"
	RunnerUsage = "Tool the migration directory is written for: ptah, which runs each file in a transaction of its own, " +
		"or golang-migrate, which opens none and runs the transactions a file opens itself"
)

// Runner is the tool a migration directory is written for, which decides who
// opens the transaction each of its files runs in. `migrations lint` judges a
// file's transaction control against it, and `migrations up` and `migrations
// down` apply and roll back each file the way it runs one.
type Runner string

const (
	// RunnerPtah is Ptah's migrator, which runs each file in a transaction it
	// opens unless the file opts out. Atlas runs files the same way. It is the
	// default.
	RunnerPtah Runner = "ptah"
	// RunnerGolangMigrate is golang-migrate, which opens no transaction around
	// a file, so a file runs in the transactions it opens itself.
	RunnerGolangMigrate Runner = "golang-migrate"
)

// ParseRunner parses a --runner value. Empty is [RunnerPtah].
func ParseRunner(value string) (Runner, error) {
	switch runner := Runner(strings.ToLower(strings.TrimSpace(value))); runner {
	case "", RunnerPtah:
		return RunnerPtah, nil
	case RunnerGolangMigrate:
		return runner, nil
	default:
		return "", fmt.Errorf("invalid runner %q: expected ptah or golang-migrate", value)
	}
}

// TransactionOwner is who opens the transaction a file runs in under r. It
// is read from the import layout of the same name, which holds the
// measurement of what golang-migrate opens, so the native surface and the
// compatibility surface cannot come to disagree about one tool. Ptah is no
// import layout, and the layouts answer a migrator that wraps each file for
// every tool but golang-migrate.
func (r Runner) TransactionOwner() lintext.TransactionOwner {
	return atlasmigrateimport.Format(r).TransactionOwner()
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
