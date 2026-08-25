package migrationfile

import (
	"fmt"
	"strings"
	"time"
)

const ptahDirectivePrefix = "-- +ptah "

// Timeouts configures per-migration database safety timeouts.
// Empty values mean no timeout is configured.
type Timeouts struct {
	LockTimeout         time.Duration
	StatementTimeout    time.Duration
	HasLockTimeout      bool
	HasStatementTimeout bool
}

// IsZero reports whether no timeout is configured.
func (t Timeouts) IsZero() bool {
	return !t.HasLockTimeout && !t.HasStatementTimeout
}

// ParseTimeouts reads the `-- +ptah lock_timeout=` and
// `-- +ptah statement_timeout=` directives from the region where directives are
// significant.
//
// It takes that region from [directiveRegion] rather than re-deciding where the
// header ends. This loop used to stop at the first executable line while
// [ParseDirectives] scanned the whole file, so two `+ptah` keys written on
// one misplaced line had different fates -- the timeout was dropped and the
// transaction mode was honored. One region is what keeps them the same fact.
func ParseTimeouts(sql string) (Timeouts, error) {
	return ParseTimeoutsForDialect(sql, "")
}

// ParseTimeoutsForDialect is [ParseTimeouts] with the target dialect's comment
// and string rules deciding where the directive header ends. Pass an empty
// dialect only when no target dialect is available.
func ParseTimeoutsForDialect(sql, dialect string) (Timeouts, error) {
	var timeouts Timeouts

	for line := range strings.SplitSeq(directiveRegion(sql, dialect), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		directive, ok := strings.CutPrefix(trimmed, ptahDirectivePrefix)
		if !ok {
			continue
		}

		directive = strings.TrimSpace(directive)
		if directive == "" {
			return Timeouts{}, fmt.Errorf("empty +ptah directive")
		}
		if isCheckDirectiveBody(directive) {
			// `-- +ptah check ...` is an ordered pre-migration check owned by
			// the migrator's ParseChecks. Its quoted assert value may contain
			// spaces and '=',
			// so it must be skipped as a whole line rather than field-split.
			continue
		}

		if err := parseTimeoutDirectiveFields(directive, &timeouts); err != nil {
			return Timeouts{}, err
		}
	}

	return timeouts, nil
}

// parseTimeoutDirectiveFields field-splits a single `-- +ptah` directive body
// and folds any timeout key=value pairs into timeouts. Unknown key=value fields
// belong to other directive families and are skipped; a bare field that is not
// no_transaction is a typo'd directive and rejected.
func parseTimeoutDirectiveFields(directive string, timeouts *Timeouts) error {
	for field := range strings.FieldsSeq(directive) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			if field == DirectiveNoTransaction {
				continue
			}
			return fmt.Errorf("invalid +ptah directive %q", field)
		}

		switch key {
		case "lock_timeout", "lock-timeout":
			duration, err := parsePositiveDuration(value)
			if err != nil {
				return fmt.Errorf("invalid +ptah %s value: %w", key, err)
			}
			timeouts.LockTimeout = duration
			timeouts.HasLockTimeout = true
		case "statement_timeout", "statement-timeout":
			duration, err := parsePositiveDuration(value)
			if err != nil {
				return fmt.Errorf("invalid +ptah %s value: %w", key, err)
			}
			timeouts.StatementTimeout = duration
			timeouts.HasStatementTimeout = true
		}
	}
	return nil
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
