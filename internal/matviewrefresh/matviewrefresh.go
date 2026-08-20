// Package matviewrefresh validates materialized-view refresh declarations
// before a target renderer or schema comparison can discard them.
package matviewrefresh

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
)

const (
	manualStrategy       = "manual"
	concurrentlyStrategy = "concurrently"
	// everyStrategyPrefix opens the family of scheduled spellings, such as
	// "every 5 minutes".
	everyStrategyPrefix = "every "
)

// ValidateDeclared refuses every declared refresh strategy Ptah cannot carry
// into target behavior. Manual means Ptah emits no separate refresh operation;
// it is the only strategy the current schema lifecycle implements.
func ValidateDeclared(dialect string, views []goschema.MaterializedView) error {
	for _, view := range views {
		if err := Validate(dialect, view.Name, view.RefreshStrategy); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks one materialized-view refresh strategy.
//
// Manual is the only strategy, and that is a decision rather than a gap. See
// [strategyReason] for what each refused value would have to mean.
func Validate(dialect, name, strategy string) error {
	if Canonical(strategy) == manualStrategy {
		return nil
	}

	normalizedDialect := platform.NormalizeDialect(dialect)
	return &ptaherr.CapabilityError{
		Dialect: normalizedDialect,
		Feature: "materialized view refresh strategy",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s cannot represent materialized view %q refresh strategy %q; %q is the only strategy, %s",
			normalizedDialect,
			name,
			strategy,
			manualStrategy,
			strategyReason(Canonical(strategy)),
		),
	}
}

// strategyReason says why a refused strategy is refused, because "only manual
// is supported" reads as a feature that has not been written yet, and these
// were each decided on measurement (stokaro/ptah#1625).
func strategyReason(canonical string) string {
	switch {
	case canonical == concurrentlyStrategy:
		// Measured on PostgreSQL 18: CREATE MATERIALIZED VIEW populates, and a
		// body change is planned as DROP plus CREATE, which populates again. So
		// after any apply the view is current, and the only moment REFRESH
		// CONCURRENTLY would matter is on a view this run did not touch --
		// a data operation on an unchanged schema, which apply performs for no
		// other object.
		return "and refreshing concurrently is a data operation with no point in a schema apply to attach to: " +
			"every moment the view could be stale is one this run has already recreated it at"
	case strings.HasPrefix(canonical, everyStrategyPrefix):
		// No engine in the family schedules a plain materialized view, so this
		// would be an external object with its own lifecycle rather than a
		// property of the view.
		return "and a schedule is not something Ptah runs: none of the supported engines schedules a plain " +
			"materialized view, so this would be a separate object rather than a property of this one"
	default:
		return "and no other value names anything the supported engines do"
	}
}

// Canonical normalizes the strategy spelling and supplies its default.
func Canonical(strategy string) string {
	canonical := strings.ToLower(strings.TrimSpace(strategy))
	if canonical == "" {
		return manualStrategy
	}
	return canonical
}
