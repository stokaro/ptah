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

const manualStrategy = "manual"

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
			"%s cannot represent materialized view %q refresh strategy %q; only %q is currently supported",
			normalizedDialect,
			name,
			strategy,
			manualStrategy,
		),
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
