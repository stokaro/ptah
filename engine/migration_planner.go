package engine

import (
	"fmt"
	"slices"

	"ptah.run/core/planext"
	"ptah.run/core/ptaherr"
)

// MigrationPlanner constructs a planner from this runtime's frozen target
// selection. Names and aliases resolve locally. A missing target wraps
// ErrUnsupportedDialect; a target without a planner wraps ErrUnsupportedFeature.
// A factory returning a nil planner is an invalid registration. No lookup calls
// another runtime or reads a process-global registry.
func (r *Runtime) MigrationPlanner(name string, options planext.Options) (planext.Planner, error) {
	target, found := r.lookup(name)
	if !found {
		return nil, &ptaherr.PlanError{Dialect: name, Err: ptaherr.ErrUnsupportedDialect, Message: fmt.Sprintf("unsupported database dialect: %s", name)}
	}
	if target.planner == nil {
		return nil, &ptaherr.PlanError{Dialect: target.name, Err: ptaherr.ErrUnsupportedFeature, Message: fmt.Sprintf("provider %s does not supply a migration planner for %s", target.owner, target.name)}
	}
	options.ConcurrentIndexRefs = slices.Clone(options.ConcurrentIndexRefs)
	options.ConcurrentIndexDropRefs = slices.Clone(options.ConcurrentIndexDropRefs)
	options.SkipChangeKinds = slices.Clone(options.SkipChangeKinds)
	result := target.planner(options)
	if result == nil || nilService(result) {
		return nil, fmt.Errorf("%w: planner factory for target %q returned nil", ErrInvalidRegistration, target.name)
	}
	return result, nil
}
