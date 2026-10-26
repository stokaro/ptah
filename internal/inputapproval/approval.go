// Package inputapproval retains approved input references for exact SQL bodies
// while a selected migration plan crosses a string-based execution interface.
// It never stores input values.
package inputapproval

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/renderer"
)

type contextKey struct{}
type unitKey struct{ target, sql string }
type approvals map[unitKey][]renderer.InputReference

// With captures references after preparation and before the caller's approval
// hook. A repeated SQL body must require exactly the same references.
func With(ctx context.Context, target string, units []renderer.Fragment) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	previous, _ := ctx.Value(contextKey{}).(approvals)
	values := maps.Clone(previous)
	if values == nil {
		values = make(approvals)
	}
	for _, unit := range units {
		if err := unit.Validate(); err != nil {
			return nil, err
		}
		if unit.Native != nil {
			return nil, fmt.Errorf("%w: SQL input approval cannot identify a native body", renderer.ErrInvalidResult)
		}
		key := unitKey{target: target, sql: unit.SQL}
		if refs, present := values[key]; present && !slices.Equal(refs, unit.Inputs) {
			return nil, fmt.Errorf("%w: repeated SQL body has different input references", renderer.ErrInvalidResult)
		}
		values[key] = slices.Clone(unit.Inputs)
	}
	return context.WithValue(ctx, contextKey{}, values), nil
}

// References returns an independent approved set for an exact target and SQL
// body. An unrecognized body has approved no inputs.
func References(ctx context.Context, target, sql string) []renderer.InputReference {
	values, _ := ctx.Value(contextKey{}).(approvals)
	return slices.Clone(values[unitKey{target: target, sql: sql}])
}
