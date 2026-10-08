package plangraph

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// Schedule validates all contributions before returning a complete topological
// order. Ties use owner and step name, independently of contribution order.
// It rejects missing dependencies, cycles, competing writers, unordered uses
// involving a write, and inconsistent declared object lifecycles. These checks
// cover supplied effects only; a missing effect remains an unknown footprint.
// Nil contexts are invalid. Cancellation discards the complete result.
func Schedule[T any](ctx context.Context, contributions ...Contribution[T]) (Plan[T], error) {
	if ctx == nil {
		return Plan[T]{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return Plan[T]{}, err
	}
	steps, edges, err := collect(ctx, contributions)
	if err != nil {
		return Plan[T]{}, err
	}
	ordered, err := order(ctx, steps, edges)
	if err != nil {
		return Plan[T]{}, err
	}
	if err := validateEffects(ctx, ordered, edges); err != nil {
		return Plan[T]{}, err
	}
	if err := ctx.Err(); err != nil {
		return Plan[T]{}, err
	}
	return Plan[T]{Steps: ordered, Dependencies: edges}, nil
}

func compareID(a, b StepID) int {
	return cmp.Or(strings.Compare(a.Owner, b.Owner), strings.Compare(a.Name, b.Name))
}

func collect[T any](ctx context.Context, contributions []Contribution[T]) ([]Step[T], []Dependency, error) {
	var steps []Step[T]
	var edges []Dependency
	seen := make(map[StepID]bool)
	for _, contribution := range contributions {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if !schemaext.Kind(contribution.Owner).Valid() {
			return nil, nil, fmt.Errorf("%w: invalid owner %q", ErrInvalid, contribution.Owner)
		}
		for _, step := range contribution.Steps {
			if step.ID.Owner != contribution.Owner || strings.TrimSpace(step.ID.Name) == "" || seen[step.ID] {
				return nil, nil, fmt.Errorf("%w: misowned, unnamed, or duplicate step %q/%q", ErrInvalid, step.ID.Owner, step.ID.Name)
			}
			if !slices.Contains([]Transaction{TransactionUnknown, TransactionAllowed, TransactionRequired, TransactionForbidden}, step.Transaction) {
				return nil, nil, fmt.Errorf("%w: unknown transaction requirement %q", ErrInvalid, step.Transaction)
			}
			seen[step.ID] = true
			step.Effects = slices.Clone(step.Effects)
			steps = append(steps, step)
		}
		edges = append(edges, contribution.Dependencies...)
	}
	slices.SortFunc(steps, func(a, b Step[T]) int { return compareID(a.ID, b.ID) })
	slices.SortFunc(edges, func(a, b Dependency) int { return cmp.Or(compareID(a.Before, b.Before), compareID(a.After, b.After)) })
	edges = slices.Compact(edges)
	for _, edge := range edges {
		if !seen[edge.Before] || !seen[edge.After] {
			return nil, nil, fmt.Errorf("%w: dependency names a missing step: %v -> %v", ErrInvalid, edge.Before, edge.After)
		}
	}
	return steps, edges, nil
}

func order[T any](ctx context.Context, steps []Step[T], edges []Dependency) ([]Step[T], error) {
	positions := make(map[StepID]int, len(steps))
	for i, step := range steps {
		positions[step.ID] = i
	}
	pending := make([]int, len(steps))
	children := make([][]int, len(steps))
	for _, edge := range edges {
		before, after := positions[edge.Before], positions[edge.After]
		pending[after]++
		children[before] = append(children[before], after)
	}
	var ready []int
	for i, count := range pending {
		if count == 0 {
			ready = append(ready, i)
		}
	}
	var result []Step[T]
	for len(ready) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		next := ready[0]
		ready = ready[1:]
		result = append(result, steps[next])
		for _, child := range children[next] {
			pending[child]--
			if pending[child] == 0 {
				position, _ := slices.BinarySearch(ready, child)
				ready = slices.Insert(ready, position, child)
			}
		}
	}
	if len(result) != len(steps) {
		return nil, ErrCycle
	}
	return result, nil
}
