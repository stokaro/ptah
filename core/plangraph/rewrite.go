package plangraph

import (
	"context"
	"fmt"
	"slices"
)

// Rewrite replaces host steps with one owner-contributed ordering unit. Sources
// must name distinct host steps with known object footprints. Replacement must
// name an existing step from another owner and account for every source object.
// The owner decides the replacement's semantics and supplies its full safety
// and transaction assessment. A rewrite does not imply transactional execution.
type Rewrite struct {
	Sources     []StepID
	Replacement StepID
}

// Clone returns an independent source list with the same replacement identity.
func (r Rewrite) Clone() Rewrite {
	r.Sources = slices.Clone(r.Sources)
	return r
}

// ScheduleRewritten transfers emission of explicitly claimed host steps to
// their replacements, then uses Schedule for the complete graph. Dependencies
// entering or leaving a source are redirected to its replacement. Edges inside
// one replacement group are consumed by that indivisible ordering unit; the
// owner is responsible for preserving the group's internal semantics.
//
// A source cannot be claimed twice, an unknown footprint cannot be rewritten,
// and replacements must preserve each source object's logical write action. A
// source read may become a write. Physical strategy belongs to the owner: a
// drop-and-create implementation of a logical alteration still declares Alter.
// Matching a footprint checks ownership, not implementation correctness.
// Inputs remain immutable. Invalid claims, conflicts, cycles, and cancellation
// return no usable plan.
func ScheduleRewritten[T any](ctx context.Context, host Contribution[T], rewrites []Rewrite, features ...Contribution[T]) (Plan[T], error) {
	if ctx == nil {
		return Plan[T]{}, fmt.Errorf("%w: scheduling requires a context", ErrInvalid)
	}
	contributions := append([]Contribution[T]{host}, features...)
	steps, edges, err := collect(ctx, contributions)
	if err != nil {
		return Plan[T]{}, err
	}
	redirect, err := rewriteTargets(ctx, host, steps, rewrites)
	if err != nil {
		return Plan[T]{}, err
	}
	rewritten, err := redirectContributions(steps, edges, redirect)
	if err != nil {
		return Plan[T]{}, err
	}
	return Schedule(ctx, rewritten...)
}

func redirectContributions[T any](steps []Step[T], edges []Dependency, redirect map[StepID]StepID) ([]Contribution[T], error) {
	groups := make(map[string]int)
	var rewritten []Contribution[T]
	for _, step := range steps {
		if _, replaced := redirect[step.ID]; replaced {
			continue
		}
		index, found := groups[step.ID.Owner]
		if !found {
			index = len(rewritten)
			groups[step.ID.Owner] = index
			rewritten = append(rewritten, Contribution[T]{Owner: step.ID.Owner})
		}
		rewritten[index].Steps = append(rewritten[index].Steps, step)
	}
	for _, edge := range edges {
		if edge.Before == edge.After {
			return nil, fmt.Errorf("%w: a source step depends on itself", ErrCycle)
		}
		before, after := edge.Before, edge.After
		if replacement, found := redirect[before]; found {
			before = replacement
		}
		if replacement, found := redirect[after]; found {
			after = replacement
		}
		if before == after {
			continue
		}
		// Dependencies do not acquire emission ownership. Attach them to a
		// retained contribution; Schedule still validates both endpoint IDs.
		if len(rewritten) == 0 {
			return nil, fmt.Errorf("%w: dependencies have no retained steps", ErrInvalid)
		}
		rewritten[0].Dependencies = append(rewritten[0].Dependencies, Dependency{Before: before, After: after})
	}
	return rewritten, nil
}

func rewriteTargets[T any](ctx context.Context, host Contribution[T], steps []Step[T], rewrites []Rewrite) (map[StepID]StepID, error) {
	byID := make(map[StepID]Step[T], len(steps))
	for _, step := range steps {
		byID[step.ID] = step
	}
	sources := make(map[StepID]Step[T], len(host.Steps))
	for _, step := range host.Steps {
		sources[step.ID] = step
	}
	redirect := make(map[StepID]StepID)
	claimed := make(map[StepID]bool)
	for _, rewrite := range rewrites {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		replacement, found := byID[rewrite.Replacement]
		if !found || replacement.ID.Owner == host.Owner || claimed[replacement.ID] || len(rewrite.Sources) == 0 {
			return nil, fmt.Errorf("%w: missing, repeated, or host-owned replacement", ErrInvalid)
		}
		claimed[replacement.ID] = true
		for _, id := range rewrite.Sources {
			source, found := sources[id]
			if _, duplicate := redirect[id]; !found || duplicate || len(source.Effects) == 0 {
				return nil, fmt.Errorf("%w: missing, repeated, foreign, or unassessed rewrite source", ErrInvalid)
			}
			if err := validateEffects(ctx, []Step[T]{source}, nil); err != nil {
				return nil, err
			}
			if !coversEffects(source.Effects, replacement.Effects) {
				return nil, fmt.Errorf("%w: replacement does not account for every source object", ErrInvalid)
			}
			redirect[id] = replacement.ID
		}
	}
	return redirect, nil
}

func coversEffects(source, replacement []Effect) bool {
	for _, required := range source {
		found := false
		for _, effect := range replacement {
			if effect.Subject.Key() == required.Subject.Key() && (required.Action == Read || effect.Action == required.Action) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
