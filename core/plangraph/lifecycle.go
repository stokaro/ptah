package plangraph

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// LifecycleDependencies returns the dependencies that order effects of
// different contributions on one subject the only way the subject's lifecycle
// allows. Each contribution orders its own steps, and neither of two owners
// sees the other's, so a host calls it with every contribution before it
// schedules them, and before rewrites merge contributions of one owner:
//
//   - a drop precedes another contribution's creation of the subject, which
//     hands a name or a path from one owner to another;
//   - a creation precedes another contribution's read;
//   - a read precedes another contribution's drop;
//   - an alteration and another contribution's read are ordered by the
//     reader's placement: an early reader uses the definition the alteration
//     replaces and runs first, and any other reader uses the one it makes.
//
// A pair the contributions already order, either way and through any chain of
// dependencies, is left alone, and Schedule still refuses one ordered against
// its lifecycle. Any other pair of writes is left for Schedule to refuse. The
// result is sorted and does not depend on contribution order. Malformed
// contributions, as Schedule defines them, and cancellation return an error
// and no dependencies.
func LifecycleDependencies[T any](ctx context.Context, contributions ...Contribution[T]) ([]Dependency, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if _, _, err := collect(ctx, contributions); err != nil {
		return nil, err
	}
	subjects, uses := lifecycleUses(contributions)
	parents := make(map[StepID][]StepID)
	for _, contribution := range contributions {
		for _, edge := range contribution.Dependencies {
			parents[edge.After] = append(parents[edge.After], edge.Before)
		}
	}
	var edges []Dependency
	for _, subject := range subjects {
		derived, err := orderUses(ctx, uses[subject.Key()], parents)
		if err != nil {
			return nil, err
		}
		edges = append(edges, derived...)
	}
	slices.SortFunc(edges, func(a, b Dependency) int { return cmp.Or(compareID(a.Before, b.Before), compareID(a.After, b.After)) })
	return edges, nil
}

// lifecycleUse is one step's effect on a subject, with the contribution the
// step belongs to.
type lifecycleUse[T any] struct {
	contribution int
	step         Step[T]
	action       Action
}

// lifecycleUses lists every subject the contributions touch, sorted, and each
// subject's uses in step order.
func lifecycleUses[T any](contributions []Contribution[T]) ([]objectidentity.ID, map[objectidentity.Key][]lifecycleUse[T]) {
	uses := make(map[objectidentity.Key][]lifecycleUse[T])
	var subjects []objectidentity.ID
	for index, contribution := range contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				key := effect.Subject.Key()
				if _, found := uses[key]; !found {
					subjects = append(subjects, effect.Subject)
				}
				uses[key] = append(uses[key], lifecycleUse[T]{contribution: index, step: step, action: effect.Action})
			}
		}
	}
	slices.SortFunc(subjects, schemaext.CompareRefs)
	for _, list := range uses {
		slices.SortStableFunc(list, func(a, b lifecycleUse[T]) int { return compareID(a.step.ID, b.step.ID) })
	}
	return subjects, uses
}

// orderUses derives the dependencies among one subject's uses and records
// each in parents, so a pair a derived edge already orders is left alone.
func orderUses[T any](ctx context.Context, list []lifecycleUse[T], parents map[StepID][]StepID) ([]Dependency, error) {
	var edges []Dependency
	for _, first := range list {
		for _, second := range list {
			if first.contribution == second.contribution {
				continue
			}
			before, after, ok := lifecycleOrder(first.step, first.action, second.step, second.action)
			if !ok {
				continue
			}
			ordered, err := precedes(ctx, before, after, parents)
			if err != nil {
				return nil, err
			}
			reversed, err := precedes(ctx, after, before, parents)
			if err != nil {
				return nil, err
			}
			if ordered || reversed {
				continue
			}
			parents[after] = append(parents[after], before)
			edges = append(edges, Dependency{Before: before, After: after})
		}
	}
	return edges, nil
}

// lifecycleOrder is the order a lifecycle gives the writer first and the
// other step second, when first's action is the one that decides it. A pair
// it does not decide from this side reports false; the caller asks again with
// the steps swapped.
func lifecycleOrder[T any](first Step[T], firstAction Action, second Step[T], secondAction Action) (before, after StepID, ok bool) {
	switch {
	case firstAction == Drop && secondAction == Create:
		return first.ID, second.ID, true
	case firstAction == Create && secondAction == Read:
		return first.ID, second.ID, true
	case firstAction == Drop && secondAction == Read:
		return second.ID, first.ID, true
	case firstAction == Alter && secondAction == Read && second.Placement == PlacementEarly:
		return second.ID, first.ID, true
	case firstAction == Alter && secondAction == Read:
		return first.ID, second.ID, true
	}
	return StepID{}, StepID{}, false
}
