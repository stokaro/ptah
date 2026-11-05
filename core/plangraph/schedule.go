package plangraph

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/schemaext"
)

// Schedule validates all contributions before returning a complete topological
// order. Among the steps ready to run, early steps come first; ties use owner
// and step name, independently of contribution order.
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
			if step.Placement != PlacementDefault && step.Placement != PlacementEarly {
				return nil, nil, fmt.Errorf("%w: unknown placement %q", ErrInvalid, step.Placement)
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
	// Ready steps wait in two queues, each sorted by position, which is owner
	// and name order: early steps leave first.
	var early, ready []int
	enqueue := func(i int) {
		queue := &ready
		if steps[i].Placement == PlacementEarly {
			queue = &early
		}
		position, _ := slices.BinarySearch(*queue, i)
		*queue = slices.Insert(*queue, position, i)
	}
	for i, count := range pending {
		if count == 0 {
			enqueue(i)
		}
	}
	var result []Step[T]
	placed := make([]bool, len(steps))
	for len(early)+len(ready) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var next int
		if len(early) > 0 {
			next, early = early[0], early[1:]
		} else {
			next, ready = ready[0], ready[1:]
		}
		result = append(result, steps[next])
		placed[next] = true
		for _, child := range children[next] {
			pending[child]--
			if pending[child] == 0 {
				enqueue(child)
			}
		}
	}
	if len(result) != len(steps) {
		return nil, fmt.Errorf("%w: %s", ErrCycle, describeCycle(steps, children, placed))
	}
	return result, nil
}

// describeCycle names the steps of one cycle among those order could not
// place, each followed by the step it must finish before, ending where it
// began. The cycle starts at its least step in owner and name order, so the
// text does not depend on contribution order.
//
// A step order left unplaced either lies on a cycle or waits for one. The
// pruning leaves out the steps nothing unplaced waits for, so every step left
// has an unplaced successor and the walk along least successors closes a loop.
func describeCycle[T any](steps []Step[T], children [][]int, placed []bool) string {
	parents := make([][]int, len(steps))
	waiting := make([]int, len(steps))
	alive := make([]bool, len(steps))
	var pruned []int
	for i := range steps {
		if placed[i] {
			continue
		}
		alive[i] = true
		for _, child := range children[i] {
			if !placed[child] {
				parents[child] = append(parents[child], i)
				waiting[i]++
			}
		}
		if waiting[i] == 0 {
			pruned = append(pruned, i)
		}
	}
	for len(pruned) > 0 {
		next := pruned[0]
		pruned = pruned[1:]
		alive[next] = false
		for _, parent := range parents[next] {
			waiting[parent]--
			if waiting[parent] == 0 {
				pruned = append(pruned, parent)
			}
		}
	}
	start := slices.Index(alive, true)
	seen := make(map[int]int)
	var path []int
	for current := start; ; {
		if at, found := seen[current]; found {
			path = path[at:]
			break
		}
		seen[current] = len(path)
		path = append(path, current)
		successor := -1
		for _, child := range children[current] {
			if alive[child] && (successor < 0 || child < successor) {
				successor = child
			}
		}
		current = successor
	}
	least := slices.Index(path, slices.Min(path))
	path = slices.Concat(path[least:], path[:least], path[least:least+1])
	names := make([]string, len(path))
	for i, index := range path {
		names[i] = describeStep(steps[index])
	}
	return strings.Join(names, " -> ")
}

// describeStep names a step for a diagnostic: its owner and name, then the
// objects it declares, which say what an owner's step name may not.
func describeStep[T any](step Step[T]) string {
	name := stepName(step.ID)
	if len(step.Effects) == 0 {
		return name
	}
	effects := make([]string, len(step.Effects))
	for i, effect := range step.Effects {
		effects[i] = string(effect.Action) + " " + effect.Subject.String()
	}
	return name + " (" + strings.Join(effects, ", ") + ")"
}

// stepName renders a step ID as an owner and a name, each quoted, so an owner
// or a name holding a slash still reads as two components.
func stepName(id StepID) string {
	return strconv.Quote(id.Owner) + "/" + strconv.Quote(id.Name)
}
