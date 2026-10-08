package plangraph

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
)

type use struct {
	step   StepID
	action Action
}

func validateEffects[T any](ctx context.Context, steps []Step[T], edges []Dependency) error {
	uses := make(map[objectidentity.Key][]use)
	writers := make(map[objectidentity.Key]string)
	parents := make(map[StepID][]StepID)
	for _, edge := range edges {
		parents[edge.After] = append(parents[edge.After], edge.Before)
	}
	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		seen := make(map[objectidentity.Key]bool)
		for _, effect := range step.Effects {
			key := effect.Subject.Key()
			if effect.Subject.Kind == "" || effect.Subject.Name.Source == "" || effect.Subject.Name.Normalized == "" || seen[key] ||
				!slices.Contains([]Action{Read, Create, Alter, Drop}, effect.Action) {
				return fmt.Errorf("%w: missing, duplicate, or invalid effect in %v", ErrInvalid, step.ID)
			}
			if _, err := objectidentity.Resolve(objectidentity.Reference{Kind: effect.Subject.Kind, ID: effect.Subject}, []objectidentity.ID{effect.Subject}); err != nil {
				return fmt.Errorf("%w: unresolved effect in %v: %w", ErrInvalid, step.ID, err)
			}
			seen[key] = true
			if effect.Action != Read {
				if owner := writers[key]; owner != "" && owner != step.ID.Owner {
					return fmt.Errorf("%w: %s has writers %q and %q", ErrConflict, effect.Subject, owner, step.ID.Owner)
				}
				writers[key] = step.ID.Owner
			}
			if err := checkUses(ctx, effect, step.ID, uses[key], parents); err != nil {
				return err
			}
			uses[key] = append(uses[key], use{step: step.ID, action: effect.Action})
		}
	}
	return nil
}

func checkUses(ctx context.Context, effect Effect, step StepID, previous []use, parents map[StepID][]StepID) error {
	var lastWrite Action
	for _, earlier := range previous {
		if effect.Action != Read || earlier.action != Read {
			ordered, err := precedes(ctx, earlier.step, step, parents)
			if err != nil {
				return err
			}
			if !ordered {
				return fmt.Errorf("%w: unordered effects on %s in %v and %v", ErrConflict, effect.Subject, earlier.step, step)
			}
		}
		if earlier.action != Read || lastWrite == "" {
			lastWrite = earlier.action
		}
	}
	if (lastWrite == Drop && effect.Action != Create) ||
		(lastWrite != "" && lastWrite != Drop && effect.Action == Create) {
		return fmt.Errorf("%w: %s after %s on %s", ErrConflict, effect.Action, lastWrite, effect.Subject)
	}
	return nil
}

func precedes(ctx context.Context, before, after StepID, parents map[StepID][]StepID) (bool, error) {
	queue := []StepID{after}
	seen := make(map[StepID]bool)
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		next := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if next == before {
			return true, nil
		}
		if !seen[next] {
			seen[next] = true
			queue = append(queue, parents[next]...)
		}
	}
	return false, nil
}
