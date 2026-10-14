package selectext

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// AdditiveRequest supplies changes whose removals must be left out. A service
// receives an independent batch and retains additions inside mixed changes.
type AdditiveRequest struct {
	Changes []schemaext.ChangeRecord
}

// AdditiveDecision accounts for one input change. A nil Value discards it;
// otherwise Value retains its kind and contains no removal. Subject preserves
// the supplied identity, including its source spelling.
type AdditiveDecision struct {
	Subject objectidentity.ID
	Kind    schemaext.Kind
	Value   schemaext.ChangeValue
}

// AdditiveResult accounts for every input in input order. Complete distinguishes
// an intentional empty answer from an unfinished batch.
type AdditiveResult struct {
	Complete  bool
	Decisions []AdditiveDecision
}

// AdditiveService selects additions without external I/O or input mutation.
// Errors and cancellation discard the entire batch. Services support concurrent calls.
type AdditiveService interface {
	SelectAdditions(context.Context, AdditiveRequest) (AdditiveResult, error)
}

// SelectAdditions preserves input order and returns independent changes. Owners
// with an Additive service can retain the additions inside a mixed change.
// Other changes are omitted only when StateRemoval reports a removal.
// A service must account for every input and cannot change its subject or kind.
func (s Set) SelectAdditions(ctx context.Context, request AdditiveRequest) ([]schemaext.ChangeRecord, error) {
	if err := s.projectionContext(ctx); err != nil {
		return nil, err
	}
	changes := make([]schemaext.ChangeRecord, len(request.Changes))
	keep := make([]bool, len(changes))
	for i, change := range request.Changes {
		snapshot, err := change.Clone()
		if err != nil {
			return nil, err
		}
		changes[i] = snapshot
		keep[i] = !schemaext.RemovesState(change.Value)
	}
	for _, extension := range s.extensions {
		if extension.Additive == nil {
			continue
		}
		var owned AdditiveRequest
		var indexes []int
		for i, change := range changes {
			if slices.Contains(extension.ChangeKinds, change.Value.Kind()) {
				snapshot, err := change.Clone()
				if err != nil {
					return nil, err
				}
				owned.Changes = append(owned.Changes, snapshot)
				indexes = append(indexes, i)
			}
		}
		if len(indexes) == 0 {
			continue
		}
		result, err := extension.Additive.SelectAdditions(ctx, owned)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := applyAdditions(changes, keep, indexes, result); err != nil {
			return nil, err
		}
	}
	var result []schemaext.ChangeRecord
	for i, change := range changes {
		if keep[i] {
			result = append(result, change)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s Set) projectionContext(ctx context.Context) error {
	if !s.selected {
		return ErrUnselected
	}
	if ctx == nil {
		return fmt.Errorf("%w: state selection requires a context", schemaext.ErrInvalidValue)
	}
	return ctx.Err()
}

// applyAdditions validates every receipt before its result can leave the batch.
func applyAdditions(changes []schemaext.ChangeRecord, keep []bool, indexes []int, result AdditiveResult) error {
	if !result.Complete || len(result.Decisions) != len(indexes) {
		return fmt.Errorf("%w: additive selection must account for every change", schemaext.ErrInvalidValue)
	}
	for i, decision := range result.Decisions {
		index := indexes[i]
		expected := changes[index]
		if decision.Subject != expected.Subject || decision.Kind != expected.Value.Kind() {
			return fmt.Errorf("%w: additive selection changes a subject or kind", schemaext.ErrInvalidValue)
		}
		keep[index] = decision.Value != nil
		if decision.Value == nil {
			continue
		}
		if err := schemaext.ValidatePayload(decision.Value); err != nil {
			return err
		}
		removal, classified := decision.Value.(schemaext.StateRemoval)
		if decision.Value.Kind() != decision.Kind || !classified || removal.RemovesState() {
			return fmt.Errorf("%w: additive selection returns another kind or a removal", schemaext.ErrInvalidValue)
		}
		snapshot, err := (schemaext.ChangeRecord{Subject: decision.Subject, Value: decision.Value}).Clone()
		if err != nil {
			return err
		}
		changes[index] = snapshot
	}
	return nil
}
