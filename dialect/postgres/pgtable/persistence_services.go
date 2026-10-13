package pgtable

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// PersistenceService converts, compares and reverses PostgreSQL table
// persistence through data contracts. Its zero value is concurrent-safe and
// performs no database access.
type PersistenceService struct{}

func persistenceContext(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: table persistence requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !slices.Contains(PersistenceTargets(), platform.NormalizeDialect(target)) {
		return fmt.Errorf("%w: PostgreSQL table persistence on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

// ConvertFeatures projects declarations as observations or captures observations
// as declarations. It preserves order and leaves inputs unchanged. Invalid
// values, unknown targets and cancellation return no partial result. This
// changes the representation only; it does not establish engine support or
// report a database observation.
func (PersistenceService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertTableFeatures(ctx, request, "table persistence", convertPersistence)
}

func convertTableFeatures(ctx context.Context, request schemaext.ConversionRequest, label string, convert func(schemaext.Representation, schemaext.Value) (schemaext.Value, error)) ([]schemaext.Value, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return nil, err
	}
	if request.From == request.To || request.From != schemaext.Desired && request.From != schemaext.Observed || request.To != schemaext.Desired && request.To != schemaext.Observed {
		return nil, fmt.Errorf("%w: invalid %s conversion direction", schemaext.ErrInvalidValue, label)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convert(request.From, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return completedPersistence(ctx, result)
}
func convertPersistence(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch p := value.(type) {
	case *DesiredPersistence:
		if from == schemaext.Desired {
			if err := ValidateDesiredPersistence(p); err != nil {
				return nil, err
			}
			return new(ObservedPersistence(*p)), nil
		}
	case *ObservedPersistence:
		if from == schemaext.Observed {
			if err := ValidateObservedPersistence(p); err != nil {
				return nil, err
			}
			return new(DesiredPersistence(*p)), nil
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s table persistence value %T", schemaext.ErrInvalidValue, from, value)
}

// CompareFacets compares persistence on tables present on both sides. Complete
// coverage interprets an absent facet as server defaults. An undeclared source
// leaves persistence unmanaged; explicit knowledge limits remain undecided.
// Cancellation and invalid input return a zero result without mutation.
func (PersistenceService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{PersistenceKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported table persistence comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if !HoldsPersistence(owner.Subject) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: table persistence requires a table, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
		change, reason, err := comparePersistence(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if change != nil {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: PersistenceKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change}})
		}
		if reason != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: PersistenceKind, Subject: owner.Subject, Reason: reason})
		}
	}
	return completedPersistence(ctx, result)
}
func comparePersistence(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (*PersistenceChange, string, error) {
	desired, _, err := schemaext.FacetAs[*DesiredPersistence](tableFacets(request.Desired, owner.Subject), PersistenceKind)
	if err != nil {
		return nil, "", err
	}
	current, _, err := schemaext.FacetAs[*ObservedPersistence](tableFacets(request.Current, owner.Subject), PersistenceKind)
	if err != nil {
		return nil, "", err
	}
	if desired != nil {
		if err := ValidateDesiredPersistence(desired); err != nil {
			return nil, "", err
		}
	}
	if current != nil {
		if err := ValidateObservedPersistence(current); err != nil {
			return nil, "", err
		}
	}
	if !SupportsPersistence(request.Target) {
		return nil, "", refuseForeignPersistence(request.Target, desired)
	}
	if !owner.Desired || !owner.Current || !request.Includes(PersistenceKind, owner.Subject) {
		return nil, "", nil
	}
	if reason := persistenceKnowledgeLimit(request, owner.Subject); reason != "" {
		return nil, reason, nil
	}
	if desired == nil && !describesPersistence(request.Desired.Coverage, owner.Subject) {
		return nil, "", nil
	}
	if current == nil && !describesPersistence(request.Current.Coverage, owner.Subject) {
		return nil, "the current source does not describe table persistence", nil
	}
	if equalPersistence(desired, current) {
		return nil, "", nil
	}
	return (&PersistenceChange{Before: current, After: desired}).Copy(), "", nil
}
func tableFacets(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}
func describesPersistence(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(PersistenceKind, subject).State {
	case schemaext.Complete, schemaext.Absent, schemaext.Defaulted:
		return true
	default:
		return false
	}
}

// ReverseChanges swaps each change's operands and predicts the forward state.
// It preserves order and inputs; errors and cancellation return no partial batch.
func (PersistenceService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return nil, err
	}
	if !SupportsPersistence(request.Target) {
		return nil, fmt.Errorf("%w: UNLOGGED changes require PostgreSQL", ptaherr.ErrUnsupportedDialect)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*PersistenceChange)
		if !ok || !HoldsPersistence(record.Subject) {
			return nil, fmt.Errorf("%w: reversal requires a persistence change on a table", schemaext.ErrInvalidValue)
		}
		inverse, err := change.Reverse()
		if err != nil {
			return nil, err
		}
		reversal := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: inverse}, Strategy: "restore the observed table persistence",
			ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: PersistenceKind}}}
		if inverse.Before != nil {
			reversal.ForwardState[0].Value = inverse.Before.Clone()
		}
		result = append(result, reversal)
	}
	return completedPersistence(ctx, result)
}

func persistenceKnowledgeLimit(request schemaext.FacetComparisonRequest, subject objectidentity.ID) string {
	for _, coverage := range []schemaext.Coverage{request.Desired.Coverage, request.Current.Coverage} {
		knowledge := coverage.Lookup(PersistenceKind, subject)
		explicit := !coverage.SelectKinds([]schemaext.Kind{PersistenceKind}).IsZero()
		if explicit && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable) {
			if knowledge.Reason != "" {
				return knowledge.Reason
			}
			return "table persistence is not fully described"
		}
	}
	return ""
}

func equalPersistence(desired *DesiredPersistence, current *ObservedPersistence) bool {
	return persistenceDesiredUnlogged(desired) == persistenceObservedUnlogged(current)
}

// A cancellation at the reply boundary discards the complete batch too.
func completedPersistence[T any](ctx context.Context, result T) (T, error) {
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}

func persistenceDesiredUnlogged(value *DesiredPersistence) bool {
	return value != nil && value.Unlogged
}
func persistenceObservedUnlogged(value *ObservedPersistence) bool {
	return value != nil && value.Unlogged
}

func refuseForeignPersistence(target string, desired *DesiredPersistence) error {
	if platform.NormalizeDialect(target) == platform.YDB && desired != nil && desired.Unlogged {
		return fmt.Errorf("%w: UNLOGGED is PostgreSQL's; YDB has no unlogged table", ptaherr.ErrUnsupportedFeature)
	}
	return nil
}
