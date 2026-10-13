package pgroutine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// PropertiesService converts, compares and reverses PostgreSQL function
// properties through data contracts. Its zero value is concurrent-safe and
// performs no database access.
type PropertiesService struct{}

func propertiesContext(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: function properties require a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch platform.NormalizeDialect(target) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return nil
	default:
		return fmt.Errorf("%w: PostgreSQL function properties on %q", ptaherr.ErrUnsupportedDialect, target)
	}
}

// ConvertFeatures projects declarations as observations or captures observations
// as declarations. It preserves order and leaves inputs unchanged. Invalid
// values, unsupported targets and cancellation return no partial result.
func (PropertiesService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if err := propertiesContext(ctx, request.Target); err != nil {
		return nil, err
	}
	if request.From == request.To || request.From != schemaext.Desired && request.From != schemaext.Observed || request.To != schemaext.Desired && request.To != schemaext.Observed {
		return nil, fmt.Errorf("%w: invalid function properties conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertProperties(request.From, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return completedProperties(ctx, result)
}
func convertProperties(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch p := value.(type) {
	case *DesiredProperties:
		if from == schemaext.Desired {
			if err := ValidateDesiredProperties(p); err != nil {
				return nil, err
			}
			return new(ObservedProperties(*p)), nil
		}
	case *ObservedProperties:
		if from == schemaext.Observed {
			if err := ValidateObservedProperties(p); err != nil {
				return nil, err
			}
			return new(DesiredProperties(*p)), nil
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s function properties value %T", schemaext.ErrInvalidValue, from, value)
}

// CompareFacets compares properties on functions present on both sides. Complete
// coverage interprets an absent facet as server defaults. An undeclared source
// leaves properties unmanaged; explicit knowledge limits remain undecided.
// Cancellation and invalid input return a zero result without mutation.
func (PropertiesService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := propertiesContext(ctx, request.Target); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{PropertiesKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported function properties comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if !HoldsProperties(owner.Subject) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: evaluation properties require a function, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
		change, reason, err := compareProperties(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if change != nil {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: PropertiesKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change}})
		}
		if reason != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: PropertiesKind, Subject: owner.Subject, Reason: reason})
		}
	}
	return completedProperties(ctx, result)
}
func compareProperties(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (*PropertiesChange, string, error) {
	desired, _, err := schemaext.FacetAs[*DesiredProperties](functionFacets(request.Desired, owner.Subject), PropertiesKind)
	if err != nil {
		return nil, "", err
	}
	current, _, err := schemaext.FacetAs[*ObservedProperties](functionFacets(request.Current, owner.Subject), PropertiesKind)
	if err != nil {
		return nil, "", err
	}
	if desired != nil {
		if err := ValidateDesiredProperties(desired); err != nil {
			return nil, "", err
		}
	}
	if current != nil {
		if err := ValidateObservedProperties(current); err != nil {
			return nil, "", err
		}
	}
	if !owner.Desired || !owner.Current || !request.Includes(PropertiesKind, owner.Subject) {
		return nil, "", nil
	}
	if reason := propertiesKnowledgeLimit(request, owner.Subject); reason != "" {
		return nil, reason, nil
	}
	if desired == nil && !describesProperties(request.Desired.Coverage, owner.Subject) {
		return nil, "", nil
	}
	if current == nil && !describesProperties(request.Current.Coverage, owner.Subject) {
		if desired != nil {
			return nil, "the current source does not describe function properties", nil
		}
		return nil, "", nil
	}
	if equalProperties(desired, current) {
		return nil, "", nil
	}
	return (&PropertiesChange{Before: current, After: desired}).Copy(), "", nil
}
func functionFacets(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}
func describesProperties(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(PropertiesKind, subject).State {
	case schemaext.Complete, schemaext.Absent, schemaext.Defaulted:
		return true
	default:
		return false
	}
}

// ReverseChanges swaps each change's operands and predicts the forward state.
// It preserves order and inputs; errors and cancellation return no partial batch.
func (PropertiesService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := propertiesContext(ctx, request.Target); err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*PropertiesChange)
		if !ok || !HoldsProperties(record.Subject) {
			return nil, fmt.Errorf("%w: reversal requires a properties change on a function", schemaext.ErrInvalidValue)
		}
		inverse, err := change.Reverse()
		if err != nil {
			return nil, err
		}
		reversal := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: inverse}, Strategy: "replace the function with its observed evaluation properties"}
		if inverse.Before != nil {
			reversal.ForwardState = []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: PropertiesKind, Value: inverse.Before.Clone()}}
		}
		result = append(result, reversal)
	}
	return completedProperties(ctx, result)
}

func propertiesKnowledgeLimit(request schemaext.FacetComparisonRequest, subject objectidentity.ID) string {
	for _, coverage := range []schemaext.Coverage{request.Desired.Coverage, request.Current.Coverage} {
		knowledge := coverage.Lookup(PropertiesKind, subject)
		explicit := !coverage.SelectKinds([]schemaext.Kind{PropertiesKind}).IsZero()
		if explicit && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable) {
			if knowledge.Reason != "" {
				return knowledge.Reason
			}
			return "function properties are not fully described"
		}
	}
	return ""
}

func equalProperties(desired *DesiredProperties, current *ObservedProperties) bool {
	if desired == nil || current == nil {
		return desired == nil && current == nil
	}
	return Properties(*desired) == Properties(*current)
}

// A cancellation at the reply boundary discards the complete batch too.
func completedProperties[T any](ctx context.Context, result T) (T, error) {
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}
