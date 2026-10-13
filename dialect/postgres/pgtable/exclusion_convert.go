package pgtable

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ExclusionService compares, converts and reverses exclusion state in batches.
// Its zero value is ready for concurrent use and does not access a database.
type ExclusionService struct{}

// ConvertFeatures preserves SQL, comments and dependencies across representations.
// Prediction fills default timing and drops source metadata and probe results.
// Invalid requests and cancellation return no partial batch or input mutation.
func (ExclusionService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, exclusionModelError(request.From, "EXCLUDE conversion requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !slices.Contains(PersistenceTargets(), platform.NormalizeDialect(request.Target)) {
		return nil, fmt.Errorf("%w: EXCLUDE conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if request.From == request.To || request.From != schemaext.Desired && request.From != schemaext.Observed || request.To != schemaext.Desired && request.To != schemaext.Observed {
		return nil, exclusionModelError(request.From, "invalid EXCLUDE conversion direction")
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertExclusion(request.From, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func convertExclusion(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch p := value.(type) {
	case *DesiredExclusion:
		if from == schemaext.Desired {
			if err := ValidateDesiredExclusion(p); err != nil {
				return nil, err
			}
			return observeExclusion(p), nil
		}
	case *ObservedExclusion:
		if from == schemaext.Observed {
			if err := ValidateObservedExclusion(p); err != nil {
				return nil, err
			}
			return declareExclusion(p), nil
		}
	}
	return nil, exclusionModelError(from, fmt.Sprintf("unexpected EXCLUDE conversion value %T", value))
}

func observeExclusion(p *DesiredExclusion) *ObservedExclusion {
	result := ObservedExclusion(comparedExclusion(p))
	result.Dependencies = slices.Clone(p.Dependencies)
	if !result.Deferrable {
		result.Initially = ""
	} else if result.Initially == "" {
		result.Initially = "immediate"
	}
	return &result
}

func declareExclusion(p *ObservedExclusion) *DesiredExclusion {
	result := &DesiredExclusion{Exclusion: Exclusion(*p)}
	result.Dependencies = slices.Clone(p.Dependencies)
	return result
}
