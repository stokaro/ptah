// Package spannerconvert projects the Spanner row deletion policy between desired and
// observed representations without reading a server.
package spannerconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
)

// Service keeps the column and the interval spelling across the conversion. An
// observation becomes an exact declaration; a declaration becomes the policy a
// CREATE or ADD would leave, in its declared spelling. The comparison owner
// reads both spellings of a rewritten interval as the same value.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the spanner target and opposite desired/observed representations.
// Invalid values or directions wrap schemaext.ErrInvalidValue; other targets
// wrap ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error,
// including cancellation, returns no partial result. An empty batch succeeds.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if request.Target != platform.Spanner {
		return nil, fmt.Errorf("%w: Spanner conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid Spanner conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertValue(request.From, value)
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

func convertValue(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	if from == schemaext.Desired {
		desired, ok := value.(*spannerschema.DesiredRowDeletion)
		if !ok {
			return nil, fmt.Errorf("%w: expected a desired Spanner row deletion policy, got %T", schemaext.ErrInvalidValue, value)
		}
		return desired.Observed()
	}
	observed, ok := value.(*spannerschema.ObservedRowDeletion)
	if !ok {
		return nil, fmt.Errorf("%w: expected an observed Spanner row deletion policy, got %T", schemaext.ErrInvalidValue, value)
	}
	if err := spannerschema.ValidateObserved(observed); err != nil {
		return nil, err
	}
	return observed.Desired(), nil
}
