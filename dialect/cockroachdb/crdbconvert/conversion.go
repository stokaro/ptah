// Package crdbconvert projects CockroachDB row-level TTL between desired and
// observed representations without reading a server.
package crdbconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// Service keeps every parameter and its spelling across the conversion. An
// observation becomes an exact declaration; a declaration becomes the policy a
// CREATE or SET would leave, in its declared spelling. The comparison owner
// reads both spellings of a rewritten interval as the same value.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the cockroachdb target and opposite desired/observed representations.
// Invalid values or directions wrap schemaext.ErrInvalidValue; other targets
// wrap ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error,
// including cancellation, returns no partial result. An empty batch succeeds.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if request.Target != platform.CockroachDB {
		return nil, fmt.Errorf("%w: CockroachDB conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid CockroachDB conversion direction", schemaext.ErrInvalidValue)
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
		desired, ok := value.(*crdbschema.DesiredRowTTL)
		if !ok {
			return nil, fmt.Errorf("%w: expected a desired CockroachDB row-level TTL, got %T", schemaext.ErrInvalidValue, value)
		}
		return desired.Observed()
	}
	observed, ok := value.(*crdbschema.ObservedRowTTL)
	if !ok {
		return nil, fmt.Errorf("%w: expected an observed CockroachDB row-level TTL, got %T", schemaext.ErrInvalidValue, value)
	}
	if err := crdbschema.ValidateObserved(observed); err != nil {
		return nil, err
	}
	return observed.Desired(), nil
}
