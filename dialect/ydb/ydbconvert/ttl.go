package ydbconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TTLService converts a TTL between its representations. An observation
// becomes an exact declaration without its run interval; a declaration becomes
// the TTL a CREATE or SET would leave, its interval written as YDB shows the
// seconds it keeps.
type TTLService struct{}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the ydb target and opposite desired/observed representations. Invalid
// values or directions wrap schemaext.ErrInvalidValue; other targets
// wrap ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error,
// including cancellation, returns no partial result. An empty batch succeeds.
func (TTLService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if request.Target != platform.YDB {
		return nil, fmt.Errorf("%w: YDB TTL conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid YDB TTL conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertTTL(request.From, value)
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

func convertTTL(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	if from == schemaext.Desired {
		desired, ok := value.(*ydbschema.DesiredTTL)
		if !ok {
			return nil, fmt.Errorf("%w: expected a desired YDB TTL, got %T", schemaext.ErrInvalidValue, value)
		}
		return desired.Observed()
	}
	observed, ok := value.(*ydbschema.ObservedTTL)
	if !ok {
		return nil, fmt.Errorf("%w: expected an observed YDB TTL, got %T", schemaext.ErrInvalidValue, value)
	}
	if err := ydbschema.ValidateObservedTTL(observed); err != nil {
		return nil, err
	}
	return observed.Desired(), nil
}
