// Package chconvert projects ClickHouse table settings between desired and
// observed representations. Target default resolution belongs to preparation.
package chconvert

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// Service preserves every observed table property when reconstructing a
// declaration. Projection to observed state requires fully explicit intent;
// unresolved settings cannot become claims about an inspected database.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the clickhouse target and opposite desired/observed representations.
// Invalid values or directions wrap schemaext.ErrInvalidValue; other targets
// wrap ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error,
// including cancellation, returns no partial result. An empty batch succeeds.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if request.Target != "clickhouse" {
		return nil, fmt.Errorf("%w: ClickHouse conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid ClickHouse conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertTable(request.From, value)
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

func convertTable(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	if from == schemaext.Desired {
		v, ok := value.(*chschema.DesiredTable)
		if !ok {
			return nil, fmt.Errorf("%w: expected a desired ClickHouse table, got %T", schemaext.ErrInvalidValue, value)
		}
		observed, err := v.Observed()
		if err != nil {
			return nil, err
		}
		return observed, nil
	}
	v, ok := value.(*chschema.ObservedTable)
	if !ok {
		return nil, fmt.Errorf("%w: expected an observed ClickHouse table, got %T", schemaext.ErrInvalidValue, value)
	}
	if err := chschema.ValidateObserved(v); err != nil {
		return nil, err
	}
	return v.Desired(), nil
}
