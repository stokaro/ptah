// Package chconvert projects ClickHouse storage settings and refresh schedules
// between desired and observed representations. Target default resolution
// belongs to preparation.
package chconvert

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// Service preserves observed table, skipping-index and refresh-schedule
// properties when reconstructing a declaration. Projection to observed state requires fully explicit intent;
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
		switch v := value.(type) {
		case *chschema.DesiredTable:
			return v.Observed()
		case *chschema.DesiredIndex:
			return v.Observed()
		case *chschema.DesiredRefresh:
			return v.Observed()
		default:
			return nil, fmt.Errorf("%w: expected desired ClickHouse storage settings, got %T", schemaext.ErrInvalidValue, value)
		}
	}
	switch v := value.(type) {
	case *chschema.ObservedTable:
		if err := chschema.ValidateObserved(v); err != nil {
			return nil, err
		}
		return v.Desired(), nil
	case *chschema.ObservedIndex:
		if err := chschema.ValidateObservedIndex(v); err != nil {
			return nil, err
		}
		return v.Desired(), nil
	case *chschema.ObservedRefresh:
		if err := chschema.ValidateObservedRefresh(v); err != nil {
			return nil, err
		}
		return v.Desired(), nil
	default:
		return nil, fmt.Errorf("%w: expected observed ClickHouse storage settings, got %T", schemaext.ErrInvalidValue, value)
	}
}
