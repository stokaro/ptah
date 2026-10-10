// Package tsconvert projects TimescaleDB values between their desired and
// observed representations. A projection is a prediction, never a claim that
// a server was inspected; source coverage travels separately.
package tsconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// Service converts hypertable and continuous-aggregate values. Its zero value
// is ready for concurrent use and never reads a database.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating its inputs. It
// accepts PostgreSQL-family targets and opposite desired and observed
// representations. Any error, including cancellation, returns no partial
// result. A hypertable whose dimension the catalog did not report has no
// declaration and is refused rather than converted into one that names no
// column.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return nil, fmt.Errorf("%w: TimescaleDB conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid TimescaleDB conversion direction", schemaext.ErrInvalidValue)
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func convert(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch typed := value.(type) {
	case *tsschema.DesiredHypertable:
		if from == schemaext.Desired {
			return typed.Observed()
		}
	case *tsschema.ObservedHypertable:
		if from == schemaext.Observed {
			return typed.Desired()
		}
	case *tsschema.DesiredContinuousAggregate:
		if from == schemaext.Desired {
			return typed.Observed()
		}
	case *tsschema.ObservedContinuousAggregate:
		if from == schemaext.Observed {
			return typed.Desired()
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s TimescaleDB value %T", schemaext.ErrInvalidValue, from, value)
}
