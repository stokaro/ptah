// Package ydbconvert interprets YDB feature values across desired and observed
// schema representations. It contains no registration or transport state.
package ydbconvert

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// Service converts named changefeeds. Omitted target defaults stay omitted in
// the resulting value; target-aware comparison resolves their equivalence.
// Observed disabled state and all topic consumers survive reconstruction.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating its inputs. The
// runtime validates registered concrete types at both sides of this boundary.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if request.Target != "ydb" {
		return nil, fmt.Errorf("%w: YDB conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid YDB conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertChangefeed(request.From, value)
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

func convertChangefeed(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	if err := schemaext.ValidatePayload(value); err != nil {
		return nil, err
	}
	switch from {
	case schemaext.Desired:
		v, ok := value.(*ydbschema.DesiredChangefeed)
		if !ok {
			return nil, fmt.Errorf("%w: expected desired changefeed, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := ydbschema.ValidateChangefeed(v.Spec); err != nil {
			return nil, err
		}
		if err := v.RetainedReplication.Validate(); err != nil {
			return nil, err
		}
		return v.Observed(), nil
	case schemaext.Observed:
		v, ok := value.(*ydbschema.ObservedChangefeed)
		if !ok {
			return nil, fmt.Errorf("%w: expected observed changefeed, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := ydbschema.ValidateChangefeed(v.Spec); err != nil {
			return nil, err
		}
		if err := v.Replication.Validate(); err != nil {
			return nil, err
		}
		return v.Desired(), nil
	default:
		return nil, fmt.Errorf("%w: unknown schema representation", schemaext.ErrInvalidValue)
	}
}
