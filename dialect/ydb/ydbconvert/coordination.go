package ydbconvert

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// CoordinationService converts standalone node configuration without applying
// defaults or claiming that a projected declaration has been inspected.
type CoordinationService struct{}

// ConvertFeatures preserves raw settings and returns an independent batch.
func (CoordinationService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != "ydb" {
		return nil, fmt.Errorf("%w: YDB conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid coordination conversion direction", schemaext.ErrInvalidValue)
	}
	values := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertCoordination(request.From, value)
		if err != nil {
			return nil, err
		}
		values = append(values, converted)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func convertCoordination(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch node := value.(type) {
	case *ydbcoordination.Desired:
		if node == nil || from != schemaext.Desired {
			return nil, fmt.Errorf("%w: expected a desired coordination node", schemaext.ErrInvalidValue)
		}
		if err := ydbcoordination.Validate(node.Spec); err != nil {
			return nil, err
		}
		return node.Observed(), nil
	case *ydbcoordination.Observed:
		if node == nil || from != schemaext.Observed {
			return nil, fmt.Errorf("%w: expected an observed coordination node", schemaext.ErrInvalidValue)
		}
		if err := ydbcoordination.Validate(node.Spec); err != nil {
			return nil, err
		}
		return node.Desired(), nil
	default:
		return nil, fmt.Errorf("%w: expected a coordination node, got %T", schemaext.ErrInvalidValue, value)
	}
}
