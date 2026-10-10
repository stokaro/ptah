package ydbconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// facetConversion converts one YDB table facet between its representations.
// D is the declaration and O the observation.
type facetConversion[D, O schemaext.Value] struct {
	// name names the facet in an error, such as "YDB TTL".
	name string
	// observe projects a declaration as the value a statement would leave,
	// refusing an invalid one.
	observe func(D) (O, error)
	// validate refuses an invalid observation.
	validate func(O) error
	// declare captures an observation as a declaration that keeps it.
	declare func(O) D
}

// convert converts an ordered batch without mutating inputs. It accepts only
// the ydb target and opposite desired/observed representations. Invalid values
// or directions wrap schemaext.ErrInvalidValue; other targets wrap
// ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error, including
// cancellation, returns no partial result. An empty batch succeeds.
func (f facetConversion[D, O]) convert(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if request.Target != platform.YDB {
		return nil, fmt.Errorf("%w: %s conversion on %q", ptaherr.ErrUnsupportedDialect, f.name, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid %s conversion direction", schemaext.ErrInvalidValue, f.name)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := f.one(request.From, value)
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

func (f facetConversion[D, O]) one(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	if from == schemaext.Desired {
		desired, ok := value.(D)
		if !ok {
			return nil, fmt.Errorf("%w: expected the desired %s, got %T", schemaext.ErrInvalidValue, f.name, value)
		}
		return f.observe(desired)
	}
	observed, ok := value.(O)
	if !ok {
		return nil, fmt.Errorf("%w: expected the observed %s, got %T", schemaext.ErrInvalidValue, f.name, value)
	}
	if err := f.validate(observed); err != nil {
		return nil, err
	}
	return f.declare(observed), nil
}
