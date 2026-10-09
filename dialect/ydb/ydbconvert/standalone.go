package ydbconvert

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

func convertStandalone[D, O schemaext.Value](ctx context.Context, request schemaext.ConversionRequest, family string, codecs []schemaext.Codec, observe func(D) O, declare func(O) D) ([]schemaext.Value, error) {
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
		return nil, fmt.Errorf("%w: invalid %s conversion direction", schemaext.ErrInvalidValue, family)
	}
	var source schemaext.Codec
	for _, codec := range codecs {
		if codec.Representation == request.From {
			source = codec
		}
	}
	values := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		snapshot, err := source.Clone(value)
		if err != nil {
			return nil, err
		}
		if observed, ok := snapshot.(O); ok {
			values = append(values, declare(observed))
			continue
		}
		desired, ok := snapshot.(D)
		if !ok {
			return nil, fmt.Errorf("%w: unexpected %s conversion operand %T", schemaext.ErrInvalidValue, family, snapshot)
		}
		values = append(values, observe(desired))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return values, nil
}
