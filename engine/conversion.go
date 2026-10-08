package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Conversion registers one semantic service for a target and a set of model
// kinds. The service receives one batch per request, even when it owns several
// kinds. Targets are canonical names; aliases are resolved before dispatch.
type Conversion struct {
	Target  string
	Kinds   []schemaext.Kind
	Service schemaext.ConversionService
}

type conversionKey struct {
	target string
	kind   schemaext.Kind
}

func (r *Runtime) registerConversion(owner string, declaration Conversion) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete conversion registration for %q", ErrInvalidRegistration, declaration.Target)
	}
	index := len(r.conversionServices)
	for _, kind := range declaration.Kinds {
		for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
			found := slices.ContainsFunc(r.codecs.Definitions(), func(model schemaext.CodecIdentity) bool {
				return model.Kind == kind && model.Representation == representation && model.Owner == owner
			})
			if !found {
				return fmt.Errorf("%w: %q does not own %q/%s", ErrInvalidRegistration, owner, kind, representation)
			}
		}
		key := conversionKey{target: target.name, kind: kind}
		if _, exists := r.conversions[key]; exists {
			return fmt.Errorf("%w: duplicate conversion for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.conversions[key] = index
	}
	r.conversionServices = append(r.conversionServices, declaration.Service)
	return nil
}

// ConvertFeatures dispatches each kind to exactly one registered owner and
// restores the caller's original order. Registration order determines service
// invocation order; no handler is tried as a fallback. All batches are validated
// before any service runs. Errors and cancellation discard the whole result.
func (r *Runtime) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: conversion requires distinct schema representations", schemaext.ErrInvalidValue)
	}
	values, err := r.codecs.SnapshotValues(ctx, request.From, request.Values)
	if err != nil {
		return nil, err
	}
	batches := make([][]int, len(r.conversionServices))
	for index, value := range values {
		service, found := r.conversions[conversionKey{target: selected.name, kind: value.Kind()}]
		if !found {
			return nil, fmt.Errorf("%w: no conversion for %q/%q", ptaherr.ErrUnsupportedFeature, selected.name, value.Kind())
		}
		batches[service] = append(batches[service], index)
	}
	result := make([]schemaext.Value, len(values))
	for service, indices := range batches {
		if len(indices) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := schemaext.ConversionRequest{Target: selected.name, From: request.From, To: request.To, Values: make([]schemaext.Value, len(indices))}
		kinds := make([]schemaext.Kind, len(indices))
		for i, index := range indices {
			batch.Values[i] = values[index]
			kinds[i] = values[index].Kind()
		}
		converted, err := r.convertBatch(ctx, service, batch, kinds)
		if err != nil {
			return nil, err
		}
		for i, index := range indices {
			result[index] = converted[i]
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Runtime) convertBatch(ctx context.Context, service int, request schemaext.ConversionRequest, kinds []schemaext.Kind) ([]schemaext.Value, error) {
	converted, err := r.conversionServices[service].ConvertFeatures(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(converted) != len(kinds) {
		return nil, fmt.Errorf("%w: conversion changed the value count", schemaext.ErrInvalidValue)
	}
	converted, err = r.codecs.SnapshotValues(ctx, request.To, converted)
	if err != nil {
		return nil, err
	}
	for i, kind := range kinds {
		if converted[i].Kind() != kind {
			return nil, fmt.Errorf("%w: conversion changed the ordered kind", schemaext.ErrInvalidValue)
		}
	}
	return converted, nil
}
