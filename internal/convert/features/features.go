// Package features batches feature values alongside common schema conversion.
// Common fields are copied by their representation converters; this package
// converts every attached facet and named object through the selected runtime.
package features

import (
	"context"
	"fmt"
	"maps"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
)

// RequireFacetPreservation refuses a conversion whose common-object lowering
// omitted or duplicated attached feature state. For example, a primary-key
// constraint folded into table fields has no constraint envelope to retain its
// facets. Such a representation change needs explicit owner support.
func RequireFacetPreservation(source, destination []*schemaext.Facets) error {
	count := func(groups []*schemaext.Facets) map[schemaext.Kind]int {
		result := make(map[schemaext.Kind]int)
		for _, group := range groups {
			for _, kind := range group.Kinds() {
				result[kind]++
			}
		}
		return result
	}
	if !maps.Equal(count(source), count(destination)) {
		return fmt.Errorf("%w: common-object conversion cannot preserve all attached feature facets", schemaext.ErrInvalidValue)
	}
	return nil
}

// Convert changes the representations of named objects and the facets at the
// supplied destinations. Each destination initially holds its source facets.
// They are assigned only after the entire batch, including coverage, succeeds.
// The caller owns the destination schema and publishes it only on success.
func Convert(ctx context.Context, runtime schemaext.ConversionRuntime, target string, from, to schemaext.Representation,
	objects schemaext.Objects, coverage schemaext.Coverage, destinations []*schemaext.Facets,
) (schemaext.Objects, schemaext.Coverage, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	// Built-in aliases share their target services. Preserve names owned by
	// external providers instead of reducing them to an empty dialect.
	if normalized := platform.NormalizeDialect(target); normalized != "" {
		target = normalized
	}
	convertedCoverage, err := runtime.Codecs().ConvertCoverage(ctx, from, to, coverage)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	inputs, err := objects.All()
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	values := make([]schemaext.Value, 0, len(inputs))
	for _, input := range inputs {
		values = append(values, input.Value)
	}
	lengths := make([]int, len(destinations))
	for i, destination := range destinations {
		facets, err := destination.Values()
		if err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, err
		}
		lengths[i] = len(facets)
		values = append(values, facets...)
	}
	// Keep an independent kind ledger: a service may mutate its request slice.
	kinds := make([]schemaext.Kind, len(values))
	for i, value := range values {
		kinds[i] = value.Kind()
	}
	converted, err := runtime.ConvertFeatures(ctx, schemaext.ConversionRequest{Target: target, From: from, To: to, Values: values})
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	if len(converted) != len(kinds) {
		return schemaext.Objects{}, schemaext.Coverage{}, fmt.Errorf("%w: conversion changed the value count", schemaext.ErrInvalidValue)
	}
	converted, err = runtime.Codecs().SnapshotValues(ctx, to, converted)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	for i, value := range converted {
		if value.Kind() != kinds[i] {
			return schemaext.Objects{}, schemaext.Coverage{}, fmt.Errorf("%w: conversion changed an ordered kind", schemaext.ErrInvalidValue)
		}
	}
	for i := range inputs {
		inputs[i].Value = converted[i]
	}
	convertedObjects, err := schemaext.NewObjects(inputs...)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	groups := make([]schemaext.Facets, len(destinations))
	offset := len(inputs)
	for i, length := range lengths {
		groups[i], err = schemaext.NewFacets(converted[offset : offset+length]...)
		if err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, err
		}
		offset += length
	}
	if err := ctx.Err(); err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	for i, group := range groups {
		*destinations[i] = group
	}
	return convertedObjects, convertedCoverage, nil
}
