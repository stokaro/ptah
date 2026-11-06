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
// omitted or duplicated attached feature state. For example, a primary key
// that names no column becomes no key of the description, and its facets would
// have nowhere to go. Such a representation change needs explicit owner
// support.
func RequireFacetPreservation(source, destination []*schemaext.Facets) error {
	count := func(groups []*schemaext.Facets) map[schemaext.Kind]int {
		result := make(map[schemaext.Kind]int)
		for _, group := range groups {
			for _, kind := range group.DeclaredKinds() {
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
	converted, err := ConvertParts(ctx, runtime, target, from, to, []Part{{Objects: objects, Coverage: coverage, Destinations: destinations}})
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	return converted[0].Objects, converted[0].Coverage, nil
}

// Part is the share of one schema in a conversion of several: its named
// objects, its coverage, and the destinations of its facets.
type Part struct {
	Objects      schemaext.Objects
	Coverage     schemaext.Coverage
	Destinations []*schemaext.Facets
}

// ConvertParts converts each part as [Convert] converts it alone, and asks the
// runtime's conversion service once for the values of every part rather than
// once for each. A conversion converts each value on its own, so the shared
// call changes no part's result; a schema projected one table at a time sends
// one call instead of one per table. The result holds one part per input, in
// order, with the converted objects and coverage; Destinations is nil, since
// the destinations are assigned in place. Nothing is assigned unless every
// part succeeds, and an error returns no parts.
func ConvertParts(ctx context.Context, runtime schemaext.ConversionRuntime, target string, from, to schemaext.Representation, parts []Part) ([]Part, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	// Built-in aliases share their target services. Preserve names owned by
	// external providers instead of reducing them to an empty dialect.
	if normalized := platform.NormalizeDialect(target); normalized != "" {
		target = normalized
	}
	shares := make([]partShare, len(parts))
	var values []schemaext.Value
	for i, part := range parts {
		share, err := sharePart(ctx, runtime, target, from, to, part)
		if err != nil {
			return nil, err
		}
		shares[i] = share
		values = append(values, share.values...)
	}
	// Keep an independent kind ledger: a service may mutate its request slice.
	kinds := make([]schemaext.Kind, len(values))
	for i, value := range values {
		kinds[i] = value.Kind()
	}
	converted, err := runtime.ConvertFeatures(ctx, schemaext.ConversionRequest{Target: target, From: from, To: to, Values: values})
	if err != nil {
		return nil, err
	}
	if len(converted) != len(kinds) {
		return nil, fmt.Errorf("%w: conversion changed the value count", schemaext.ErrInvalidValue)
	}
	converted, err = runtime.Codecs().SnapshotValues(ctx, to, converted)
	if err != nil {
		return nil, err
	}
	for i, value := range converted {
		if value.Kind() != kinds[i] {
			return nil, fmt.Errorf("%w: conversion changed an ordered kind", schemaext.ErrInvalidValue)
		}
	}
	result := make([]Part, len(parts))
	groups := make([][]schemaext.Facets, len(parts))
	for i, part := range parts {
		share := shares[i]
		result[i], groups[i], err = share.converted(part, converted[:len(share.values)])
		if err != nil {
			return nil, err
		}
		converted = converted[len(share.values):]
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i, part := range parts {
		for j, group := range groups[i] {
			*part.Destinations[j] = group
		}
	}
	return result, nil
}

// partShare is what one part sends to the conversion service: its converted
// coverage, its objects, and the values of its objects and then of its facet
// destinations, with the number of facets each destination holds.
type partShare struct {
	coverage schemaext.Coverage
	inputs   []schemaext.Object
	lengths  []int
	values   []schemaext.Value
}

// sharePart reads what part sends to the conversion service.
func sharePart(ctx context.Context, runtime schemaext.ConversionRuntime, target string, from, to schemaext.Representation, part Part) (partShare, error) {
	convertedCoverage, err := runtime.Codecs().ConvertCoverage(ctx, from, to, part.Coverage)
	if err != nil {
		return partShare{}, err
	}
	objects, err := scopeOffTarget(runtime, target, part.Objects)
	if err != nil {
		return partShare{}, err
	}
	inputs, err := objects.All()
	if err != nil {
		return partShare{}, err
	}
	share := partShare{coverage: convertedCoverage, inputs: inputs, lengths: make([]int, len(part.Destinations))}
	for _, input := range inputs {
		share.values = append(share.values, input.Value)
	}
	for j, destination := range part.Destinations {
		facets, err := destination.Values()
		if err != nil {
			return partShare{}, err
		}
		share.lengths[j] = len(facets)
		share.values = append(share.values, facets...)
	}
	return share, nil
}

// converted is the part the converted values of this share make, and the facet
// groups its destinations take once every part has converted.
func (s partShare) converted(part Part, values []schemaext.Value) (Part, []schemaext.Facets, error) {
	for j := range s.inputs {
		s.inputs[j].Value = values[j]
	}
	convertedObjects, err := schemaext.NewObjects(s.inputs...)
	if err != nil {
		return Part{}, nil, err
	}
	groups, err := convertedFacetGroups(part.Destinations, s.lengths, values[len(s.inputs):])
	if err != nil {
		return Part{}, nil, err
	}
	return Part{Objects: convertedObjects, Coverage: s.coverage}, groups, nil
}

// scopeOffTarget leaves out an object the target does not serve, whose owner
// answers that the target skips it: it is not part of the target's state, so
// no service of the target converts it. A runtime that cannot scope leaves
// objects as they are.
func scopeOffTarget(runtime schemaext.ConversionRuntime, target string, objects schemaext.Objects) (schemaext.Objects, error) {
	scoper, ok := runtime.(schemaext.OffTargetScoper)
	if !ok || objects.Len() == 0 {
		return objects, nil
	}
	kept, _, err := scoper.ScopeOffTarget(target, objects)
	return kept, err
}

// Replacing each converted value retains source bindings, including exclusions
// with no payload. Rebuilding the groups from Values would lose that intent.
func convertedFacetGroups(destinations []*schemaext.Facets, lengths []int, converted []schemaext.Value) ([]schemaext.Facets, error) {
	groups := make([]schemaext.Facets, len(destinations))
	offset := 0
	for i, length := range lengths {
		groups[i] = *destinations[i]
		for _, value := range converted[offset : offset+length] {
			replaced, err := groups[i].Replace(value)
			if err != nil {
				return nil, err
			}
			groups[i] = replaced
		}
		offset += length
	}
	return groups, nil
}
