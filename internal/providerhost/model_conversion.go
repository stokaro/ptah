package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/schemaext"
)

func (h *Host) conversionKinds(engine string) []schemaext.Kind {
	var result []schemaext.Kind
	for key, source := range h.models {
		destination := h.models[modelKey{kind: key.kind, representation: schemaext.Observed}]
		if key.representation == schemaext.Desired && source.engines[engine] != nil && destination != nil && destination.engines[engine] != nil {
			result = append(result, key.kind)
		}
	}
	slices.Sort(result)
	return result
}

// ConvertFeatures sends a complete owner batch with its captured target profile.
// Different profiles cannot share a semantic operation. Conversion supplies no
// coverage: the caller must retain the original inspection knowledge separately.
func (t *target) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if request.Target != t.engine.GetId() {
		return nil, fmt.Errorf("%w: conversion target changed", schemaext.ErrInvalidValue)
	}
	remote := &protocol.ConvertValuesRequest{Engine: request.Target, From: protocolRepresentation(request.From), To: protocolRepresentation(request.To)}
	sources := make([]*resolvedModel, len(request.Values))
	destinations := make([]*modelContract, len(request.Values))
	for i, value := range request.Values {
		source := t.host.models[modelKey{kind: value.Kind(), representation: request.From}]
		destination := t.host.models[modelKey{kind: value.Kind(), representation: request.To}]
		if source == nil || destination == nil || source.engines[request.Target] == nil || destination.engines[request.Target] == nil {
			return nil, fmt.Errorf("%w: conversion model is unavailable", schemaext.ErrInvalidValue)
		}
		captured, err := source.value(value)
		if err != nil {
			return nil, err
		}
		features := featureSet(captured.wire.Features)
		if i == 0 {
			remote.TargetFeatures = features
		}
		if captured.wire.Engine != request.Target || !slices.Equal(features, remote.TargetFeatures) || captured.normalized == nil {
			return nil, fmt.Errorf("%w: conversion requires one captured target profile", schemaext.ErrInvalidValue)
		}
		input := proto.CloneOf(captured.normalized)
		input.Id = strconv.Itoa(i)
		remote.Values = append(remote.Values, input)
		sources[i], destinations[i] = captured, destination
	}
	converted, err := t.host.client.ConvertValues(ctx, remote)
	if err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, len(converted.GetValues()))
	for i, value := range converted.GetValues() {
		resolved, err := destinations[i].resolved(sources[i].wire, value)
		if err != nil {
			return nil, err
		}
		result[i] = resolved
	}
	return result, nil
}
