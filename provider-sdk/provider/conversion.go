package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ConvertValuesFeature advertises semantic conversion between desired and
// observed models. Conversion is offline and does not establish coverage.
const ConvertValuesFeature = "values.convert"

// ValueConverter interprets one complete batch in the requested representation.
// It validates source payloads instead of trusting supplied projections, retains
// both source and normalized identity, and returns normalized destination values.
// Errors and cancellation discard the batch; no session is required.
type ValueConverter interface {
	ConvertValues(context.Context, *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error)
}

// ValidateConversionRequest checks negotiated source and destination models,
// captured normalization, and the selected target before owner dispatch. Only
// desired-to-observed and observed-to-desired requests are accepted.
func ValidateConversionRequest(description *protocol.Description, request *protocol.ConvertValuesRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), ConvertValuesFeature) {
		return fmt.Errorf("%w: value conversion is unavailable", ErrProtocol)
	}
	if !schemaRepresentation(request.GetFrom()) || !schemaRepresentation(request.GetTo()) || request.GetFrom() == request.GetTo() {
		return fmt.Errorf("%w: conversion requires distinct schema representations", ErrProtocol)
	}
	source := conversionInputs(request, request.GetFrom(), request.GetValues())
	if err := ValidateNormalizedValues(description, source, &protocol.NormalizedValues{Complete: true, Values: request.GetValues()}); err != nil {
		return err
	}
	for _, value := range request.GetValues() {
		if findValueModel(description, request.GetEngine(), value.GetValue().GetKind(), request.GetTo()) == nil {
			return fmt.Errorf("%w: conversion destination model is not declared", ErrProtocol)
		}
	}
	return nil
}

// ValidateConvertedValues refuses partial batches, changed ordering or identity,
// and output that does not match the negotiated destination model. Payloads may
// change meaning and required features, but every required feature must belong
// to the same selected target profile. The owner defines destination semantics.
func ValidateConvertedValues(description *protocol.Description, request *protocol.ConvertValuesRequest, result *protocol.NormalizedValues) error {
	if err := ValidateConversionRequest(description, request); err != nil {
		return err
	}
	if result == nil || !result.GetComplete() || len(request.GetValues()) != len(result.GetValues()) {
		return fmt.Errorf("%w: incomplete value conversion", ErrProtocol)
	}
	for i, source := range request.GetValues() {
		converted := result.GetValues()[i]
		if converted.GetId() != source.GetId() || converted.GetValue().GetKind() != source.GetValue().GetKind() ||
			!proto.Equal(converted.GetValue().GetOwner(), source.GetValue().GetOwner()) ||
			!proto.Equal(converted.GetValue().GetContext(), source.GetValue().GetContext()) ||
			compareReferences(converted.GetNormalizedOwner(), source.GetNormalizedOwner()) != 0 {
			return fmt.Errorf("%w: conversion changed ordered model or identity", ErrProtocol)
		}
	}
	return ValidateNormalizedValues(description, conversionInputs(request, request.GetTo(), result.GetValues()), result)
}

func conversionInputs(request *protocol.ConvertValuesRequest, representation protocol.ValueInput_Representation, values []*protocol.NormalizedValue) *protocol.NormalizeValuesRequest {
	result := &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures()}
	for _, value := range values {
		result.Values = append(result.Values, &protocol.ValueInput{Id: value.GetId(), Representation: representation, Value: value.GetValue()})
	}
	return result
}

func schemaRepresentation(value protocol.ValueInput_Representation) bool {
	return value == protocol.ValueInput_DESIRED || value == protocol.ValueInput_OBSERVED
}
