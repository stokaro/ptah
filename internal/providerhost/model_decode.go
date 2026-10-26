package providerhost

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/schemaext"
)

type modelDecodeGroup struct {
	request *protocol.NormalizeValuesRequest
	indexes []int
}

type modelDecodeKey struct{ engine, features string }

type modelDecodeInput struct {
	contract *modelContract
	wire     modelWire
}

func (h *Host) decodeModels(ctx context.Context, envelopes []schemaext.Envelope) ([]schemaext.Payload, error) {
	inputs, groups, err := h.prepareModelDecode(envelopes)
	if err != nil {
		return nil, err
	}
	result := make([]schemaext.Payload, len(inputs))
	for _, group := range groups {
		normalized, err := h.client.NormalizeValues(ctx, group.request)
		if err != nil {
			return nil, err
		}
		for i, value := range normalized.GetValues() {
			index := group.indexes[i]
			input := inputs[index]
			if value.GetRequiresDatabaseContext() != input.wire.RequiresDatabaseContext {
				return nil, fmt.Errorf("%w: captured dependency readiness changed", schemaext.ErrIncompatibleCodec)
			}
			resolved, err := input.contract.resolved(input.wire, value)
			if err != nil {
				return nil, err
			}
			result[index] = input.contract.payload(resolved)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (h *Host) prepareModelDecode(envelopes []schemaext.Envelope) ([]modelDecodeInput, []modelDecodeGroup, error) {
	inputs := make([]modelDecodeInput, len(envelopes))
	var groups []modelDecodeGroup
	indexes := make(map[modelDecodeKey]int)
	for i, envelope := range envelopes {
		input, err := h.modelDecodeInput(envelope)
		if err != nil {
			return nil, nil, err
		}
		inputs[i] = input
		features := featureSet(input.wire.Features)
		encoded, err := json.Marshal(features)
		if err != nil {
			return nil, nil, err
		}
		key := modelDecodeKey{engine: input.wire.Engine, features: string(encoded)}
		index, found := indexes[key]
		if !found {
			index = len(groups)
			indexes[key] = index
			groups = append(groups, modelDecodeGroup{request: &protocol.NormalizeValuesRequest{Engine: key.engine, TargetFeatures: features}})
		}
		model := input.contract.engines[key.engine]
		group := &groups[index]
		group.indexes = append(group.indexes, i)
		group.request.Values = append(group.request.Values, &protocol.ValueInput{Id: strconv.Itoa(i), Representation: protocolRepresentation(envelope.Representation), Value: &protocol.Envelope{
			ProviderId: h.description.GetProviderId(), Kind: model.GetKind(), SchemaVersion: model.GetSchemaVersion(), Encoding: model.GetEncoding(),
			DefinitionSha256: model.GetDefinitionSha256(), Owner: input.wire.Subject.protocol(), RequiredFeatures: slices.Clone(input.wire.RequiredFeatures), Payload: slices.Clone(input.wire.Payload),
			Context: wireDatabaseContext(input.wire.Context),
		}})
	}
	for _, group := range groups {
		if err := provider.ValidateValueInputs(h.description, group.request); err != nil {
			return nil, nil, err
		}
	}
	return inputs, groups, nil
}

func (h *Host) modelDecodeInput(envelope schemaext.Envelope) (modelDecodeInput, error) {
	contract := h.models[modelKey{kind: envelope.Kind, representation: envelope.Representation}]
	if contract == nil || envelope.Owner != h.description.GetProviderId() || envelope.Format != 1 || envelope.Version != 1 || envelope.Definition != contract.signature {
		return modelDecodeInput{}, fmt.Errorf("%w: provider model binding changed", schemaext.ErrIncompatibleCodec)
	}
	wire, err := schemaext.DecodeJSON[modelWire](envelope.Payload)
	if err != nil {
		return modelDecodeInput{}, err
	}
	if wire.Format != 1 || contract.engines[wire.Engine] == nil || len(featureSet(wire.Features)) != len(wire.Features) {
		return modelDecodeInput{}, fmt.Errorf("%w: unsupported provider model wire format or engine", schemaext.ErrInvalidValue)
	}
	if !wire.Context.IsZero() {
		if err := wire.Context.Validate(); err != nil {
			return modelDecodeInput{}, err
		}
		if wire.RequiresDatabaseContext {
			return modelDecodeInput{}, fmt.Errorf("%w: bound model has unresolved dependencies", schemaext.ErrInvalidValue)
		}
	}
	return modelDecodeInput{contract: contract, wire: wire}, nil
}
