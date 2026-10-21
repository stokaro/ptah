package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func (p *comparer) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range request.GetValues() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if input.GetRepresentation() == protocol.ValueInput_CHANGE {
			value, err := normalizeChange(input)
			if err != nil {
				return nil, err
			}
			result.Values = append(result.Values, value)
			continue
		}
		captured := proto.CloneOf(input)
		if captured.GetValue().GetKind() == nativeKind {
			captured.Value.Kind = facetKind
		}
		values, err := p.Provider.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(), Values: []*protocol.ValueInput{captured}})
		if err != nil {
			return nil, err
		}
		value := values.Values[0]
		value.Value.Kind = input.GetValue().GetKind()
		result.Values = append(result.Values, value)
	}
	return result, nil
}

type retentionChange struct {
	Before *uint64 `json:"before"`
	After  *uint64 `json:"after"`
}

func normalizeChange(input *protocol.ValueInput) (*protocol.NormalizedValue, error) {
	if input.GetValue().GetKind() != changeKind {
		return nil, fmt.Errorf("unknown change kind")
	}
	canonical, err := modeljson.Canonical(input.GetValue().GetPayload())
	if err != nil {
		return nil, err
	}
	var change retentionChange
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&change); err != nil {
		return nil, err
	}
	if change.Before == nil || change.After == nil {
		return nil, fmt.Errorf("change requires both retention values")
	}
	value := proto.CloneOf(input.GetValue())
	value.Payload = canonical
	return &protocol.NormalizedValue{Id: input.GetId(),
			Value:               value,
			NormalizedOwner:     proto.CloneOf(value.GetOwner()),
			CanonicalFormat:     "retention-change-v1",
			CanonicalProjection: bytes.Clone(canonical),
			ChangeSemantics: &protocol.ChangeSemantics{ReplacesOwner: new(false),
				RemovesState: new(*change.After < *change.Before),
				Impact:       protocol.ChangeSemantics_BEHAVIORAL,
				Reason:       "Retention affects stored data",
				Access:       protocol.ChangeSemantics_UNCHANGED,
				AccessReason: "Retention does not change grants"}},
		nil
}

func (p *comparer) seconds(ctx context.Context, request *protocol.CompareModelsRequest, state *protocol.ComparisonState, record *protocol.ComparisonRecord) (uint64, error) {
	value := record.GetValue()
	values,
		err := p.NormalizeValues(ctx,
		&protocol.NormalizeValuesRequest{Engine: request.GetEngine(),
			TargetFeatures: request.GetTargetFeatures(),
			Values: []*protocol.ValueInput{{Id: value.GetId(),
				Representation: state.GetRepresentation(),
				Value:          value.GetValue()}}})
	if err != nil {
		return 0, err
	}
	var normalized struct {
		Seconds uint64 `json:"seconds"`
	}
	if err := json.Unmarshal(values.Values[0].Value.Payload, &normalized); err != nil {
		return 0, err
	}
	return normalized.Seconds, nil
}
