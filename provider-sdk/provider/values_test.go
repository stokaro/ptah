package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

const testValueDefinition = "a2c799262a3ce3c19ef5cdd983bf3d12b43ab3c426227091b909dcb7054738c0"

func valueInputs() (*protocol.Description, *protocol.NormalizeValuesRequest) {
	owner := description()
	owner.Features = append(owner.Features, provider.NormalizeValuesFeature)
	owner.Engines[0].Features = []string{"retention"}
	owner.Engines[0].ValueModels = []*protocol.ValueModel{{Kind: "example.org/fixture/retention", Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json", DefinitionJson: []byte(`{"type":"object"}`), DefinitionSha256: testValueDefinition, CanonicalFormat: "seconds-json-v1"}}
	request := &protocol.NormalizeValuesRequest{Engine: "fixture", TargetFeatures: []string{"retention"}, Values: []*protocol.ValueInput{{
		Id: "first", Representation: protocol.ValueInput_DESIRED, Value: &protocol.Envelope{
			DefinitionSha256: testValueDefinition, ProviderId: owner.ProviderId, Kind: "example.org/fixture/retention", SchemaVersion: 1, Encoding: "json", Payload: []byte(`{"minutes":1}`),
			Owner:            &protocol.Reference{Engine: "fixture", Database: "database", Catalog: "catalog", Schema: "a.b", Path: []string{"a/b", "c"}, Kind: "table", Name: "C", Overload: []string{"integer"}, DisplayName: "Original"},
			RequiredFeatures: []string{"retention"},
		},
	}}}
	return owner, request
}

func normalizedValues(request *protocol.NormalizeValuesRequest) *protocol.NormalizedValues {
	value := proto.CloneOf(request.Values[0].Value)
	value.Payload = []byte(`{"seconds":60}`)
	return &protocol.NormalizedValues{Complete: true, Values: []*protocol.NormalizedValue{{Id: "first", Value: value,
		NormalizedOwner: proto.CloneOf(value.Owner), CanonicalFormat: "seconds-json-v1", CanonicalProjection: []byte(`{"seconds":60}`)}}}
}

func TestValueInputsRefuseUnownedOrAmbiguousBatches(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.NormalizeValuesRequest)
	}{
		{"unadvertised", func(d *protocol.Description, _ *protocol.NormalizeValuesRequest) { d.Features = nil }},
		{"foreign engine", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.Engine = "other" }},
		{"unknown target feature", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) {
			r.TargetFeatures = []string{"other"}
		}},
		{"duplicate target feature", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) {
			r.TargetFeatures = []string{"retention", "retention"}
		}},
		{"missing required feature", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.TargetFeatures = nil }},
		{"nil input", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.Values[0] = nil }},
		{"duplicate ID", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) {
			r.Values = append(r.Values, proto.CloneOf(r.Values[0]))
		}},
		{"unknown representation", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.Values[0].Representation = 99 }},
		{"nil value", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.Values[0].Value = nil }},
		{"foreign provider", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) {
			r.Values[0].Value.ProviderId = "other"
		}},
		{"missing kind", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.Values[0].Value.Kind = "" }},
		{"missing version", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.Values[0].Value.SchemaVersion = 0 }},
		{"missing encoding", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.Values[0].Value.Encoding = "" }},
		{"missing subject", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) { r.Values[0].Value.Owner = nil }},
		{"foreign subject", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) {
			r.Values[0].Value.Owner.Engine = "other"
		}},
		{"duplicate required feature", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest) {
			r.Values[0].Value.RequiredFeatures = []string{"retention", "retention"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner, request := valueInputs()
			c.Assert(provider.ValidateValueInputs(owner, request), qt.IsNil)
			test.mutate(owner, request)
			c.Assert(provider.ValidateValueInputs(owner, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestNormalizedValuesRequireCompleteProvenance(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.NormalizedValues)
	}{
		{"incomplete", func(r *protocol.NormalizedValues) { r.Complete = false }},
		{"missing result", func(r *protocol.NormalizedValues) { r.Values = nil }},
		{"extra result", func(r *protocol.NormalizedValues) { r.Values = append(r.Values, proto.CloneOf(r.Values[0])) }},
		{"nil result", func(r *protocol.NormalizedValues) { r.Values[0] = nil }},
		{"different ID", func(r *protocol.NormalizedValues) { r.Values[0].Id = "second" }},
		{"different provider", func(r *protocol.NormalizedValues) { r.Values[0].Value.ProviderId = "other" }},
		{"different kind", func(r *protocol.NormalizedValues) { r.Values[0].Value.Kind = "other" }},
		{"different version", func(r *protocol.NormalizedValues) { r.Values[0].Value.SchemaVersion++ }},
		{"different encoding", func(r *protocol.NormalizedValues) { r.Values[0].Value.Encoding = "text" }},
		{"changed subject", func(r *protocol.NormalizedValues) { r.Values[0].Value.Owner.Path = []string{"a", "b/c"} }},
		{"removed required feature", func(r *protocol.NormalizedValues) { r.Values[0].Value.RequiredFeatures = nil }},
		{"missing identity", func(r *protocol.NormalizedValues) { r.Values[0].NormalizedOwner = nil }},
		{"foreign identity", func(r *protocol.NormalizedValues) { r.Values[0].NormalizedOwner.Engine = "other" }},
		{"changed identity kind", func(r *protocol.NormalizedValues) { r.Values[0].NormalizedOwner.Kind = "column" }},
		{"missing canonical format", func(r *protocol.NormalizedValues) { r.Values[0].CanonicalFormat = "" }},
		{"missing canonical projection", func(r *protocol.NormalizedValues) { r.Values[0].CanonicalProjection = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner, request := valueInputs()
			result := normalizedValues(request)
			c.Assert(provider.ValidateNormalizedValues(owner, request, result), qt.IsNil)
			test.mutate(result)
			c.Assert(provider.ValidateNormalizedValues(owner, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValueNormalizationDistinguishesEmptyFromMissing(t *testing.T) {
	c := qt.New(t)
	owner, request := valueInputs()
	c.Assert(provider.ValidateValueInputs(owner, nil), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateValueInputs(nil, request), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateNormalizedValues(owner, request, nil), qt.ErrorIs, provider.ErrProtocol)
	request.Values = nil
	c.Assert(provider.ValidateNormalizedValues(owner, request, &protocol.NormalizedValues{Complete: true}), qt.IsNil)
	c.Assert(provider.ValidateNormalizedValues(owner, request, &protocol.NormalizedValues{}), qt.ErrorIs, provider.ErrProtocol)
}
