package provider_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func conversionExchange() (*protocol.Description, *protocol.ConvertValuesRequest, *protocol.NormalizedValues) {
	description, inputs := valueInputs()
	description.Features = append(description.Features, provider.ConvertValuesFeature)
	destination := proto.CloneOf(description.Engines[0].ValueModels[0])
	destination.Representation, destination.SchemaVersion, destination.Encoding = protocol.ValueInput_OBSERVED, 2, "text"
	destination.DefinitionJson = []byte(`{"type":"string"}`)
	digest := sha256.Sum256(destination.DefinitionJson)
	destination.DefinitionSha256 = hex.EncodeToString(digest[:])
	destination.CanonicalFormat = "seconds-text-v2"
	description.Engines[0].ValueModels = append(description.Engines[0].ValueModels, destination)
	normalized := normalizedValues(inputs)
	request := &protocol.ConvertValuesRequest{Engine: inputs.Engine, TargetFeatures: inputs.TargetFeatures, From: protocol.ValueInput_DESIRED, To: protocol.ValueInput_OBSERVED, Values: normalized.Values}
	result := proto.CloneOf(normalized)
	result.Values[0].Value.SchemaVersion, result.Values[0].Value.Encoding = destination.SchemaVersion, destination.Encoding
	result.Values[0].Value.DefinitionSha256 = destination.DefinitionSha256
	result.Values[0].Value.Payload = []byte("60")
	result.Values[0].CanonicalFormat, result.Values[0].CanonicalProjection = destination.CanonicalFormat, []byte("60")
	return description, request, result
}

func TestConversionBindsDistinctModelsWithoutMutatingInput(t *testing.T) {
	c := qt.New(t)
	description, request, result := conversionExchange()
	original := proto.CloneOf(request)
	c.Assert(provider.ValidateConversionRequest(description, request), qt.IsNil)
	c.Assert(provider.ValidateConvertedValues(description, request, result), qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	reverse := &protocol.ConvertValuesRequest{Engine: request.Engine, TargetFeatures: request.TargetFeatures, From: request.To, To: request.From, Values: result.Values}
	c.Assert(provider.ValidateConvertedValues(description, reverse, &protocol.NormalizedValues{Complete: true, Values: request.Values}), qt.IsNil)
	// Conversion may change required capabilities, but cannot acquire an unavailable one.
	result.Values[0].Value.RequiredFeatures = nil
	c.Assert(provider.ValidateConvertedValues(description, request, result), qt.IsNil)
	result.Values[0].Value.RequiredFeatures = []string{"unavailable"}
	c.Assert(provider.ValidateConvertedValues(description, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestConversionRefusesInvalidSourceOrDestination(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.ConvertValuesRequest)
	}{
		{"unadvertised", func(d *protocol.Description, _ *protocol.ConvertValuesRequest) {
			d.Features = []string{provider.NormalizeValuesFeature}
		}},
		{"missing destination", func(d *protocol.Description, _ *protocol.ConvertValuesRequest) {
			d.Engines[0].ValueModels = d.Engines[0].ValueModels[:1]
		}},
		{"same representation", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) { r.To = r.From }},
		{"operation source", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) {
			r.From = protocol.ValueInput_OPERATION
		}},
		{"change destination", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) { r.To = protocol.ValueInput_CHANGE }},
		{"nil source", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) { r.Values[0] = nil }},
		{"source model mismatch", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) { r.Values[0].Value.SchemaVersion++ }},
		{"unavailable feature", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) { r.TargetFeatures = nil }},
		{"missing normalization", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) { r.Values[0].NormalizedOwner = nil }},
		{"wrong projection format", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) {
			r.Values[0].CanonicalFormat = "foreign"
		}},
		{"duplicate ID", func(_ *protocol.Description, r *protocol.ConvertValuesRequest) {
			r.Values = append(r.Values, proto.CloneOf(r.Values[0]))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, _ := conversionExchange()
			test.mutate(d, r)
			c.Assert(provider.ValidateConversionRequest(d, r), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestConversionRefusesPartialOrRenamedResults(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.NormalizedValues)
	}{
		{"incomplete", func(r *protocol.NormalizedValues) { r.Complete = false }},
		{"missing result", func(r *protocol.NormalizedValues) { r.Values = nil }},
		{"extra result", func(r *protocol.NormalizedValues) { r.Values = append(r.Values, proto.CloneOf(r.Values[0])) }},
		{"nil result", func(r *protocol.NormalizedValues) { r.Values[0] = nil }},
		{"changed ID", func(r *protocol.NormalizedValues) { r.Values[0].Id = "foreign" }},
		{"changed kind", func(r *protocol.NormalizedValues) { r.Values[0].Value.Kind = "foreign" }},
		{"changed owner", func(r *protocol.NormalizedValues) { r.Values[0].Value.ProviderId = "foreign" }},
		{"relabelled model", func(r *protocol.NormalizedValues) { r.Values[0].Value.SchemaVersion = 1 }},
		{"wrong digest", func(r *protocol.NormalizedValues) { r.Values[0].Value.DefinitionSha256 = testValueDefinition }},
		{"source spelling", func(r *protocol.NormalizedValues) { r.Values[0].Value.Owner.Name = "c" }},
		{"source display", func(r *protocol.NormalizedValues) { r.Values[0].Value.Owner.DisplayName = "changed" }},
		{"source tuple", func(r *protocol.NormalizedValues) { r.Values[0].Value.Owner.Path = []string{"a", "b/c"} }},
		{"normalized tuple", func(r *protocol.NormalizedValues) { r.Values[0].NormalizedOwner.Path = []string{"a", "b/c"} }},
		{"normalized overload", func(r *protocol.NormalizedValues) { r.Values[0].NormalizedOwner.Overload = []string{"text"} }},
		{"missing projection", func(r *protocol.NormalizedValues) { r.Values[0].CanonicalProjection = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, result := conversionExchange()
			c.Assert(provider.ValidateConvertedValues(d, r, result), qt.IsNil)
			test.mutate(result)
			c.Assert(provider.ValidateConvertedValues(d, r, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestConversionDistinguishesEmptyFromMissing(t *testing.T) {
	c := qt.New(t)
	d, r, _ := conversionExchange()
	c.Assert(provider.ValidateConversionRequest(d, nil), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateConversionRequest(nil, r), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateConvertedValues(d, r, nil), qt.ErrorIs, provider.ErrProtocol)
	r.Values = nil
	c.Assert(provider.ValidateConvertedValues(d, r, &protocol.NormalizedValues{Complete: true}), qt.IsNil)
	c.Assert(provider.ValidateConvertedValues(d, r, &protocol.NormalizedValues{}), qt.ErrorIs, provider.ErrProtocol)
}
