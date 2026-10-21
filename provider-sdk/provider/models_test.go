package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func TestModelDefinitionsAreNegotiatedExactly(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ValueModel)
	}{
		{"missing definition", func(m *protocol.ValueModel) { m.DefinitionJson = nil }},
		{"empty definition", func(m *protocol.ValueModel) { m.DefinitionJson = []byte(`{}`) }},
		{"non-object definition", func(m *protocol.ValueModel) { m.DefinitionJson = []byte(`[]`) }},
		{"noncanonical definition", func(m *protocol.ValueModel) { m.DefinitionJson = []byte(` {"type":"object"} `) }},
		{"duplicate definition key", func(m *protocol.ValueModel) { m.DefinitionJson = []byte(`{"type":"object","type":"object"}`) }},
		{"different definition", func(m *protocol.ValueModel) { m.DefinitionJson = []byte(`{"type":"string"}`) }},
		{"multiline label", func(m *protocol.ValueModel) { m.DisplayName = "retention\nvalues" }},
		{"padded label", func(m *protocol.ValueModel) { m.DisplayName = " retention values" }},
		{"Unicode line separator", func(m *protocol.ValueModel) { m.DisplayName = "retention\u2028values" }},
		{"invalid UTF-8 label", func(m *protocol.ValueModel) { m.DisplayName = string([]byte{0xff}) }},
		{"operation label", func(m *protocol.ValueModel) {
			m.Representation = protocol.ValueInput_OPERATION
			m.SourceFormats = nil
			m.DisplayName = "operations"
		}},
		{"missing kind", func(m *protocol.ValueModel) { m.Kind = "" }},
		{"missing version", func(m *protocol.ValueModel) { m.SchemaVersion = 0 }},
		{"missing encoding", func(m *protocol.ValueModel) { m.Encoding = "" }},
		{"missing projection", func(m *protocol.ValueModel) { m.CanonicalFormat = "" }},
		{"unknown representation", func(m *protocol.ValueModel) { m.Representation = 99 }},
		{"missing digest", func(m *protocol.ValueModel) { m.DefinitionSha256 = "" }},
		{"uppercase digest", func(m *protocol.ValueModel) { m.DefinitionSha256 = strings.ToUpper(testValueDefinition) }},
		{"invalid digest", func(m *protocol.ValueModel) { m.DefinitionSha256 = strings.Repeat("z", 64) }},
		{"unknown source format", func(m *protocol.ValueModel) { m.SourceFormats = []protocol.SourceDocument_Format{99} }},
		{"duplicate source format", func(m *protocol.ValueModel) {
			m.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_YAML}
		}},
		{"observed source", func(m *protocol.ValueModel) {
			m.Representation = protocol.ValueInput_OBSERVED
			m.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r := valueInputs()
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			test.mutate(d.Engines[0].ValueModels[0])
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
			c.Assert(provider.ValidateValueInputs(d, r), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	d, _ := valueInputs()
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, proto.CloneOf(d.Engines[0].ValueModels[0]))
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
	d, r := valueInputs()
	r.Values[0].Value.DefinitionSha256 = strings.Repeat("b", 64)
	c.Assert(provider.ValidateValueInputs(d, r), qt.ErrorIs, provider.ErrProtocol)
	d, r = valueInputs()
	result := normalizedValues(r)
	result.Values[0].CanonicalFormat = "different-format"
	c.Assert(provider.ValidateNormalizedValues(d, r, result), qt.ErrorIs, provider.ErrProtocol)
	result = normalizedValues(r)
	result.Values[0].Value.DefinitionSha256 = strings.Repeat("b", 64)
	c.Assert(provider.ValidateNormalizedValues(d, r, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestModelDisplayNameIsOptionalOwnerMetadata(t *testing.T) {
	for _, label := range []string{"", "retention values", "保存期間"} {
		t.Run(label, func(t *testing.T) {
			c := qt.New(t)
			description, _ := valueInputs()
			description.Engines[0].ValueModels[0].DisplayName = label
			c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
		})
	}
}
