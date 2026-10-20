package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func TestYAMLGrammarNegotiationRefusesAmbiguousOwnership(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.Description)
	}{
		{"missing grammar", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys = nil }},
		{"nil claim", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys[0] = nil }},
		{"empty key", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys[0].Key = " " }},
		{"empty selector", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys[0].Selector = " " }},
		{"missing scope", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys[0].Scope = 0 }},
		{"unknown scope", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys[0].Scope = 99 }},
		{"duplicate key", func(d *protocol.Description) {
			d.Engines[0].YamlSourceKeys = append(d.Engines[0].YamlSourceKeys, proto.CloneOf(d.Engines[0].YamlSourceKeys[0]))
		}},
		{"no source service", func(d *protocol.Description) { d.Features = []string{provider.NormalizeValuesFeature} }},
		{"missing models", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys[0].Models = nil }},
		{"unknown model", func(d *protocol.Description) {
			d.Engines[0].YamlSourceKeys[0].Models = []string{"example.org/other/model"}
		}},
		{"unsupported model", func(d *protocol.Description) { d.Engines[0].ValueModels[0].SourceFormats = nil }},
		{"duplicate model", func(d *protocol.Description) {
			claim := d.Engines[0].YamlSourceKeys[0]
			claim.Models = append(claim.Models, claim.Models[0])
		}},
		{"uncovered model", func(d *protocol.Description) {
			model := proto.CloneOf(d.Engines[0].ValueModels[0])
			model.Kind = "example.org/other/model"
			d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, model)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, _, _ := sourceExchange()
			c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
			test.change(description)
			original := proto.CloneOf(description)
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
			c.Assert(proto.Equal(original, description), qt.IsTrue)
		})
	}
}

func TestYAMLGrammarScopesKeepDocumentAndTableKeysDistinct(t *testing.T) {
	c := qt.New(t)
	description, request, _ := sourceExchange()
	documentClaim := proto.CloneOf(description.Engines[0].YamlSourceKeys[0])
	documentClaim.Scope = protocol.YAMLSourceKey_DOCUMENT
	description.Engines[0].YamlSourceKeys = append(description.Engines[0].YamlSourceKeys, documentClaim)
	original := proto.CloneOf(description)
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(proto.Equal(description, original), qt.IsTrue)
	c.Assert(provider.ValidateSourceRequest(description, request), qt.IsNil)
	request.Documents[0].Fragments[0].YamlKey = proto.CloneOf(documentClaim)
	request.Documents[0].Fragments[0].BindingId = ""
	c.Assert(provider.ValidateSourceRequest(description, request), qt.IsNil)
}

func TestYAMLFragmentsRequireTheirFrozenClaimAndBinding(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.DecodeSourceRequest)
	}{
		{"missing claim", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Fragments[0].YamlKey = nil }},
		{"changed key", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Fragments[0].YamlKey.Key = "another" }},
		{"changed context", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].YamlKey.Scope = protocol.YAMLSourceKey_DOCUMENT
		}},
		{"changed claim selector", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Fragments[0].YamlKey.Selector = "another" }},
		{"changed fragment selector", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Fragments[0].Selector = "another" }},
		{"changed claim models", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].YamlKey.Models = []string{"example.org/other/model"}
		}},
		{"narrowed model scope", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Models = nil }},
		{"missing table attachment", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Fragments[0].BindingId = "" }},
		{"conflicting attachment", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].Subject = proto.CloneOf(r.Documents[0].Bindings[0].Object)
		}},
		{"not a table", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Bindings[0].Object.Kind = "view" }},
		{"missing entry key", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Bindings[0].EntryKey = "" }},
		{"claim on annotation", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Format = protocol.SourceDocument_GO_ANNOTATIONS }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, _ := sourceExchange()
			c.Assert(provider.ValidateSourceRequest(description, request), qt.IsNil)
			test.change(request)
			original := proto.CloneOf(request)
			c.Assert(provider.ValidateSourceRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
			c.Assert(proto.Equal(original, request), qt.IsTrue)
		})
	}
}

func TestDocumentYAMLClaimsRefuseInventedAttachments(t *testing.T) {
	c := qt.New(t)
	description, request, _ := sourceExchange()
	description.Engines[0].YamlSourceKeys[0].Scope = protocol.YAMLSourceKey_DOCUMENT
	request.Documents[0].Fragments[0].YamlKey = proto.CloneOf(description.Engines[0].YamlSourceKeys[0])
	c.Assert(provider.ValidateSourceRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
	request.Documents[0].Fragments[0].BindingId = ""
	request.Documents[0].Fragments[0].Subject = proto.CloneOf(request.Documents[0].Bindings[0].Object)
	c.Assert(provider.ValidateSourceRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
	request.Documents[0].Fragments[0].Subject = nil
	c.Assert(provider.ValidateSourceRequest(description, request), qt.IsNil)
}
