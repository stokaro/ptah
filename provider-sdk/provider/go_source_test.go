package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// goSourceExchange turns the YAML exchange into one that declares a table
// directive with two attributes and sends one fragment written with it.
func goSourceExchange() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource) {
	description, request, result := sourceExchange()
	model := description.Engines[0].ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_GO_ANNOTATIONS}
	description.Engines[0].YamlSourceKeys = nil
	claim := &protocol.GoSourceDirective{
		Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:fixture:retention", Selector: "retention",
		Attributes: []string{"seconds", "minutes"}, Models: []string{model.Kind},
	}
	description.Engines[0].GoSourceDirectives = []*protocol.GoSourceDirective{claim}
	document := request.Documents[0]
	document.Format = protocol.SourceDocument_GO_ANNOTATIONS
	document.Models = []*protocol.ValueModel{proto.CloneOf(model)}
	fragment := document.Fragments[0]
	fragment.YamlKey = nil
	fragment.GoDirective = proto.CloneOf(claim)
	fragment.Content = &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Mapping{
		Mapping: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "seconds", KeySpan: sourceSpan(), Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Number{Number: "60"}}}}},
	}}}
	result.Documents[0].Coverage.Kinds[0].Model = proto.CloneOf(model)
	return description, request, result
}

func TestGoSourceExchangeHappyPath(t *testing.T) {
	c := qt.New(t)
	description, request, result := goSourceExchange()
	original := proto.CloneOf(description)
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(description, request), qt.IsNil)
	c.Assert(provider.ValidateDecodedSource(description, request, result), qt.IsNil)
	c.Assert(proto.Equal(original, description), qt.IsTrue)
}

func TestGoSourceDirectiveNegotiationFailurePath(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.Description)
	}{
		{"nil claim", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0] = nil }},
		{"empty directive", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0].Directive = " " }},
		{"directive with space", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0].Directive = "ptah:fixture retention" }},
		{"empty selector", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0].Selector = " " }},
		{"missing scope", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0].Scope = 0 }},
		{"unknown scope", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0].Scope = 99 }},
		{"empty attribute", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0].Attributes = []string{""} }},
		{"attribute with equals sign", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0].Attributes = []string{"a=b"} }},
		{"duplicate attribute", func(d *protocol.Description) {
			d.Engines[0].GoSourceDirectives[0].Attributes = []string{"seconds", "seconds"}
		}},
		{"duplicate directive", func(d *protocol.Description) {
			d.Engines[0].GoSourceDirectives = append(d.Engines[0].GoSourceDirectives, proto.CloneOf(d.Engines[0].GoSourceDirectives[0]))
		}},
		{"same directive in another scope", func(d *protocol.Description) {
			other := proto.CloneOf(d.Engines[0].GoSourceDirectives[0])
			other.Scope = protocol.GoSourceDirective_COLUMN
			d.Engines[0].GoSourceDirectives = append(d.Engines[0].GoSourceDirectives, other)
		}},
		{"no source service", func(d *protocol.Description) { d.Features = []string{provider.NormalizeValuesFeature} }},
		{"missing models", func(d *protocol.Description) { d.Engines[0].GoSourceDirectives[0].Models = nil }},
		{"unknown model", func(d *protocol.Description) {
			d.Engines[0].GoSourceDirectives[0].Models = []string{"example.org/other/model"}
		}},
		{"model without Go format", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
		}},
		{"duplicate model", func(d *protocol.Description) {
			claim := d.Engines[0].GoSourceDirectives[0]
			claim.Models = append(claim.Models, claim.Models[0])
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, _, _ := goSourceExchange()
			c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
			test.change(description)
			original := proto.CloneOf(description)
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
			c.Assert(proto.Equal(original, description), qt.IsTrue)
		})
	}
}

func TestGoSourceFragmentsFailurePath(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.DecodeSourceRequest)
	}{
		{"missing claim", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Fragments[0].GoDirective = nil }},
		{"changed directive", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].GoDirective.Directive = "ptah:fixture:another"
		}},
		{"changed attributes", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].GoDirective.Attributes = []string{"seconds", "minutes", "hours"}
		}},
		{"changed scope", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].GoDirective.Scope = protocol.GoSourceDirective_DOCUMENT
		}},
		{"fragment selector differs from claim", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Fragments[0].Selector = "another" }},
		{"model outside document scope", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Models = nil }},
		{"attribute outside claim", func(r *protocol.DecodeSourceRequest) {
			mapping := r.Documents[0].Fragments[0].GetValue().GetMapping()
			mapping.Entries[0].Key = "hours"
		}},
		{"attributes are not a mapping", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].GetValue().Value = &protocol.SourceValue_Number{Number: "60"}
		}},
		{"table directive without binding", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Fragments[0].BindingId = "" }},
		{"table directive with subject", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].Subject = proto.CloneOf(r.Documents[0].Bindings[0].Object)
		}},
		{"binding is not a table", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Bindings[0].Object.Kind = "view" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, _ := goSourceExchange()
			c.Assert(provider.ValidateSourceRequest(description, request), qt.IsNil)
			test.change(request)
			c.Assert(provider.ValidateSourceRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestGoSourceScopesAttachToTheirObject(t *testing.T) {
	column := &protocol.Reference{Engine: "fixture", Kind: "column", Name: "id", Path: []string{"a.b"}}
	index := &protocol.Reference{Engine: "fixture", Kind: "index", Name: "ix", Path: []string{"a.b"}}
	cases := []struct {
		name    string
		scope   protocol.GoSourceDirective_Scope
		binding string
		subject *protocol.Reference
	}{
		{name: "document", scope: protocol.GoSourceDirective_DOCUMENT},
		{name: "column", scope: protocol.GoSourceDirective_COLUMN, binding: "table", subject: column},
		{name: "index", scope: protocol.GoSourceDirective_INDEX, binding: "table", subject: index},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, _ := goSourceExchange()
			description.Engines[0].GoSourceDirectives[0].Scope = test.scope
			fragment := request.Documents[0].Fragments[0]
			fragment.GoDirective = proto.CloneOf(description.Engines[0].GoSourceDirectives[0])
			fragment.BindingId = test.binding
			fragment.Subject = proto.CloneOf(test.subject)
			c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
			c.Assert(provider.ValidateSourceRequest(description, request), qt.IsNil)
		})
	}
	refused := []struct {
		name    string
		scope   protocol.GoSourceDirective_Scope
		binding string
		subject *protocol.Reference
	}{
		{"document with binding", protocol.GoSourceDirective_DOCUMENT, "table", nil},
		{"document with subject", protocol.GoSourceDirective_DOCUMENT, "", column},
		{"column without subject", protocol.GoSourceDirective_COLUMN, "table", nil},
		{"column with an index subject", protocol.GoSourceDirective_COLUMN, "table", index},
		{"column without table binding", protocol.GoSourceDirective_COLUMN, "", column},
		{"index with a column subject", protocol.GoSourceDirective_INDEX, "table", column},
	}
	for _, test := range refused {
		t.Run("refused "+test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, _ := goSourceExchange()
			description.Engines[0].GoSourceDirectives[0].Scope = test.scope
			fragment := request.Documents[0].Fragments[0]
			fragment.GoDirective = proto.CloneOf(description.Engines[0].GoSourceDirectives[0])
			fragment.BindingId = test.binding
			fragment.Subject = proto.CloneOf(test.subject)
			c.Assert(provider.ValidateSourceRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestGoSourceDecodedValueMustStayInsideItsClaim_FailurePath(t *testing.T) {
	c := qt.New(t)
	description, request, result := goSourceExchange()
	other := proto.CloneOf(description.Engines[0].ValueModels[0])
	other.Kind = "example.org/other/model"
	description.Engines[0].ValueModels = append(description.Engines[0].ValueModels, other)
	request.Documents[0].Models = append(request.Documents[0].Models, proto.CloneOf(other))
	result.Documents[0].Fragments[0].Values[0].Value.Value.Kind = other.Kind
	c.Assert(provider.ValidateDecodedSource(description, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestGoDirectiveClaimIsRefusedOnOtherFormats_FailurePath(t *testing.T) {
	c := qt.New(t)
	description, request, _ := sourceExchange()
	request.Documents[0].Fragments[0].GoDirective = &protocol.GoSourceDirective{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:fixture:retention", Selector: "retention"}
	c.Assert(provider.ValidateSourceRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
}

func TestGoFragmentsWithoutDeclaredSyntax_HappyPath(t *testing.T) {
	c := qt.New(t)
	description, request, _ := goSourceExchange()
	description.Engines[0].GoSourceDirectives = nil
	request.Documents[0].Fragments[0].GoDirective = nil
	c.Assert(provider.ValidateSourceRequest(description, request), qt.IsNil)
}

func TestGoFragmentClaimNeedsDeclaredSyntax_FailurePath(t *testing.T) {
	c := qt.New(t)
	description, request, _ := goSourceExchange()
	description.Engines[0].GoSourceDirectives = nil
	c.Assert(provider.ValidateSourceRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
}
