package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func validationExchange() (*protocol.Description, *protocol.ValidateSchemaRequest) {
	description, creation, _ := creationExchange()
	description.Features = append(description.Features, provider.ValidateSchemaFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.ValidateSchemaFeature)
	request := &protocol.ValidateSchemaRequest{Engine: creation.Engine, Identifiers: creation.Identifiers, TargetFeatures: creation.TargetFeatures, Capabilities: creation.Capabilities,
		Tables:   []*protocol.ValidationTable{{Subject: creation.Tables[0].Subject, Declaration: creation.Tables[0].Declaration}},
		Features: proto.CloneOf(creation.Tables[0].Declaration.Features), NoSkipped: true}
	return description, request
}

func TestSchemaValidationRetainsCaptureAndTypedDiagnostics(t *testing.T) {
	c := qt.New(t)
	description, request := validationExchange()
	original := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	result := &protocol.SchemaValidation{Complete: true, Diagnostics: []*protocol.SchemaDiagnostic{
		{Code: protocol.SchemaDiagnostic_INVALID_SCHEMA, Kind: "table", Object: "records", Message: "Invalid key"},
		{Code: protocol.SchemaDiagnostic_UNSUPPORTED_FEATURE, Kind: "index", Message: "Unsupported index"},
		{Code: protocol.SchemaDiagnostic_OMITTED_DECLARATION, Kind: "comment", Message: "Comment would be omitted"},
	}}
	c.Assert(provider.ValidateSchemaResult(description, request, result), qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	request.Tables = nil
	request.Features.Objects.Records = nil
	request.Features.Facets.Records = nil
	c.Assert(provider.ValidateSchemaResult(description, request, &protocol.SchemaValidation{Complete: true}), qt.IsNil)
}

func TestSchemaValidationRefusesIncompleteContext(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*protocol.Description, *protocol.ValidateSchemaRequest)
	}{
		{"missing service", func(d *protocol.Description, _ *protocol.ValidateSchemaRequest) { d.Features = nil }},
		{"missing engine service", func(d *protocol.Description, _ *protocol.ValidateSchemaRequest) {
			d.Engines[0].Features = []string{"retention"}
		}},
		{"missing identifiers", func(_ *protocol.Description, r *protocol.ValidateSchemaRequest) { r.Identifiers = nil }},
		{"missing table", func(_ *protocol.Description, r *protocol.ValidateSchemaRequest) { r.Tables[0] = nil }},
		{"changed identity", func(_ *protocol.Description, r *protocol.ValidateSchemaRequest) {
			r.Tables[0].Declaration.Name = "other"
		}},
		{"duplicate table", func(_ *protocol.Description, r *protocol.ValidateSchemaRequest) {
			r.Tables = append(r.Tables, proto.CloneOf(r.Tables[0]))
		}},
		{"missing schema features", func(_ *protocol.Description, r *protocol.ValidateSchemaRequest) { r.Features = nil }},
		{"missing table features", func(_ *protocol.Description, r *protocol.ValidateSchemaRequest) {
			r.Tables[0].Declaration.Features = nil
		}},
		{"nil namespace", func(_ *protocol.Description, r *protocol.ValidateSchemaRequest) {
			r.Namespaces = []*protocol.DeclaredNamespace{nil}
		}},
		{"nil enum", func(_ *protocol.Description, r *protocol.ValidateSchemaRequest) {
			r.Enums = []*protocol.DeclaredEnum{nil}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r := validationExchange()
			test.change(d, r)
			c.Assert(provider.ValidateSchemaRequest(d, r), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSchemaValidationDiscardsMalformedDiagnostics(t *testing.T) {
	for _, result := range []*protocol.SchemaValidation{
		nil, {}, {Complete: true, Diagnostics: []*protocol.SchemaDiagnostic{nil}},
		{Complete: true, Diagnostics: []*protocol.SchemaDiagnostic{{Code: protocol.SchemaDiagnostic_Code(99), Kind: "table", Message: "bad"}}},
		{Complete: true, Diagnostics: []*protocol.SchemaDiagnostic{{Code: protocol.SchemaDiagnostic_INVALID_SCHEMA, Message: "bad"}}},
		{Complete: true, Diagnostics: []*protocol.SchemaDiagnostic{{Code: protocol.SchemaDiagnostic_INVALID_SCHEMA, Kind: "table", Message: "bad"}, {Code: protocol.SchemaDiagnostic_INVALID_SCHEMA, Kind: "table"}}},
	} {
		c := qt.New(t)
		d, r := validationExchange()
		c.Assert(provider.ValidateSchemaResult(d, r, result), qt.ErrorIs, provider.ErrProtocol)
	}
}

func TestSchemaValidationRequiresNormalizationAndExclusivePolicy(t *testing.T) {
	c := qt.New(t)
	d, _ := validationExchange()
	d.Features = []string{provider.ValidateSchemaFeature}
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
	d, _ = validationExchange()
	d.Engines[0].Features = append(d.Engines[0].Features, provider.CommonTableIdentityFeature)
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
}
