package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func declarationsExchange() (*protocol.Description, *protocol.PlanDeclarationsRequest, *protocol.PlannedDeclarations) {
	d, planning, planned := planningExchange()
	d.Features = append(d.Features, provider.SchemaDeclarationsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.SchemaDeclarationsFeature)
	kind := engine.ValueModels[0].Kind
	service := &protocol.ModelDeclarations{Id: "authored", Kinds: []string{kind}, OperationKinds: engine.Planning[0].OperationKinds}
	engine.Declarations = []*protocol.ModelDeclarations{service}
	object := proto.CloneOf(planning.Tables[0].Desired.Features.Facets.Records[0])
	object.Subject.Kind = kind
	object.Value.Value.Owner.Kind, object.Value.NormalizedOwner.Kind = kind, kind
	part := func(text string) *protocol.ComparisonPart {
		return &protocol.ComparisonPart{Source: text, Normalized: text}
	}
	object.Subject.Database, object.Subject.Catalog = part("database"), part("catalog")
	object.Subject.Path = []*protocol.ComparisonPart{part("a/b"), part("c")}
	object.Subject.Signature = []*protocol.ComparisonPart{part("integer")}
	request := &protocol.PlanDeclarationsRequest{Engine: planning.Engine, DeclarationId: service.Id, TargetFeatures: planning.TargetFeatures,
		Identifiers: planning.Identifiers, Capabilities: planning.Capabilities, Objects: []*protocol.ComparisonRecord{object},
		Tables: []*protocol.ValidationTable{{Subject: planning.Tables[0].Subject, Declaration: planning.Tables[0].Desired}}, CommonSteps: planning.CommonSteps}
	result := &protocol.PlannedDeclarations{Complete: true, Contributions: planned.Contributions,
		Declarations: []*protocol.DeclarationReceipt{{Subject: proto.CloneOf(object.Subject), Strategy: "Create authored settings", Steps: planned.Changes[0].Steps}}}
	return d, request, proto.CloneOf(result)
}

func TestDeclarationsPreserveAuthoredContext(t *testing.T) {
	c := qt.New(t)
	d, request, result := declarationsExchange()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidatePlannedDeclarations(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(request, before), qt.IsTrue)
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.Engines[0].Declarations, qt.IsNil)
	c.Assert(provider.ValidateDeclarationsRequest(unlisted, request), qt.ErrorIs, provider.ErrProtocol)
	listed := provider.NegotiatedDescription(d, []string{provider.SchemaDeclarationsFeature})
	c.Assert(provider.ValidateDeclarationsRequest(listed, request), qt.IsNil)
	result.Contributions = nil
	result.Declarations[0].Steps = nil
	c.Assert(provider.ValidatePlannedDeclarations(d, request, result), qt.IsNil)
}

func TestDeclarationsRequestRefusals(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PlanDeclarationsRequest)
	}{
		{"no engine", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) { r.Engine = "" }},
		{"unknown service", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) { r.DeclarationId = "other" }},
		{"duplicate service", func(d *protocol.Description, _ *protocol.PlanDeclarationsRequest) {
			d.Engines[0].Declarations = append(d.Engines[0].Declarations, proto.CloneOf(d.Engines[0].Declarations[0]))
		}},
		{"unowned kind", func(d *protocol.Description, _ *protocol.PlanDeclarationsRequest) {
			d.Engines[0].Declarations[0].Kinds = []string{"other"}
		}},
		{"unowned operation", func(d *protocol.Description, _ *protocol.PlanDeclarationsRequest) {
			d.Engines[0].Declarations[0].OperationKinds = []string{"other"}
		}},
		{"missing value", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) { r.Objects[0].Value = nil }},
		{"subject differs from value owner", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects[0].Subject.Name = &protocol.ComparisonPart{Source: "different", Normalized: "different"}
		}},
		{"database differs", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects[0].Subject.Database.Source = "different"
		}},
		{"catalog differs", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects[0].Subject.Catalog.Normalized = "different"
		}},
		{"schema differs", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects[0].Subject.Schema.Normalized = "different"
		}},
		{"path boundaries differ", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects[0].Subject.Path = []*protocol.ComparisonPart{{Source: "a", Normalized: "a"}, {Source: "b/c", Normalized: "b/c"}}
		}},
		{"signature differs", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects[0].Subject.Signature[0].Source = "text"
		}},
		{"normalized owner differs", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects[0].Value.NormalizedOwner.Name = "different"
		}},
		{"missing identifiers", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) { r.Identifiers = nil }},
		{"duplicate object", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects = append(r.Objects, proto.CloneOf(r.Objects[0]))
		}},
		{"changed kind", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) { r.Objects[0].Kind = "other" }},
		{"changed representation", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Objects[0].Value.Value.SchemaVersion++
		}},
		{"table without declaration", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) { r.Tables[0].Declaration = nil }},
		{"table identity mismatch", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.Tables[0].Declaration.Name = "other"
		}},
		{"duplicate common step", func(_ *protocol.Description, r *protocol.PlanDeclarationsRequest) {
			r.CommonSteps = append(r.CommonSteps, proto.CloneOf(r.CommonSteps[0]))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := declarationsExchange()
			test.mutate(d, request)
			c.Assert(provider.ValidateDeclarationsRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestDeclarationIdentityKeepsParentAndDefaultSchema(t *testing.T) {
	c := qt.New(t)
	d, request, _ := declarationsExchange()
	object := request.Objects[0]
	object.Subject.Parent = object.Subject.Path[1]
	object.Subject.Path = object.Subject.Path[:1]
	c.Assert(provider.ValidateDeclarationsRequest(d, request), qt.IsNil)
	object.Subject.Schema = &protocol.ComparisonPart{Source: "public", Normalized: "public", Defaulted: true}
	request.Identifiers.DefaultSchema, d.Engines[0].DefaultSchema = "public", "public"
	object.Value.Value.Owner.Schema, object.Value.NormalizedOwner.Schema = "", ""
	c.Assert(provider.ValidateDeclarationsRequest(d, request), qt.IsNil)
	object.Subject.Schema.Defaulted = false
	c.Assert(provider.ValidateDeclarationsRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
}

func TestDeclarationsResultRefusals(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.PlannedDeclarations)
	}{
		{"incomplete", func(r *protocol.PlannedDeclarations) { r.Complete = false }},
		{"missing receipt", func(r *protocol.PlannedDeclarations) { r.Declarations = nil }},
		{"changed subject", func(r *protocol.PlannedDeclarations) { r.Declarations[0].Subject.Name.Source = "other" }},
		{"no strategy", func(r *protocol.PlannedDeclarations) { r.Declarations[0].Strategy = "" }},
		{"unaccounted operation", func(r *protocol.PlannedDeclarations) { r.Declarations[0].Steps = nil }},
		{"repeated receipt step", func(r *protocol.PlannedDeclarations) {
			r.Declarations[0].Steps = append(r.Declarations[0].Steps, proto.CloneOf(r.Declarations[0].Steps[0]))
		}},
		{"unknown receipt step", func(r *protocol.PlannedDeclarations) { r.Declarations[0].Steps[0].Name = "other" }},
		{"foreign contribution", func(r *protocol.PlannedDeclarations) { r.Contributions[0].Owner = "other" }},
		{"unknown dependency", func(r *protocol.PlannedDeclarations) { r.Contributions[0].Dependencies[0].Before.Name = "missing" }},
		{"unknown operation model", func(r *protocol.PlannedDeclarations) {
			r.Contributions[0].Steps[0].Operation.Value.Value.Kind = "other"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := declarationsExchange()
			test.mutate(result)
			c.Assert(provider.ValidatePlannedDeclarations(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestDeclarationDiagnosticsNameInputObjects(t *testing.T) {
	c := qt.New(t)
	d, request, result := declarationsExchange()
	refused := &protocol.PlannedDeclarations{Complete: true, Diagnostics: []*protocol.DeclarationDiagnostic{{Object: new(uint32(0)),
		Problem: &protocol.SchemaDiagnostic{Code: protocol.SchemaDiagnostic_UNSUPPORTED_FEATURE, Kind: request.Objects[0].Kind, Message: "The requested setting cannot be created"}}}}
	c.Assert(provider.ValidatePlannedDeclarations(d, request, refused), qt.IsNil)
	refused.Declarations = result.Declarations
	c.Assert(provider.ValidatePlannedDeclarations(d, request, refused), qt.ErrorIs, provider.ErrProtocol)
	refused.Declarations = nil
	refused.Diagnostics[0].Object = new(uint32(1))
	c.Assert(provider.ValidatePlannedDeclarations(d, request, refused), qt.ErrorIs, provider.ErrProtocol)
	refused.Diagnostics[0].Object = new(uint32(0))
	refused.Diagnostics[0].Problem.Kind = "other"
	c.Assert(provider.ValidatePlannedDeclarations(d, request, refused), qt.ErrorIs, provider.ErrProtocol)
}
