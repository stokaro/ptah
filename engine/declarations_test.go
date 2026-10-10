package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

type declarationFunc func(context.Context, featureplan.DeclarationRequest) (featureplan.DeclarationResult, error)

func (f declarationFunc) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	return f(ctx, request)
}

func declarationProvider(service featureplan.DeclarationService) engine.Provider {
	// A source-only provider needs no catalog or change model and no built-ins.
	return engine.Provider{ID: "example.org/declarations", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}}},
		Codecs:       []schemaext.Codec{conversionCodec(conversionFirst, schemaext.Desired), conversionCodec(conversionSecond, schemaext.Desired), planningCodec()},
		Declarations: []engine.DeclarationPlanning{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, OperationKinds: []schemaext.Kind{planningOperationKind}, Service: service}},
	}
}

func declarationRequest() featureplan.DeclarationRequest {
	semantics := identifier.ForDialect("postgres")
	builder := objectidentity.NewBuilder(semantics)
	return featureplan.DeclarationRequest{Target: " ALTERNATE ", Identifiers: semantics, Capabilities: capability.Capabilities{capability.Changefeeds: true},
		Objects: []schemaext.Object{
			{Ref: builder.SchemaScopedParts(objectidentity.Kind(conversionSecond), "", "first"), Value: &conversionValue{ID: conversionSecond, Number: 3}},
			{Ref: builder.SchemaScopedParts(objectidentity.Kind(conversionFirst), "", "second"), Value: &conversionValue{ID: conversionFirst, Number: 7}},
			{Ref: builder.SchemaScopedParts(objectidentity.Kind(conversionSecond), "", "third"), Value: &conversionValue{ID: conversionSecond, Number: 11}},
		},
		Tables: []schemacapture.TableDeclaration{{Table: schemamodel.Table{Name: "items", PrimaryKey: []string{"id"}}}},
		CommonSteps: []featureplan.CommonStep{{ID: plangraph.StepID{Owner: "example.org/host", Name: "table"},
			Effects: []plangraph.Effect{{Subject: builder.TableParts("", "items"), Action: plangraph.Create}}}},
	}
}

func declaredFixture(_ context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/declarations"}
	result := featureplan.DeclarationResult{Complete: true}
	for _, object := range request.Objects {
		step := plangraph.StepID{Owner: contribution.Owner, Name: object.Ref.Name.Source}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: step,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &planningOperation{Values: []int{object.Value.(*conversionValue).Number}}, Notes: []string{"owner note"}},
			Effects: []plangraph.Effect{{Subject: object.Ref, Action: plangraph.Create}}, Transaction: plangraph.TransactionForbidden})
		contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: request.CommonSteps[0].ID, After: step})
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref, Strategy: "create the authored object", Steps: []plangraph.StepID{step}})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

func TestDeclarationsSnapshotInputsAndRepliesWithoutEncoding(t *testing.T) {
	c := qt.New(t)
	var reply featureplan.DeclarationResult
	var received featureplan.DeclarationRequest
	var receivedContext context.Context
	calls, encodes := 0, 0
	p := declarationProvider(declarationFunc(func(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
		calls++
		received, receivedContext = request, ctx
		var err error
		reply, err = declaredFixture(ctx, request)
		request.Objects[0].Value.(*conversionValue).Number = 99
		request.Tables[0].Table.PrimaryKey[0] = "changed"
		request.Capabilities[capability.Changefeeds] = false
		request.CommonSteps[0].Effects[0].Action = plangraph.Drop
		return reply, err
	}))
	for i := range p.Codecs {
		p.Codecs[i].Encode = func(schemaext.Payload) (json.RawMessage, error) {
			encodes++
			return nil, errors.New("local planning does not encode")
		}
		p.Codecs[i].Canonical = p.Codecs[i].Encode
	}
	runtime := mustRuntime(c, p)
	p.Declarations[0].Kinds[0] = "example.org/replaced"
	p.Declarations[0].OperationKinds[0] = "example.org/replaced"
	request := declarationRequest()
	result, err := runtime.PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(encodes, qt.Equals, 0)
	c.Assert(receivedContext, qt.Equals, t.Context())
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(received.Objects, qt.HasLen, 3)
	c.Assert(request.Objects[0].Value.(*conversionValue).Number, qt.Equals, 3)
	c.Assert(request.Tables[0].Table.PrimaryKey, qt.DeepEquals, []string{"id"})
	c.Assert(request.Capabilities.Has(capability.Changefeeds), qt.IsTrue)
	c.Assert(request.CommonSteps[0].Effects[0].Action, qt.Equals, plangraph.Create)
	reply.Contributions[0].Steps[0].Payload.Payload.(*planningOperation).Values[0] = 99
	reply.Contributions[0].Steps[0].Payload.Notes[0] = "changed"
	reply.Contributions[0].Steps[0].Effects[0].Action = plangraph.Drop
	reply.Contributions[0].Dependencies[0].Before.Name = "changed"
	reply.Declarations[0].Steps[0].Name = "changed"
	step := result.Contributions[0].Steps[0]
	c.Assert(step.Payload.Payload.(*planningOperation).Values, qt.DeepEquals, []int{3})
	c.Assert(step.Payload.Notes, qt.DeepEquals, []string{"owner note"})
	c.Assert(step.Effects[0].Action, qt.Equals, plangraph.Create)
	c.Assert(result.Contributions[0].Dependencies[0].Before, qt.Equals, request.CommonSteps[0].ID)
	c.Assert(result.Declarations[0].Steps[0], qt.Equals, step.ID)
}

func TestDeclarationsRejectInvalidRegistration(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*engine.Provider)
	}{
		{"missing service", func(p *engine.Provider) { p.Declarations[0].Service = nil }},
		{"typed nil service", func(p *engine.Provider) { var missing declarationFunc; p.Declarations[0].Service = missing }},
		{"missing models", func(p *engine.Provider) { p.Declarations[0].Kinds = nil }},
		{"duplicate model", func(p *engine.Provider) { p.Declarations[0].Kinds = append(p.Declarations[0].Kinds, conversionFirst) }},
		{"missing operations", func(p *engine.Provider) { p.Declarations[0].OperationKinds = nil }},
		{"duplicate operation", func(p *engine.Provider) {
			p.Declarations[0].OperationKinds = append(p.Declarations[0].OperationKinds, planningOperationKind)
		}},
		{"missing desired codec", func(p *engine.Provider) { p.Codecs = p.Codecs[1:] }},
		{"missing operation codec", func(p *engine.Provider) { p.Codecs = p.Codecs[:2] }},
		{"competing service", func(p *engine.Provider) { p.Declarations = append(p.Declarations, p.Declarations[0]) }},
		{"unknown target", func(p *engine.Provider) { p.Declarations[0].Target = "missing" }},
		{"alias target", func(p *engine.Provider) { p.Declarations[0].Target = "alternate" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := declarationProvider(declarationFunc(declaredFixture))
			test.edit(&p)
			runtime, err := engine.New(p)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func TestDeclarationsPreflightAllInputsBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*engine.Provider, *featureplan.DeclarationRequest)
		want error
	}{
		{"unregistered later kind", func(p *engine.Provider, _ *featureplan.DeclarationRequest) {
			p.Declarations[0].Kinds = p.Declarations[0].Kinds[1:]
		}, ptaherr.ErrUnsupportedFeature},
		{"duplicate object", func(_ *engine.Provider, r *featureplan.DeclarationRequest) { r.Objects[2] = r.Objects[0] }, schemaext.ErrInvalidValue},
		{"nil value", func(_ *engine.Provider, r *featureplan.DeclarationRequest) { r.Objects[2].Value = nil }, schemaext.ErrInvalidValue},
		{"changed kind", func(_ *engine.Provider, r *featureplan.DeclarationRequest) {
			r.Objects[2].Ref.Kind = objectidentity.KindTable
		}, schemaext.ErrInvalidValue},
		{"table-bound object", func(_ *engine.Provider, r *featureplan.DeclarationRequest) {
			r.Objects[2].Ref.Parent = r.Objects[0].Ref.Name
		}, schemaext.ErrInvalidValue},
		{"duplicate table", func(_ *engine.Provider, r *featureplan.DeclarationRequest) { r.Tables = append(r.Tables, r.Tables[0]) }, schemaext.ErrInvalidValue},
		{"empty dependency", func(_ *engine.Provider, r *featureplan.DeclarationRequest) {
			r.Tables[0] = schemacapture.TableDeclaration{}
		}, schemaext.ErrInvalidValue},
		{"duplicate common step", func(_ *engine.Provider, r *featureplan.DeclarationRequest) {
			r.CommonSteps = append(r.CommonSteps, r.CommonSteps[0])
		}, schemaext.ErrInvalidValue},
		{"unknown target", func(_ *engine.Provider, r *featureplan.DeclarationRequest) { r.Target = "missing" }, ptaherr.ErrUnsupportedDialect},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			p := declarationProvider(declarationFunc(func(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
				calls++
				return declaredFixture(ctx, request)
			}))
			request := declarationRequest()
			test.edit(&p, &request)
			result, err := mustRuntime(c, p).PlanDeclarations(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, featureplan.DeclarationResult{})
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestDeclarationsRejectMalformedRepliesAtomically(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.DeclarationResult)
	}{
		{"incomplete", func(r *featureplan.DeclarationResult) { r.Complete = false }},
		{"missing receipt", func(r *featureplan.DeclarationResult) { r.Declarations = r.Declarations[:1] }},
		{"reordered receipts", func(r *featureplan.DeclarationResult) {
			r.Declarations[0], r.Declarations[2] = r.Declarations[2], r.Declarations[0]
		}},
		{"changed spelling", func(r *featureplan.DeclarationResult) { r.Declarations[0].Subject.Name.Source = "FIRST" }},
		{"empty strategy", func(r *featureplan.DeclarationResult) { r.Declarations[0].Strategy = "" }},
		{"multiline strategy", func(r *featureplan.DeclarationResult) { r.Declarations[0].Strategy = "create\nSQL" }},
		{"missing step", func(r *featureplan.DeclarationResult) { r.Declarations[0].Steps[0].Name = "missing" }},
		{"duplicate receipt step", func(r *featureplan.DeclarationResult) {
			r.Declarations[0].Steps = append(r.Declarations[0].Steps, r.Declarations[0].Steps[0])
		}},
		{"unaccounted step", func(r *featureplan.DeclarationResult) { r.Declarations[0].Steps = nil }},
		{"foreign owner", func(r *featureplan.DeclarationResult) { r.Contributions[0].Owner = "example.org/foreign" }},
		{"foreign step", func(r *featureplan.DeclarationResult) { r.Contributions[0].Steps[0].ID.Owner = "example.org/foreign" }},
		{"duplicate step", func(r *featureplan.DeclarationResult) {
			r.Contributions[0].Steps = append(r.Contributions[0].Steps, r.Contributions[0].Steps[0])
		}},
		{"missing payload", func(r *featureplan.DeclarationResult) { r.Contributions[0].Steps[0].Payload.Payload = nil }},
		{"uncaptured alter parent", func(r *featureplan.DeclarationResult) { r.Contributions[0].Steps[0].Payload.Role = ast.AlterExtension }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := declarationProvider(declarationFunc(func(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
				result, err := declaredFixture(ctx, request)
				test.edit(&result)
				return result, err
			}))
			result, err := mustRuntime(c, p).PlanDeclarations(t.Context(), declarationRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.DeclarationResult{})
		})
	}
}

// TestDeclarationsAcceptChildrenOfDeclaredTables pins that a declaration may
// be a child of a table the request declares, as a whole-schema render sends a
// table's child its owner creates in steps of its own, and that the runtime
// says which kinds it declares.
func TestDeclarationsAcceptChildrenOfDeclaredTables(t *testing.T) {
	c := qt.New(t)
	var received featureplan.DeclarationRequest
	runtime := mustRuntime(c, declarationProvider(declarationFunc(func(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
		received = request
		return declaredFixture(ctx, request)
	})))
	request := declarationRequest()
	request.Objects[2].Ref.Parent = objectidentity.NewBuilder(request.Identifiers).TableParts("", "items").Name

	result, err := runtime.PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(received.Objects[2].Ref.Parent.Source, qt.Equals, "items")
	c.Assert(runtime.DeclaresKind("alternate", conversionFirst), qt.IsTrue)
	c.Assert(runtime.DeclaresKind("custom", planningOperationKind), qt.IsFalse)
	c.Assert(runtime.DeclaresKind("missing", conversionFirst), qt.IsFalse)
}
