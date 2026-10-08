package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

const planningOperationKind schemaext.Kind = "example.org/planning-operation"

type planningFunc func(context.Context, featureplan.Request) (featureplan.Result, error)

func (f planningFunc) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return f(ctx, request)
}

type planningOperation struct{ Values []int }

func (*planningOperation) Kind() schemaext.Kind { return planningOperationKind }
func (p *planningOperation) CloneExtension() ast.ExtensionPayload {
	return &planningOperation{Values: slices.Clone(p.Values)}
}

func planningCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		return json.Marshal(payload.(*planningOperation).Values)
	}
	return schemaext.Codec{
		Prototype: &planningOperation{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"array","items":{"type":"integer"}}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			return payload.(*planningOperation).CloneExtension(), nil
		}, Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			values, err := schemaext.DecodeJSON[[]int](data)
			return &planningOperation{Values: values}, err
		},
	}
}

func planningProvider(service featureplan.Service) engine.Provider {
	p := reversalProvider(nil)
	p.Reversals = nil
	p.Codecs = append(p.Codecs, planningCodec())
	p.Planning = []engine.Planning{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, OperationKinds: []schemaext.Kind{planningOperationKind}, Service: service}}
	return p
}

func planningRequest() featureplan.Request {
	r := reversalRequest()
	parent := objectidentity.NewBuilder(r.Identifiers).TableParts("", "items")
	return featureplan.Request{Target: r.Target, Identifiers: r.Identifiers, Capabilities: r.Capabilities, Changes: r.Changes,
		Tables: []featureplan.Table{{Subject: parent,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "items", PrimaryKey: []string{"id"}}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items", Columns: []catalog.Column{{Name: "id"}}}},
		}},
	}
}

func plannedFixture(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/reverser"}
	result := featureplan.Result{Complete: true}
	for _, change := range request.Changes {
		step := plangraph.StepID{Owner: contribution.Owner, Name: change.Subject.Name.Source}
		operation := featureplan.Operation{Role: ast.AlterExtension, Parent: request.Tables[0].Subject,
			Payload: &planningOperation{Values: []int{change.Value.(*reversalChange).Number}}, Notes: []string{"owner note"}}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: step, Payload: operation,
			Effects: []plangraph.Effect{{Subject: change.Subject, Action: plangraph.Alter}}, Transaction: plangraph.TransactionForbidden,
			Impact: schemaext.Effect{}})
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "restore the prior definition", Steps: []plangraph.StepID{step}})
	}
	contribution.Dependencies = []plangraph.Dependency{{Before: plangraph.StepID{Owner: "host", Name: "parent"}, After: contribution.Steps[0].ID}}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

func TestPlanningSnapshotsInputsAndRepliesWithoutEncoding(t *testing.T) {
	c := qt.New(t)
	var received featureplan.Request
	var reply featureplan.Result
	var receivedContext context.Context
	calls, encodes := 0, 0
	p := planningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		calls++
		received, receivedContext = request, ctx
		var err error
		reply, err = plannedFixture(ctx, request)
		request.Capabilities[capability.Changefeeds] = false
		request.Changes[0].Value.(*reversalChange).Number = 99
		request.Tables[0].Desired.Table.PrimaryKey[0] = "changed"
		request.Tables[0].Current.Table.Columns[0].Name = "changed"
		return reply, err
	}))
	for i := range p.Codecs {
		p.Codecs[i].Encode = func(schemaext.Payload) (json.RawMessage, error) {
			encodes++
			return nil, errors.New("encoding is not a planning operation")
		}
		p.Codecs[i].Canonical = p.Codecs[i].Encode
	}
	runtime := mustRuntime(c, p)
	p.Planning[0].Kinds[0] = "example.org/replaced"
	p.Planning[0].OperationKinds[0] = "example.org/replaced"
	request := planningRequest()
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(encodes, qt.Equals, 0)
	c.Assert(receivedContext, qt.Equals, t.Context())
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(received.Changes, qt.HasLen, 3)
	c.Assert(request.Capabilities.Has(capability.Changefeeds), qt.IsTrue)
	c.Assert(request.Changes[0].Value.(*reversalChange).Number, qt.Equals, 3)
	c.Assert(request.Tables[0].Desired.Table.PrimaryKey, qt.DeepEquals, []string{"id"})
	c.Assert(request.Tables[0].Current.Table.Columns, qt.DeepEquals, []catalog.Column{{Name: "id"}})
	reply.Contributions[0].Steps[0].Payload.Payload.(*planningOperation).Values[0] = 99
	reply.Contributions[0].Steps[0].Payload.Notes[0] = "changed"
	reply.Contributions[0].Steps[0].Effects[0].Action = plangraph.Drop
	reply.Contributions[0].Dependencies[0].Before.Name = "changed"
	reply.Changes[0].Steps[0].Name = "changed"
	step := result.Contributions[0].Steps[0]
	c.Assert(step.Payload.Payload.(*planningOperation).Values, qt.DeepEquals, []int{3})
	c.Assert(step.Payload.Notes, qt.DeepEquals, []string{"owner note"})
	c.Assert(step.Effects[0].Action, qt.Equals, plangraph.Alter)
	c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(step.Impact, qt.Equals, schemaext.Effect{})
	c.Assert(result.Contributions[0].Dependencies[0].Before.Name, qt.Equals, "parent")
	c.Assert(result.Changes[0].Steps[0], qt.Equals, step.ID)
}

func TestPlanningRejectsInvalidRegistration(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*engine.Provider)
	}{
		{"missing service", func(p *engine.Provider) { p.Planning[0].Service = nil }},
		{"typed nil service", func(p *engine.Provider) { var missing planningFunc; p.Planning[0].Service = missing }},
		{"no kinds", func(p *engine.Provider) { p.Planning[0].Kinds = nil }},
		{"duplicate kind", func(p *engine.Provider) { p.Planning[0].Kinds = append(p.Planning[0].Kinds, conversionFirst) }},
		{"no operations", func(p *engine.Provider) { p.Planning[0].OperationKinds = nil }},
		{"duplicate operation", func(p *engine.Provider) {
			p.Planning[0].OperationKinds = append(p.Planning[0].OperationKinds, planningOperationKind)
		}},
		{"competing services", func(p *engine.Provider) { p.Planning = append(p.Planning, p.Planning[0]) }},
		{"missing change codec", func(p *engine.Provider) { p.Codecs = p.Codecs[1:] }},
		{"missing operation codec", func(p *engine.Provider) { p.Codecs = p.Codecs[:2] }},
		{"unknown target", func(p *engine.Provider) { p.Planning[0].Target = "missing" }},
		{"alias target", func(p *engine.Provider) { p.Planning[0].Target = "alternate" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := planningProvider(planningFunc(plannedFixture))
			test.edit(&p)
			runtime, err := engine.New(p)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func TestPlanningPreflightsAllInputsBeforeCallingAService(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*engine.Provider, *featureplan.Request)
		want error
	}{
		{"missing later service", func(p *engine.Provider, _ *featureplan.Request) { p.Planning[0].Kinds = p.Planning[0].Kinds[:1] }, ptaherr.ErrUnsupportedFeature},
		{"duplicate change", func(_ *engine.Provider, r *featureplan.Request) { r.Changes[2] = r.Changes[0] }, schemaext.ErrInvalidValue},
		{"nil payload", func(_ *engine.Provider, r *featureplan.Request) { r.Changes[2].Value = nil }, schemaext.ErrInvalidValue},
		{"unknown target", func(_ *engine.Provider, r *featureplan.Request) { r.Target = "unknown" }, ptaherr.ErrUnsupportedDialect},
		{"duplicate parent", func(_ *engine.Provider, r *featureplan.Request) { r.Tables = append(r.Tables, r.Tables[0]) }, schemaext.ErrInvalidValue},
		{"wrong declaration", func(_ *engine.Provider, r *featureplan.Request) { r.Tables[0].Desired.Table.Name = "other" }, schemaext.ErrInvalidValue},
		{"wrong observation", func(_ *engine.Provider, r *featureplan.Request) { r.Tables[0].Current.Table.Name = "other" }, schemaext.ErrInvalidValue},
		{"non-table parent", func(_ *engine.Provider, r *featureplan.Request) { r.Tables[0].Subject = r.Changes[0].Subject }, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			p := planningProvider(planningFunc(func(ctx context.Context, r featureplan.Request) (featureplan.Result, error) {
				calls++
				return plannedFixture(ctx, r)
			}))
			request := planningRequest()
			test.edit(&p, &request)
			result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestPlanningRejectsMalformedRepliesWithoutPartialResults(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Result)
	}{
		{"missing completion receipt", func(r *featureplan.Result) { r.Complete = false }},
		{"missing result", func(r *featureplan.Result) { r.Changes = r.Changes[:1] }},
		{"reordered results", func(r *featureplan.Result) { r.Changes[0], r.Changes[2] = r.Changes[2], r.Changes[0] }},
		{"changed provenance", func(r *featureplan.Result) { r.Changes[0].Subject.Name.Source = "ONE" }},
		{"changed kind", func(r *featureplan.Result) { r.Changes[0].Kind = conversionSecond }},
		{"empty strategy", func(r *featureplan.Result) { r.Changes[0].Strategy = "" }},
		{"multiline strategy", func(r *featureplan.Result) { r.Changes[0].Strategy = "restore\nSQL" }},
		{"missing step", func(r *featureplan.Result) { r.Changes[0].Steps[0].Name = "missing" }},
		{"duplicate receipt", func(r *featureplan.Result) { r.Changes[0].Steps = append(r.Changes[0].Steps, r.Changes[0].Steps[0]) }},
		{"unaccounted step", func(r *featureplan.Result) { r.Changes[0].Steps = nil }},
		{"foreign owner", func(r *featureplan.Result) { r.Contributions[0].Owner = "example.org/foreign" }},
		{"foreign step", func(r *featureplan.Result) { r.Contributions[0].Steps[0].ID.Owner = "example.org/foreign" }},
		{"empty step name", func(r *featureplan.Result) { r.Contributions[0].Steps[0].ID.Name = "" }},
		{"duplicate step", func(r *featureplan.Result) {
			r.Contributions[0].Steps = append(r.Contributions[0].Steps, r.Contributions[0].Steps[0])
		}},
		{"missing payload", func(r *featureplan.Result) { r.Contributions[0].Steps[0].Payload.Payload = nil }},
		{"unknown role", func(r *featureplan.Result) { r.Contributions[0].Steps[0].Payload.Role = "unknown" }},
		{"missing parent", func(r *featureplan.Result) { r.Contributions[0].Steps[0].Payload.Parent = objectidentity.ID{} }},
		{"standalone with parent", func(r *featureplan.Result) { r.Contributions[0].Steps[0].Payload.Role = ast.StatementExtension }},
		{"multiline note", func(r *featureplan.Result) { r.Contributions[0].Steps[0].Payload.Notes[0] = "note\nSQL" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, planningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
				reply, err := plannedFixture(ctx, request)
				test.edit(&reply)
				return reply, err
			})))
			result, err := runtime.PlanFeatures(t.Context(), planningRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}

func TestPlanningBatchesServicesAndRestoresInputOrder(t *testing.T) {
	c := qt.New(t)
	var sizes []int
	service := planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		sizes = append(sizes, len(request.Changes))
		return plannedFixture(ctx, request)
	})
	p := planningProvider(service)
	p.Planning[0].Kinds = []schemaext.Kind{conversionFirst}
	p.Planning = append(p.Planning, engine.Planning{Target: "custom", Kinds: []schemaext.Kind{conversionSecond}, OperationKinds: []schemaext.Kind{planningOperationKind}, Service: service})
	request := planningRequest()
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(sizes, qt.DeepEquals, []int{2, 1})
	c.Assert(result.Contributions, qt.HasLen, 2)
	for i, change := range result.Changes {
		c.Assert(change.Subject, qt.Equals, request.Changes[i].Subject)
		c.Assert(change.Kind, qt.Equals, request.Changes[i].Value.Kind())
	}
}

func TestPlanningDiscardsEarlierResultsWhenLaterServiceFails(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider stopped")
	var sizes []int
	p := planningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		sizes = append(sizes, len(request.Changes))
		return plannedFixture(ctx, request)
	}))
	p.Planning[0].Kinds = []schemaext.Kind{conversionFirst}
	p.Planning = append(p.Planning, engine.Planning{Target: "custom", Kinds: []schemaext.Kind{conversionSecond}, OperationKinds: []schemaext.Kind{planningOperationKind},
		Service: planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
			sizes = append(sizes, len(request.Changes))
			partial, _ := plannedFixture(ctx, request)
			return partial, failure
		}),
	})
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), planningRequest())
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
	c.Assert(sizes, qt.DeepEquals, []int{2, 1})
}

func TestPlanningCancellationDiscardsServiceReply(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	runtime := mustRuntime(c, planningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		calls++
		cancel()
		return plannedFixture(ctx, request)
	})))
	result, err := runtime.PlanFeatures(ctx, planningRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
	c.Assert(calls, qt.Equals, 1)
	result, err = runtime.PlanFeatures(ctx, planningRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
	c.Assert(calls, qt.Equals, 1)
}

func TestPlanningKeepsValidationInputsSeparateFromTheService(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, planningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		request.Changes[0].Subject.Name.Source = "tampered"
		return plannedFixture(ctx, request)
	})))
	result, err := runtime.PlanFeatures(t.Context(), planningRequest())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}
