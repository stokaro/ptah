package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
)

type comparisonFunc func(context.Context, schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error)

func (f comparisonFunc) CompareObjects(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	return f(ctx, r)
}

type comparedChange struct{ Number int }

const comparedKind schemaext.Kind = "example.org/compared"

func (*comparedChange) Kind() schemaext.Kind { return comparedKind }
func (v *comparedChange) CloneChange() schemaext.ChangeValue {
	return &comparedChange{Number: v.Number}
}

func comparisonProvider(service schemaext.ObjectComparisonService) engine.Provider {
	p := conversionProvider(nil)
	p.Targets[0].Preparation = schemapreparation.Identity{}
	p.Conversions = nil
	p.Comparisons = []engine.ObjectComparison{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, ChangeKinds: []schemaext.Kind{comparedKind}, Service: service}}
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
	p.Codecs = append(p.Codecs, schemaext.Codec{Prototype: &comparedChange{}, Representation: schemaext.Change, Version: 1, Definition: json.RawMessage(`{"type":"object","properties":{"Number":{"type":"integer"}}}`), Clone: func(v schemaext.Payload) (schemaext.Payload, error) { return v.(*comparedChange).CloneChange(), nil }, Encode: encode, Canonical: encode, Decode: func(data json.RawMessage) (schemaext.Payload, error) {
		return schemaext.DecodeJSON[*comparedChange](data)
	}})
	return p
}

func comparedRef(kind schemaext.Kind, name string) objectidentity.ID {
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	ref := builder.SchemaScopedParts(objectidentity.Kind(kind), "", name)
	ref.Parent = builder.TableParts("", "orders").Name
	return ref
}

func comparisonRequest(c *qt.C) schemaext.ObjectComparisonRequest {
	c.Helper()
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: comparedRef(conversionFirst, "first"), Value: &conversionValue{ID: conversionFirst, Number: 1}}, schemaext.Object{Ref: comparedRef(conversionSecond, "second"), Value: &conversionValue{ID: conversionSecond, Number: 2}})
	c.Assert(err, qt.IsNil)
	return schemaext.ObjectComparisonRequest{Target: "alternate", Desired: schemaext.ObjectState{Objects: objects}, Parents: []schemaext.ParentState{{Subject: objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "orders"), Desired: true, Current: true}}}
}

func unchangedComparison(_ context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	return schemaext.ObjectComparisonResult{Complete: true, Desired: r.Desired}, nil
}

func TestObjectComparison_BatchesOwnersAndSnapshotsReplies(t *testing.T) {
	c := qt.New(t)
	var received schemaext.ObjectComparisonRequest
	returned := &comparedChange{Number: 9}
	calls := 0
	service := comparisonFunc(func(_ context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		calls++
		received = r
		return schemaext.ObjectComparisonResult{Complete: true, Desired: r.Desired, Changes: []schemaext.ChangeRecord{{Subject: comparedRef(conversionFirst, "first"), Value: returned}}}, nil
	})
	provider := comparisonProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Comparisons[0].Kinds[0] = "example.org/mutated"
	provider.Comparisons[0].ChangeKinds[0] = "example.org/mutated"
	provider.Comparisons[0].Service = nil
	request := comparisonRequest(c)
	result, err := runtime.CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(received.Kinds, qt.DeepEquals, []schemaext.Kind{conversionFirst, conversionSecond})
	c.Assert(result.Changes, qt.HasLen, 1)
	returned.Number = 100
	received.Parents[0].Desired = false
	c.Assert(result.Changes[0].Value.(*comparedChange).Number, qt.Equals, 9)
	c.Assert(request.Parents[0].Desired, qt.IsTrue)
}

func TestObjectComparison_RejectsUntrustedReplies(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*schemaext.ObjectComparisonRequest, *schemaext.ObjectComparisonResult)
	}{
		{name: "incomplete reply", mutate: func(_ *schemaext.ObjectComparisonRequest, r *schemaext.ObjectComparisonResult) {
			r.Complete = false
		}},
		{name: "lost explicit declaration", mutate: func(_ *schemaext.ObjectComparisonRequest, r *schemaext.ObjectComparisonResult) {
			r.Desired.Objects = schemaext.Objects{}
		}},
		{name: "changed explicit declaration", mutate: func(_ *schemaext.ObjectComparisonRequest, r *schemaext.ObjectComparisonResult) {
			r.Desired.Objects, _ = r.Desired.Objects.Replace(schemaext.Object{Ref: comparedRef(conversionFirst, "first"), Value: &conversionValue{ID: conversionFirst, Number: 99}})
		}},
		{name: "invented desired object", mutate: func(_ *schemaext.ObjectComparisonRequest, r *schemaext.ObjectComparisonResult) {
			r.Desired.Objects, _ = r.Desired.Objects.With(schemaext.Object{Ref: comparedRef(conversionFirst, "invented"), Value: &conversionValue{ID: conversionFirst}})
		}},
		{name: "duplicate changes", mutate: func(_ *schemaext.ObjectComparisonRequest, r *schemaext.ObjectComparisonResult) {
			r.Changes = append(r.Changes, r.Changes[0])
		}},
		{name: "unrelated subject", mutate: func(_ *schemaext.ObjectComparisonRequest, r *schemaext.ObjectComparisonResult) {
			r.Changes[0].Subject = comparedRef(conversionFirst, "invented")
		}},
		{name: "already captured by parent", mutate: func(request *schemaext.ObjectComparisonRequest, _ *schemaext.ObjectComparisonResult) {
			request.Parents[0].Current = false
		}},
		{name: "empty diagnostic reason", mutate: func(_ *schemaext.ObjectComparisonRequest, r *schemaext.ObjectComparisonResult) {
			r.Undecided = []schemaext.UndecidedChange{{Kind: conversionSecond, Subject: comparedRef(conversionSecond, "second")}}
		}},
		{name: "changed and undecided same subject", mutate: func(_ *schemaext.ObjectComparisonRequest, r *schemaext.ObjectComparisonResult) {
			r.Undecided = []schemaext.UndecidedChange{{Kind: conversionFirst, Subject: comparedRef(conversionFirst, "first"), Reason: "unknown"}}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparisonRequest(c)
			reply := schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired, Changes: []schemaext.ChangeRecord{{Subject: comparedRef(conversionFirst, "first"), Value: &comparedChange{Number: 1}}}}
			test.mutate(&request, &reply)
			runtime := mustRuntime(c, comparisonProvider(comparisonFunc(func(context.Context, schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
				return reply, nil
			})))
			result, err := runtime.CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

func TestObjectComparison_ValidatesAllKindsBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	calls := 0
	provider := comparisonProvider(comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		calls++
		return unchangedComparison(ctx, r)
	}))
	provider.Comparisons[0].Kinds = []schemaext.Kind{conversionFirst}
	runtime := mustRuntime(c, provider)
	result, err := runtime.CompareObjects(t.Context(), comparisonRequest(c))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(calls, qt.Equals, 0)
	c.Assert(result.Changes, qt.HasLen, 0)
}

func TestObjectComparison_DiscardsFailuresAndCancellation(t *testing.T) {
	failure := errors.New("provider exited")
	canceled, cancel := context.WithCancel(t.Context())
	defer cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		service comparisonFunc
		want    error
	}{
		{name: "transport error", ctx: t.Context(), want: failure, service: comparisonFunc(func(_ context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
			return schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}, failure
		})},
		{name: "canceled reply", ctx: canceled, want: context.Canceled, service: comparisonFunc(func(_ context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
			cancel()
			return schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}, nil
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, comparisonProvider(test.service))
			result, err := runtime.CompareObjects(test.ctx, comparisonRequest(c))
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
		})
	}
}

func TestObjectComparison_RejectsRegistrationConflicts(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*engine.Provider)
	}{
		{name: "duplicate kind", mutate: func(p *engine.Provider) { p.Comparisons[0].Kinds = append(p.Comparisons[0].Kinds, conversionFirst) }},
		{name: "duplicate change kind", mutate: func(p *engine.Provider) {
			p.Comparisons[0].ChangeKinds = append(p.Comparisons[0].ChangeKinds, comparedKind)
		}},
		{name: "missing change codec", mutate: func(p *engine.Provider) { p.Codecs = p.Codecs[:4] }},
		{name: "missing observed codec", mutate: func(p *engine.Provider) { p.Codecs = append(p.Codecs[:1], p.Codecs[2:]...) }},
		{name: "alias target", mutate: func(p *engine.Provider) { p.Comparisons[0].Target = "alternate" }},
		{name: "nil service", mutate: func(p *engine.Provider) { p.Comparisons[0].Service = nil }},
		{name: "typed nil service", mutate: func(p *engine.Provider) { var service comparisonFunc; p.Comparisons[0].Service = service }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := comparisonProvider(comparisonFunc(unchangedComparison))
			test.mutate(&provider)
			runtime, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func comparisonCoverage(c *qt.C, registry schemaext.Registry, state schemaext.KnowledgeState, subjects ...schemaext.SubjectCoverage) schemaext.Coverage {
	c.Helper()
	var definitions []schemaext.KindCoverage
	for _, model := range registry.Definitions() {
		if model.Kind == conversionFirst && model.Representation == schemaext.Desired {
			definitions = append(definitions, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: state, Reason: "source claim"}})
		}
	}
	result, err := schemaext.NewCoverage(schemaext.Desired, definitions, subjects)
	c.Assert(err, qt.IsNil)
	return result
}

func TestObjectComparison_KnowledgeNeedsSourceEvidence(t *testing.T) {
	parent := comparisonRequest(qt.New(t)).Parents[0].Subject
	cases := []struct {
		name   string
		state  schemaext.KnowledgeState
		claims []schemaext.SubjectCoverage
	}{
		{name: "invented source authority", state: schemaext.Complete},
		{name: "invented parent authority", state: schemaext.Uninspected, claims: []schemaext.SubjectCoverage{{Kind: conversionFirst, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var reply schemaext.ObjectComparisonResult
			runtime := mustRuntime(c, comparisonProvider(comparisonFunc(func(context.Context, schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
				return reply, nil
			})))
			request := comparisonRequest(c)
			request.Desired.Coverage = comparisonCoverage(c, runtime.Codecs(), schemaext.Uninspected)
			reply.Desired = request.Desired
			reply.Desired.Coverage = comparisonCoverage(c, runtime.Codecs(), test.state, test.claims...)
			result, err := runtime.CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Desired.Coverage.IsZero(), qt.IsTrue)
		})
	}
}

func TestObjectComparison_EmptyNamespaceDoesNotGrantTargetSupport(t *testing.T) {
	c := qt.New(t)
	p := comparisonProvider(comparisonFunc(unchangedComparison))
	p.Comparisons = nil
	runtime := mustRuntime(c, p)
	request := comparisonRequest(c)
	request.Desired.Objects = schemaext.Objects{}
	request.Desired.Coverage = comparisonCoverage(c, runtime.Codecs(), schemaext.Complete)
	result, err := runtime.CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Desired.Coverage.KindRecords(), qt.DeepEquals, request.Desired.Coverage.KindRecords())
	request.Desired.Coverage = comparisonCoverage(c, runtime.Codecs(), schemaext.Complete, schemaext.SubjectCoverage{Kind: conversionFirst, Subject: comparedRef(conversionFirst, "first"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown object"}})
	_, err = runtime.CompareObjects(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}

func TestObjectComparison_ProviderGrowthDoesNotEnrollKinds(t *testing.T) {
	c := qt.New(t)
	calls := 0
	runtime := mustRuntime(c, comparisonProvider(comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		calls++
		return unchangedComparison(ctx, r)
	})))
	request := comparisonRequest(c)
	request.Desired.Objects = schemaext.Objects{}
	result, err := runtime.CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 0)
	c.Assert(result.Desired.Coverage.IsZero(), qt.IsTrue)
}
