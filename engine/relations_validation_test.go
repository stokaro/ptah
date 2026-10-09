package engine_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func TestRelationRegistrationRejectsAmbiguousOwnership(t *testing.T) {
	var typedNil relationsFunc
	for _, test := range []struct {
		name   string
		adjust func(*engine.Provider)
	}{
		{name: "unknown target", adjust: func(p *engine.Provider) { p.Relations[0].Target = "unknown" }},
		{name: "alias registration", adjust: func(p *engine.Provider) { p.Relations[0].Target = "alternate" }},
		{name: "missing models", adjust: func(p *engine.Provider) { p.Relations[0].Kinds = nil }},
		{name: "missing service", adjust: func(p *engine.Provider) { p.Relations[0].Service = nil }},
		{name: "typed nil service", adjust: func(p *engine.Provider) { p.Relations[0].Service = typedNil }},
		{name: "missing representation", adjust: func(p *engine.Provider) { p.Relations[0].Representation = "" }},
		{name: "non-schema representation", adjust: func(p *engine.Provider) { p.Relations[0].Representation = schemaext.Operation }},
		{name: "missing codec", adjust: func(p *engine.Provider) { p.Codecs = p.Codecs[1:] }},
		{name: "unknown model", adjust: func(p *engine.Provider) { p.Relations[0].Kinds[0] = "example.org/unknown" }},
		{name: "duplicate model", adjust: func(p *engine.Provider) { p.Relations[0].Kinds[1] = p.Relations[0].Kinds[0] }},
		{name: "overlapping owners", adjust: func(p *engine.Provider) { p.Relations = append(p.Relations, p.Relations[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			provider := relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
				calls++
				return relationReply(r), nil
			}))
			test.adjust(&provider)
			runtime, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestRelationDiscoveryPreflightsEveryInput(t *testing.T) {
	for _, test := range []struct {
		name   string
		adjust func(*schemaext.RelationRequest)
		want   error
	}{
		{name: "unknown target", want: ptaherr.ErrUnsupportedDialect, adjust: func(r *schemaext.RelationRequest) { r.Target = "missing" }},
		{name: "missing representation", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) { r.Representation = "" }},
		{name: "nil value", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) { r.Values[1].Value = nil }},
		{name: "typed nil value", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) { r.Values[1].Value = (*linkedValue)(nil) }},
		{name: "wrong model", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) { r.Values[1].Value = &linkedValue{Model: conversionSecond} }},
		{name: "duplicate subject", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) { r.Values[1] = r.Values[0] }},
		{name: "object identity differs from model", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) { r.Values[1].Subject.Subject.Kind = objectidentity.KindTable }},
		{name: "missing placement", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) { r.Values[1].Subject.Placement = "" }},
		{name: "unstructured identity", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) { r.Values[1].Subject.Subject.Name.Source = "" }},
		{name: "value contradicts absence", want: schemaext.ErrInvalidValue, adjust: func(r *schemaext.RelationRequest) {
			r.Coverage = must.Must(schemaext.NewCoverage(r.Representation, r.Coverage.KindRecords(), []schemaext.SubjectCoverage{{
				Kind: conversionFirst, Subject: r.Values[1].Subject.Subject, Knowledge: schemaext.Knowledge{State: schemaext.Absent}}}))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			runtime := mustRuntime(c, relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
				calls++
				return relationReply(r), nil
			})))
			request := relationRequest(runtime, relationObject(conversionFirst, "first"), relationObject(conversionFirst, "second"))
			test.adjust(&request)
			snapshot, err := runtime.CaptureRelations(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(calls, qt.Equals, 0)
			c.Assert(snapshot.Records(), qt.HasLen, 0)
		})
	}
}

func TestRelationRegistryGrowthCannotEnrollOldSources(t *testing.T) {
	c := qt.New(t)
	calls := 0
	provider := relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
		calls++
		return relationReply(r), nil
	}))
	runtime := mustRuntime(c, provider)
	request := relationRequest(runtime)
	request.Coverage = request.Coverage.SelectKinds([]schemaext.Kind{conversionFirst})
	request.Kinds = []schemaext.Kind{conversionFirst}
	snapshot, err := runtime.CaptureRelations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(snapshot.Coverage().KindRecords(), qt.HasLen, 1)
	related, err := snapshot.CaptureRelated(t.Context(), []objectidentity.ID{objectidentity.NewBuilder(identifier.ForDialect("postgres")).Table("orders")})
	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompleteRelations)
	c.Assert(related, qt.DeepEquals, schemaext.RelatedFeatures{})
	complete, err := runtime.CaptureRelations(t.Context(), relationRequest(runtime))
	c.Assert(err, qt.IsNil)
	related, err = complete.CaptureRelated(t.Context(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(related.Values, qt.HasLen, 0)
	c.Assert(related.Required, qt.HasLen, 0)
}

func TestRelationCaptureTerminatesCyclesAndSelectsQualifiedSchemas(t *testing.T) {
	c := qt.New(t)
	first, second := relationObject(conversionFirst, "first"), relationObject(conversionSecond, "second")
	first.Value.(*linkedValue).Targets = []objectidentity.ID{second.Subject.Subject}
	second.Value.(*linkedValue).Targets = []objectidentity.ID{first.Subject.Subject}
	unrelated := relationObject(conversionFirst, "unrelated")
	unrelated.Subject.Subject.Schema = objectidentity.Part{Source: "elsewhere", Normalized: "elsewhere"}
	runtime := mustRuntime(c, relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
		return relationReply(r), nil
	})))
	snapshot, err := runtime.CaptureRelations(t.Context(), relationRequest(runtime, first, second, unrelated))
	c.Assert(err, qt.IsNil)
	fromObject, err := snapshot.CaptureRelated(t.Context(), []objectidentity.ID{first.Subject.Subject})
	c.Assert(err, qt.IsNil)
	c.Assert(fromObject.Values, qt.DeepEquals, []schemaext.RelationValue{first, second})
	fromSchema, err := snapshot.CaptureRelated(t.Context(), []objectidentity.ID{{Kind: objectidentity.KindSchema, Name: first.Subject.Subject.Schema}})
	c.Assert(err, qt.IsNil)
	c.Assert(fromSchema.Values, qt.DeepEquals, fromObject.Values)
}

func TestRelationCaptureRequiresContextAndCannotBeImplicitlySerialized(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
		return relationReply(r), nil
	})))
	request := relationRequest(runtime, relationObject(conversionFirst, "policy"))
	var missingContext context.Context
	_, err := runtime.CaptureRelations(missingContext, request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	var missingRuntime *engine.Runtime
	_, err = missingRuntime.CaptureRelations(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	snapshot, err := runtime.CaptureRelations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	related, err := snapshot.CaptureRelated(ctx, nil)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(related, qt.DeepEquals, schemaext.RelatedFeatures{})
	_, err = json.Marshal(snapshot)
	c.Assert(err, qt.ErrorIs, schemaext.ErrExplicitCodec)
	err = json.Unmarshal([]byte(`{}`), &snapshot)
	c.Assert(err, qt.ErrorIs, schemaext.ErrExplicitCodec)
	_, err = json.Marshal(request.Values[0])
	c.Assert(err, qt.ErrorIs, schemaext.ErrExplicitCodec)
}
