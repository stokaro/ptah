package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type facetNormalizationFunc func(context.Context, schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error)

func (f facetNormalizationFunc) NormalizeFacets(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
	return f(ctx, request)
}

func facetNormalizationProvider(service schemaext.FacetNormalizationService) engine.Provider {
	provider := conversionProvider(nil)
	provider.Conversions = nil
	provider.FacetNormalizations = []engine.FacetNormalization{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Service: service}}
	return provider
}
func facetNormalizationRequest() schemaext.FacetNormalizationRequest {
	compared := facetRequest()
	return schemaext.FacetNormalizationRequest{Target: compared.Target, Desired: compared.Desired, Current: compared.Current, Owners: compared.Owners, Session: &probeSession{}}
}
func unchangedFacets(_ context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
	return schemaext.FacetNormalizationResult{Complete: true, Desired: request.Desired}, nil
}

func TestNormalizeFacets_HappyPath(t *testing.T) {
	c := qt.New(t)
	var received schemaext.FacetNormalizationRequest
	var returned schemaext.FacetNormalizationResult
	provider := facetNormalizationProvider(facetNormalizationFunc(func(_ context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		received = request
		values := request.Desired.Records[0].Values
		request.Desired.Records[0].Values = must.Must(values.Replace(&conversionValue{ID: conversionFirst, Number: 42}))
		returned = schemaext.FacetNormalizationResult{Complete: true, Desired: request.Desired}
		return returned, nil
	}))
	runtime := mustRuntime(c, provider)
	provider.FacetNormalizations[0].Kinds[0] = conversionSecond
	provider.FacetNormalizations[0].OwnerKinds[0] = objectidentity.KindColumn
	request := facetNormalizationRequest()
	before := must.Must(request.Desired.Records[0].Values.Values())
	result, err := runtime.NormalizeFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(received.Session, qt.Equals, request.Session)
	c.Assert(received.Desired.Records[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionFirst})
	c.Assert(received.Current.Records[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionFirst})
	c.Assert(received.Owners, qt.DeepEquals, request.Owners)
	value, found, err := schemaext.FacetAs[*conversionValue](result.Desired.Records[0].Values, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Number, qt.Equals, 42)
	originalOther := must.Must(request.Desired.Records[0].Values.Values())[1]
	actualOther, _, err := result.Desired.Records[0].Values.Get(conversionSecond)
	c.Assert(err, qt.IsNil)
	c.Assert(actualOther.Equal(originalOther), qt.IsTrue)
	returned.Desired.Records[0].Subject.Name.Source = "changed"
	received.Owners[0].Current = false
	result.Desired.Records[0].Subject.Name.Source = "result only"
	c.Assert(request.Owners[0].Current, qt.IsTrue)
	c.Assert(request.Desired.Records[0].Subject.Name.Source, qt.Not(qt.Equals), "result only")
	c.Assert(must.Must(request.Desired.Records[0].Values.Values()), qt.DeepEquals, before)
}

func TestNormalizeFacets_PreservesBindingsAndSkipsExcludedValues(t *testing.T) {
	for _, test := range []struct {
		name, target string
		calls        int
	}{
		{name: "active alias", target: "alternate", calls: 1},
		{name: "excluded", target: "elsewhere", calls: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			runtime := mustRuntime(c, facetNormalizationProvider(facetNormalizationFunc(func(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
				calls++
				return unchangedFacets(ctx, request)
			})))
			request := facetNormalizationRequest()
			request.Desired.Records[0].Values = must.Must(request.Desired.Records[0].Values.WithTargetScope(conversionFirst, test.target))
			result, err := runtime.NormalizeFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(calls, qt.Equals, test.calls)
			c.Assert(result.Desired.Records[0].Values.TargetScope(conversionFirst), qt.DeepEquals, []string{test.target})
			c.Assert(must.Must(result.Desired.Records[0].Values.Values()), qt.DeepEquals, must.Must(request.Desired.Records[0].Values.Values()))
		})
	}
}

func TestNormalizeFacets_RejectsChangedDeclarations(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*schemaext.FacetNormalizationResult)
	}{
		{name: "incomplete", change: func(r *schemaext.FacetNormalizationResult) { r.Complete = false }},
		{name: "dropped subject", change: func(r *schemaext.FacetNormalizationResult) { r.Desired.Records = nil }},
		{name: "duplicate subject", change: func(r *schemaext.FacetNormalizationResult) {
			r.Desired.Records = append(r.Desired.Records, r.Desired.Records[0])
		}},
		{name: "renamed subject", change: func(r *schemaext.FacetNormalizationResult) { r.Desired.Records[0].Subject.Name.Source = "renamed" }},
		{name: "lost kind", change: func(r *schemaext.FacetNormalizationResult) { r.Desired.Records[0].Values = schemaext.Facets{} }},
		{name: "extra kind", change: func(r *schemaext.FacetNormalizationResult) {
			r.Desired.Records[0].Values = must.Must(r.Desired.Records[0].Values.With(&conversionValue{ID: conversionSecond, Number: 9}))
		}},
		{name: "changed binding", change: func(r *schemaext.FacetNormalizationResult) {
			r.Desired.Records[0].Values = must.Must(r.Desired.Records[0].Values.WithTargetScope(conversionFirst, "custom"))
		}},
		{name: "lost coverage", change: func(r *schemaext.FacetNormalizationResult) { r.Desired.Coverage = schemaext.Coverage{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, facetNormalizationProvider(facetNormalizationFunc(func(_ context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
				result := schemaext.FacetNormalizationResult{Complete: true, Desired: request.Desired}
				test.change(&result)
				return result, nil
			})))
			request := facetNormalizationRequest()
			request.Desired.Coverage = facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)
			result, err := runtime.NormalizeFacets(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Desired.Records, qt.HasLen, 0)
		})
	}
}

func TestNormalizeFacets_ValidatesInputs(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*schemaext.FacetNormalizationRequest)
	}{
		{name: "missing parent", change: func(r *schemaext.FacetNormalizationRequest) { r.Owners = nil }},
		{name: "duplicate parent", change: func(r *schemaext.FacetNormalizationRequest) { r.Owners = append(r.Owners, r.Owners[0]) }},
		{name: "absent parent", change: func(r *schemaext.FacetNormalizationRequest) { r.Owners[0].Desired = false; r.Owners[0].Current = false }},
		{name: "no desired parent", change: func(r *schemaext.FacetNormalizationRequest) { r.Owners[0].Desired = false }},
		{name: "duplicate declaration", change: func(r *schemaext.FacetNormalizationRequest) {
			r.Desired.Records = append(r.Desired.Records, r.Desired.Records[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, facetNormalizationProvider(facetNormalizationFunc(unchangedFacets)))
			request := facetNormalizationRequest()
			test.change(&request)
			result, err := runtime.NormalizeFacets(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

func TestNormalizeFacets_PreservesFailureAndCancellation(t *testing.T) {
	failure := errors.New("normalization failed")
	for _, test := range []struct {
		name                           string
		serviceError, want             error
		cancelAtEntry, cancelInService bool
	}{
		{name: "failure", serviceError: failure, want: failure},
		{name: "already canceled", cancelAtEntry: true, want: context.Canceled},
		{name: "canceled provider", cancelInService: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelers := map[bool]func(){true: cancel, false: func() {}}
			cancelers[test.cancelAtEntry]()
			runtime := mustRuntime(c, facetNormalizationProvider(facetNormalizationFunc(func(_ context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
				cancelers[test.cancelInService]()
				return schemaext.FacetNormalizationResult{Complete: true, Desired: request.Desired}, test.serviceError
			})))
			result, err := runtime.NormalizeFacets(ctx, facetNormalizationRequest())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Desired.Records, qt.HasLen, 0)
		})
	}
}

func TestNormalizeFacets_RejectsUnknownTarget(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, facetNormalizationProvider(facetNormalizationFunc(unchangedFacets)))
	request := facetNormalizationRequest()
	request.Target = "elsewhere"
	result, err := runtime.NormalizeFacets(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result.Complete, qt.IsFalse)
}

func TestFacetNormalization_RejectsInvalidRegistration(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*engine.Provider)
	}{
		{name: "no target", change: func(p *engine.Provider) { p.FacetNormalizations[0].Target = "" }},
		{name: "alias target", change: func(p *engine.Provider) { p.FacetNormalizations[0].Target = "alternate" }},
		{name: "no kinds", change: func(p *engine.Provider) { p.FacetNormalizations[0].Kinds = nil }},
		{name: "no parent kinds", change: func(p *engine.Provider) { p.FacetNormalizations[0].OwnerKinds = nil }},
		{name: "duplicate kind", change: func(p *engine.Provider) {
			p.FacetNormalizations[0].Kinds = []schemaext.Kind{conversionFirst, conversionFirst}
		}},
		{name: "unknown kind", change: func(p *engine.Provider) { p.FacetNormalizations[0].Kinds = []schemaext.Kind{"example.org/unknown"} }},
		{name: "no observation codec", change: func(p *engine.Provider) { p.Codecs = p.Codecs[:1] }},
		{name: "nil service", change: func(p *engine.Provider) { p.FacetNormalizations[0].Service = nil }},
		{name: "typed nil service", change: func(p *engine.Provider) {
			var service facetNormalizationFunc
			p.FacetNormalizations[0].Service = service
		}},
		{name: "duplicate owner", change: func(p *engine.Provider) {
			p.FacetNormalizations = append(p.FacetNormalizations, p.FacetNormalizations[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := facetNormalizationProvider(facetNormalizationFunc(unchangedFacets))
			test.change(&provider)
			runtime, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func TestNormalizeFacets_PassesThroughUnregisteredKinds(t *testing.T) {
	c := qt.New(t)
	calls := 0
	provider := facetNormalizationProvider(facetNormalizationFunc(func(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		calls++
		return unchangedFacets(ctx, request)
	}))
	runtime := mustRuntime(c, provider)
	request := facetNormalizationRequest()
	request.Desired.Records[0].Values = request.Desired.Records[0].Values.Without(conversionFirst)
	result, err := runtime.NormalizeFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 0)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(must.Must(result.Desired.Records[0].Values.Values()), qt.DeepEquals, must.Must(request.Desired.Records[0].Values.Values()))
}

func TestNormalizeFacets_FiltersParentKinds(t *testing.T) {
	c := qt.New(t)
	var received schemaext.FacetNormalizationRequest
	runtime := mustRuntime(c, facetNormalizationProvider(facetNormalizationFunc(func(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		received = request
		return unchangedFacets(ctx, request)
	})))
	request := facetNormalizationRequest()
	column := request.Owners[0].Subject
	column.Kind = objectidentity.KindColumn
	column.Parent = column.Name
	column.Name = objectidentity.Part{Source: "id", Normalized: "id"}
	request.Owners = append(request.Owners, schemaext.ParentState{Subject: column, Desired: true})
	request.Desired.Records = append(request.Desired.Records, schemaext.FacetRecord{Subject: column, Values: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 7}))})
	// An ordinary current table has no facet but remains a present parent.
	request.Current = schemaext.FacetState{}
	result, err := runtime.NormalizeFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(received.Owners, qt.HasLen, 1)
	c.Assert(received.Owners[0].Current, qt.IsTrue)
	c.Assert(received.Current.Records, qt.HasLen, 0)
	c.Assert(received.Desired.Records, qt.HasLen, 1)
	c.Assert(result.Desired.Records, qt.HasLen, 2)
	c.Assert(result.Desired.Records[1].Subject, qt.DeepEquals, column)
}

func TestNormalizeFacets_DiscardsEarlierSuccessfulBatchesOnFailure(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("second owner failed")
	calls := 0
	provider := facetNormalizationProvider(facetNormalizationFunc(func(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		calls++
		return unchangedFacets(ctx, request)
	}))
	provider.FacetNormalizations = append(provider.FacetNormalizations, engine.FacetNormalization{Target: "custom", Kinds: []schemaext.Kind{conversionSecond}, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Service: facetNormalizationFunc(func(context.Context, schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		calls++
		return schemaext.FacetNormalizationResult{}, failure
	})})
	runtime := mustRuntime(c, provider)
	result, err := runtime.NormalizeFacets(t.Context(), facetNormalizationRequest())
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(calls, qt.Equals, 2)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Desired.Records, qt.HasLen, 0)
}
