package engine_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
)

func facetPreparation() engine.Provider {
	provider := preparationProvider(preparationFunc(func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		request.Tables[0].ResolvedFacets = []schemaext.FacetRecord{{Subject: request.Tables[0].Subject, Values: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 2}))}}
		return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
	}))
	provider.Codecs = []schemaext.Codec{conversionCodec(conversionFirst, schemaext.Desired)}
	return provider
}

func facetPreparationRequest() schemapreparation.Request {
	request := preparationRequest()
	request.Tables[0].Desired.Table.Facets = must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 1}))
	return request
}

func TestPreparationResolvesOnlyItsOwnModels(t *testing.T) {
	c := qt.New(t)
	provider := facetPreparation()
	source := facetPreparationRequest()
	runtime := mustRuntime(c, provider)
	result, err := runtime.PrepareTables(t.Context(), source)
	c.Assert(err, qt.IsNil)
	resolved, found, err := schemaext.FacetAs[*conversionValue](result.Tables[0].ResolvedFacets[0].Values, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(resolved.Number, qt.Equals, 2)
	c.Assert(result.Tables[0].Desired.Table.Facets, qt.DeepEquals, source.Tables[0].Desired.Table.Facets)
	foreign := engine.Provider{ID: "example.org/foreign", Codecs: provider.Codecs}
	provider.Codecs = nil
	runtime = mustRuntime(c, provider, foreign)
	result, err = runtime.PrepareTables(t.Context(), source)
	c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
	c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
}

func cloneValidPreparedValue(value schemaext.Payload) (schemaext.Payload, error) {
	typed, ok := value.(*conversionValue)
	if !ok {
		return nil, schemaext.ErrInvalidValue
	}
	if typed.Number == 2 {
		return nil, fmt.Errorf("%w: invalid resolved payload", schemaext.ErrInvalidValue)
	}
	return typed.Clone(), nil
}

func TestPreparationValidatesResolvedPayloadThroughItsCodec(t *testing.T) {
	c := qt.New(t)
	provider := facetPreparation()
	provider.Codecs[0].Clone = cloneValidPreparedValue
	runtime := mustRuntime(c, provider)
	result, err := runtime.PrepareTables(t.Context(), facetPreparationRequest())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
}

func TestPreparationRefusesResolvedInputBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	called := false
	provider := facetPreparation()
	provider.Targets[0].Preparation = preparationFunc(func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		called = true
		return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
	})
	runtime := mustRuntime(c, provider)
	request := facetPreparationRequest()
	request.Tables[0].ResolvedFacets = []schemaext.FacetRecord{{Subject: request.Tables[0].Subject, Values: request.Tables[0].Desired.Table.Facets}}
	result, err := runtime.PrepareTables(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
	c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
	c.Assert(called, qt.IsFalse)
}
