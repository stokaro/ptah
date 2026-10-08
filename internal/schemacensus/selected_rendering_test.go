package schemacensus_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/internal/schemacensus"
)

type schemaRenderFunc func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error)

func (f schemaRenderFunc) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	return f(ctx, request)
}

func TestCensusDoesNotCountProviderFailureAsSchemaRefusal(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("census renderer failed")
	calls := 0
	service := schemaRenderFunc(func(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Schema, qt.IsNotNil)
		return renderer.SchemaResult{Complete: true}, failure
	})
	observations, err := schemacensus.Measure(t.Context(), service)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(observations, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	emissions, err := schemacensus.MeasureEmissions(t.Context(), service)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(emissions, qt.DeepEquals, schemacensus.CorpusEmissions{})
	c.Assert(calls, qt.Equals, 2)
}

func TestCensusModelRefusalDoesNotHideProviderFailure(t *testing.T) {
	failure := errors.New("provider transport failed")
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "unclassified invalid value", err: schemaext.ErrInvalidValue, want: schemaext.ErrInvalidValue},
		{name: "joined transport failure", err: errors.Join(
			&schemaext.InvalidModelError{Kind: "example.org/table", Representation: schemaext.Desired, Message: "invalid setting"},
			failure,
		), want: failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := schemaRenderFunc(func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
				return renderer.SchemaResult{Complete: true}, test.err
			})
			result, err := schemacensus.Measure(t.Context(), service)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}
