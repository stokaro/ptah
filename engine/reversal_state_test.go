package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func projectedFixture(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	results, err := reverseFixture(ctx, request)
	if err != nil {
		return nil, err
	}
	for i, result := range results {
		kind := result.Change.Value.Kind()
		results[i].ForwardState = []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: kind,
			Value: &conversionValue{ID: kind, Number: request.Changes[i].Value.(*reversalChange).Number}}}
	}
	return results, nil
}

func TestReversalProjectsOwnedStateWithoutAliasingTheReply(t *testing.T) {
	for _, test := range []struct {
		name      string
		placement schemaext.Placement
		value     schemaext.Value
	}{
		{name: "named object", placement: schemaext.ObjectPlacement, value: &conversionValue{ID: conversionFirst, Number: 3}},
		{name: "facet", placement: schemaext.FacetPlacement, value: &conversionValue{ID: conversionFirst, Number: 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var reply []schemaext.Reversal
			provider := reversalProvider(reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
				var err error
				reply, err = reverseFixture(ctx, request)
				reply[0].ForwardState = []schemaext.ProjectedValue{{Placement: test.placement, Kind: conversionFirst, Value: test.value}}
				return reply, err
			}))
			provider.Codecs = append(provider.Codecs, conversionCodec(conversionFirst, schemaext.Observed))
			result, err := mustRuntime(c, provider).ReverseChanges(t.Context(), reversalRequest())
			c.Assert(err, qt.IsNil)
			projection := result[0].ForwardState[0]
			c.Assert(projection.Placement, qt.Equals, test.placement)
			c.Assert(projection.Value, qt.DeepEquals, test.value)
			reply[0].ForwardState[0].Kind = conversionSecond
			c.Assert(projection.Kind, qt.Equals, conversionFirst)
			test.value.(*conversionValue).Number = 99
			c.Assert(projection.Value.(*conversionValue).Number, qt.Equals, 3)
		})
	}
}

func TestReversalRejectsInvalidStateProjections(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func([]schemaext.Reversal)
	}{
		{name: "unknown placement", edit: func(r []schemaext.Reversal) { r[0].ForwardState[0].Placement = "unknown" }},
		{name: "object kind changes subject", edit: func(r []schemaext.Reversal) { r[0].ForwardState[0].Kind = conversionSecond }},
		{name: "unowned absent facet", edit: func(r []schemaext.Reversal) {
			r[0].ForwardState[0] = schemaext.ProjectedValue{Placement: schemaext.FacetPlacement, Kind: "example.org/foreign"}
		}},
		{name: "typed nil value", edit: func(r []schemaext.Reversal) { r[0].ForwardState[0].Value = (*conversionValue)(nil) }},
		{name: "wrong value kind", edit: func(r []schemaext.Reversal) { r[0].ForwardState[0].Value = &conversionValue{ID: conversionSecond} }},
		{name: "duplicate state", edit: func(r []schemaext.Reversal) { r[0].ForwardState = append(r[0].ForwardState, r[0].ForwardState[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := reversalProvider(reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
				result, err := projectedFixture(ctx, request)
				test.edit(result)
				return result, err
			}))
			provider.Codecs = append(provider.Codecs, conversionCodec(conversionFirst, schemaext.Observed), conversionCodec(conversionSecond, schemaext.Observed))
			result, err := mustRuntime(c, provider).ReverseChanges(t.Context(), reversalRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestReversalRejectsCompetingProjectionsFromDistinctChanges(t *testing.T) {
	c := qt.New(t)
	provider := reversalProvider(reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
		result, err := reverseFixture(ctx, request)
		for i := range result {
			result[i].ForwardState = []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: conversionFirst}}
		}
		return result, err
	}))
	provider.Codecs = append(provider.Codecs, conversionCodec(conversionFirst, schemaext.Observed))
	request := reversalRequest()
	request.Changes = request.Changes[:2]
	request.Changes[1].Subject = request.Changes[0].Subject
	result, err := mustRuntime(c, provider).ReverseChanges(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}

func TestReversalProjectsExplicitAbsenceWithoutAliasingTheReply(t *testing.T) {
	for _, placement := range []schemaext.Placement{schemaext.ObjectPlacement, schemaext.FacetPlacement} {
		t.Run(string(placement), func(t *testing.T) {
			c := qt.New(t)
			var reply []schemaext.Reversal
			provider := reversalProvider(reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
				var err error
				reply, err = reverseFixture(ctx, request)
				reply[0].ForwardState = []schemaext.ProjectedValue{{Placement: placement, Kind: conversionFirst}}
				return reply, err
			}))
			provider.Codecs = append(provider.Codecs, conversionCodec(conversionFirst, schemaext.Observed))
			result, err := mustRuntime(c, provider).ReverseChanges(t.Context(), reversalRequest())
			c.Assert(err, qt.IsNil)
			c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: placement, Kind: conversionFirst}})
			reply[0].ForwardState[0].Kind = conversionSecond
			c.Assert(result[0].ForwardState[0].Kind, qt.Equals, conversionFirst)
		})
	}
}
