package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type reversalFunc func(context.Context, schemaext.ReversalRequest) ([]schemaext.Reversal, error)

func (f reversalFunc) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return f(ctx, request)
}

type reversalChange struct {
	ID     schemaext.Kind
	Number int
}

func (v *reversalChange) Kind() schemaext.Kind { return v.ID }
func (v *reversalChange) CloneChange() schemaext.ChangeValue {
	return &reversalChange{ID: v.ID, Number: v.Number}
}

func reversalCodec(kind schemaext.Kind) schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) {
		return json.Marshal(value.(*reversalChange).Number)
	}
	return schemaext.Codec{Prototype: &reversalChange{ID: kind}, Representation: schemaext.Change, Version: 1, Definition: json.RawMessage(`{"type":"integer"}`),
		Clone: func(value schemaext.Payload) (schemaext.Payload, error) {
			return value.(*reversalChange).CloneChange(), nil
		}, Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			number, err := schemaext.DecodeJSON[int](data)
			return &reversalChange{ID: kind, Number: number}, err
		},
	}
}

func reversalProvider(service schemaext.ReversalService) engine.Provider {
	return engine.Provider{ID: "example.org/reverser", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}}},
		Codecs:    []schemaext.Codec{reversalCodec(conversionFirst), reversalCodec(conversionSecond)},
		Reversals: []engine.Reversal{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, Service: service}},
	}
}

func reverseFixture(_ context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	result := make([]schemaext.Reversal, len(request.Changes))
	for i, change := range request.Changes {
		value := change.Value.(*reversalChange)
		change.Value = &reversalChange{ID: value.ID, Number: -value.Number}
		result[i] = schemaext.Reversal{Change: change, Strategy: "restore the prior definition", Limitations: []string{"previously removed data is not recovered"}}
	}
	return result, nil
}

func reversalRequest() schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: " ALTERNATE ", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Capabilities{capability.Changefeeds: true},
		Changes: []schemaext.ChangeRecord{
			{Subject: comparedRef(conversionFirst, "one"), Value: &reversalChange{ID: conversionFirst, Number: 3}},
			{Subject: comparedRef(conversionSecond, "two"), Value: &reversalChange{ID: conversionSecond, Number: 5}},
			{Subject: comparedRef(conversionFirst, "three"), Value: &reversalChange{ID: conversionFirst, Number: 7}},
		},
	}
}

func TestReversalBatchesOwnedKindsAndIsolatesRequestsAndReplies(t *testing.T) {
	c := qt.New(t)
	calls := 0
	var received schemaext.ReversalRequest
	var reply []schemaext.Reversal
	var receivedContext context.Context
	service := reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
		calls++
		received, receivedContext = request, ctx
		request.Capabilities[capability.Changefeeds] = false
		var err error
		reply, err = reverseFixture(ctx, request)
		request.Changes[0].Value.(*reversalChange).Number = 99
		return reply, err
	})
	provider := reversalProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Reversals[0].Kinds[0] = "example.org/replaced"
	provider.Reversals[0].Service = nil
	request := reversalRequest()
	result, err := runtime.ReverseChanges(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(receivedContext, qt.Equals, t.Context())
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(received.Changes, qt.HasLen, 3)
	c.Assert(request.Capabilities.Has(capability.Changefeeds), qt.IsTrue)
	c.Assert(request.Changes[0].Value.(*reversalChange).Number, qt.Equals, 3)
	c.Assert(result[0].Change.Value.(*reversalChange).Number, qt.Equals, -3)
	c.Assert(result[1].Change.Value.(*reversalChange).Number, qt.Equals, -5)
	c.Assert(result[2].Change.Value.(*reversalChange).Number, qt.Equals, -7)
	reply[0].Change.Value.(*reversalChange).Number = 500
	reply[0].Limitations[0] = "mutated"
	c.Assert(result[0].Change.Value.(*reversalChange).Number, qt.Equals, -3)
	c.Assert(result[0].Limitations, qt.DeepEquals, []string{"previously removed data is not recovered"})
}

func TestReversalRejectsConflictingOrUnownedRegistration(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*engine.Provider)
	}{
		{name: "missing service", change: func(p *engine.Provider) { p.Reversals[0].Service = nil }},
		{name: "typed nil service", change: func(p *engine.Provider) { var missing reversalFunc; p.Reversals[0].Service = missing }},
		{name: "no kinds", change: func(p *engine.Provider) { p.Reversals[0].Kinds = nil }},
		{name: "duplicate kind", change: func(p *engine.Provider) { p.Reversals[0].Kinds = append(p.Reversals[0].Kinds, conversionFirst) }},
		{name: "competing handlers", change: func(p *engine.Provider) { p.Reversals = append(p.Reversals, p.Reversals[0]) }},
		{name: "missing codec", change: func(p *engine.Provider) { p.Codecs = p.Codecs[:1] }},
		{name: "unknown target", change: func(p *engine.Provider) { p.Reversals[0].Target = "missing" }},
		{name: "alias target", change: func(p *engine.Provider) { p.Reversals[0].Target = "alternate" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := reversalProvider(reversalFunc(reverseFixture))
			test.change(&provider)
			runtime, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func TestReversalValidatesWholeBatchBeforeCallingAnOwner(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*engine.Provider, *schemaext.ReversalRequest)
		want error
	}{
		{name: "missing later handler", edit: func(p *engine.Provider, _ *schemaext.ReversalRequest) {
			p.Reversals[0].Kinds = p.Reversals[0].Kinds[:1]
		}, want: ptaherr.ErrUnsupportedFeature},
		{name: "duplicate subject and kind", edit: func(_ *engine.Provider, r *schemaext.ReversalRequest) { r.Changes[2] = r.Changes[0] }, want: schemaext.ErrInvalidValue},
		{name: "nil later payload", edit: func(_ *engine.Provider, r *schemaext.ReversalRequest) { r.Changes[2].Value = nil }, want: schemaext.ErrInvalidValue},
		{name: "unknown target", edit: func(_ *engine.Provider, r *schemaext.ReversalRequest) { r.Target = "missing" }, want: ptaherr.ErrUnsupportedDialect},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			provider := reversalProvider(reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
				calls++
				return reverseFixture(ctx, request)
			}))
			request := reversalRequest()
			test.edit(&provider, &request)
			result, err := mustRuntime(c, provider).ReverseChanges(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestReversalDiscardsInvalidOrFailedReplies(t *testing.T) {
	transport := errors.New("provider stopped")
	for _, test := range []struct {
		name string
		edit func([]schemaext.Reversal) ([]schemaext.Reversal, error)
		want error
	}{
		{name: "service failure", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) { return r[:1], transport }, want: transport},
		{name: "irreversible change", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) { return r[:1], schemaext.ErrIrreversible }, want: schemaext.ErrIrreversible},
		{name: "partial success", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) { return r[:1], nil }, want: schemaext.ErrInvalidValue},
		{name: "reordered subjects", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) { r[0], r[2] = r[2], r[0]; return r, nil }, want: schemaext.ErrInvalidValue},
		{name: "changed provenance", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) {
			r[0].Change.Subject.Name.Source = "ONE"
			return r, nil
		}, want: schemaext.ErrInvalidValue},
		{name: "changed kind", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) {
			r[0].Change.Value.(*reversalChange).ID = conversionSecond
			return r, nil
		}, want: schemaext.ErrInvalidValue},
		{name: "no strategy", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) { r[0].Strategy = ""; return r, nil }, want: schemaext.ErrInvalidValue},
		{name: "multiline strategy", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) {
			r[0].Strategy = "restore\nSQL"
			return r, nil
		}, want: schemaext.ErrInvalidValue},
		{name: "empty limitation", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) {
			r[0].Limitations = []string{""}
			return r, nil
		}, want: schemaext.ErrInvalidValue},
		{name: "unicode line separator", edit: func(r []schemaext.Reversal) ([]schemaext.Reversal, error) {
			r[0].Limitations = []string{"loss\u2028SQL"}
			return r, nil
		}, want: schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, reversalProvider(reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
				result, err := reverseFixture(ctx, request)
				c.Assert(err, qt.IsNil)
				return test.edit(result)
			})))
			result, err := runtime.ReverseChanges(t.Context(), reversalRequest())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestReversalCancellationDiscardsServiceOutput(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	runtime := mustRuntime(c, reversalProvider(reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
		calls++
		cancel()
		return reverseFixture(ctx, request)
	})))
	result, err := runtime.ReverseChanges(ctx, reversalRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	result, err = runtime.ReverseChanges(ctx, reversalRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
}

func TestReversalRequiresItsOwnCodecEvenWhenAnotherProviderKnowsIt(t *testing.T) {
	c := qt.New(t)
	provider := reversalProvider(reversalFunc(reverseFixture))
	foreign := engine.Provider{ID: "example.org/foreign", Codecs: provider.Codecs}
	provider.Codecs = nil
	runtime, err := engine.New(provider, foreign)
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
}

func TestReversalDiscardsEarlierOwnerResultsWhenALaterOwnerFails(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("second owner unavailable")
	var calls []string
	provider := reversalProvider(nil)
	provider.Reversals = []engine.Reversal{
		{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, Service: reversalFunc(func(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
			calls = append(calls, "first")
			c.Assert(request.Changes, qt.HasLen, 2)
			return reverseFixture(ctx, request)
		})},
		{Target: "custom", Kinds: []schemaext.Kind{conversionSecond}, Service: reversalFunc(func(_ context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
			calls = append(calls, "second")
			c.Assert(request.Changes, qt.HasLen, 1)
			return nil, failure
		})},
	}
	result, err := mustRuntime(c, provider).ReverseChanges(t.Context(), reversalRequest())
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.IsNil)
	c.Assert(calls, qt.DeepEquals, []string{"first", "second"})
}
