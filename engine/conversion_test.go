package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type conversionFunc func(context.Context, schemaext.ConversionRequest) ([]schemaext.Value, error)

func (f conversionFunc) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return f(ctx, request)
}

type conversionValue struct {
	ID     schemaext.Kind
	Number int
}

func (v *conversionValue) Kind() schemaext.Kind { return v.ID }
func (v *conversionValue) Clone() schemaext.Value {
	return &conversionValue{ID: v.ID, Number: v.Number}
}
func (v *conversionValue) Equal(other schemaext.Value) bool {
	w, ok := other.(*conversionValue)
	return ok && w != nil && *v == *w
}

const conversionFirst schemaext.Kind = "example.org/first"
const conversionSecond schemaext.Kind = "example.org/second"

func conversionCodec(kind schemaext.Kind, representation schemaext.Representation) schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) {
		return json.Marshal(value.(*conversionValue).Number)
	}
	return schemaext.Codec{Prototype: &conversionValue{ID: kind}, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"integer"}`),
		Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*conversionValue).Clone(), nil },
		Encode:     encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			number, err := schemaext.DecodeJSON[int](data)
			return &conversionValue{ID: kind, Number: number}, err
		},
	}
}

func conversionProvider(service schemaext.ConversionService) engine.Provider {
	return engine.Provider{ID: "example.org/converter", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}}},
		Codecs: []schemaext.Codec{conversionCodec(conversionFirst, schemaext.Desired), conversionCodec(conversionFirst, schemaext.Observed),
			conversionCodec(conversionSecond, schemaext.Desired), conversionCodec(conversionSecond, schemaext.Observed)},
		Conversions: []engine.Conversion{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, Service: service}},
	}
}

func TestConversion_BatchedByServiceAndIsolatedFromMutations(t *testing.T) {
	c := qt.New(t)
	calls := 0
	var received schemaext.ConversionRequest
	service := conversionFunc(func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
		calls++
		received = request
		for _, value := range request.Values {
			value.(*conversionValue).Number *= 2
		}
		return request.Values, nil
	})
	provider := conversionProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Conversions[0].Kinds[0] = "example.org/mutated"
	provider.Conversions[0].Service = nil
	input := []schemaext.Value{&conversionValue{ID: conversionSecond, Number: 3}, &conversionValue{ID: conversionFirst, Number: 5}}
	result, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: " ALTERNATE ", From: schemaext.Desired, To: schemaext.Observed, Values: input})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(input[0].(*conversionValue).Number, qt.Equals, 3)
	c.Assert(result[0].(*conversionValue).Number, qt.Equals, 6)
	c.Assert(result[1].(*conversionValue).Number, qt.Equals, 10)
	received.Values[0].(*conversionValue).Number = 100
	c.Assert(result[0].(*conversionValue).Number, qt.Equals, 6)
}

func TestConversion_RejectsRegistrationConflicts(t *testing.T) {
	service := conversionFunc(func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
		return request.Values, nil
	})
	cases := []struct {
		name   string
		change func(*engine.Provider)
	}{
		{name: "duplicate kind", change: func(p *engine.Provider) { p.Conversions[0].Kinds = append(p.Conversions[0].Kinds, conversionFirst) }},
		{name: "overlapping services", change: func(p *engine.Provider) { p.Conversions = append(p.Conversions, p.Conversions[0]) }},
		{name: "unknown target", change: func(p *engine.Provider) { p.Conversions[0].Target = "absent" }},
		{name: "alias registration", change: func(p *engine.Provider) { p.Conversions[0].Target = "alternate" }},
		{name: "missing representation", change: func(p *engine.Provider) { p.Codecs = p.Codecs[:1] }},
		{name: "no kinds", change: func(p *engine.Provider) { p.Conversions[0].Kinds = nil }},
		{name: "nil service", change: func(p *engine.Provider) { p.Conversions[0].Service = nil }},
		{name: "typed nil service", change: func(p *engine.Provider) { var missing conversionFunc; p.Conversions[0].Service = missing }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := conversionProvider(service)
			test.change(&provider)
			runtime, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func TestConversion_RejectsIncompleteOrReorderedReplies(t *testing.T) {
	transport := errors.New("provider exited")
	cases := []struct {
		name    string
		service conversionFunc
		want    error
	}{
		{name: "transport failure", want: transport, service: func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
			return request.Values[:1], transport
		}},
		{name: "missing value", want: schemaext.ErrInvalidValue, service: func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
			return request.Values[:1], nil
		}},
		{name: "reordered kinds", want: schemaext.ErrInvalidValue, service: func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
			return []schemaext.Value{request.Values[1], request.Values[0]}, nil
		}},
		{name: "nil value", want: schemaext.ErrInvalidValue, service: func(context.Context, schemaext.ConversionRequest) ([]schemaext.Value, error) {
			return []schemaext.Value{nil, nil}, nil
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, conversionProvider(test.service))
			result, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "custom", From: schemaext.Desired, To: schemaext.Observed,
				Values: []schemaext.Value{&conversionValue{ID: conversionFirst}, &conversionValue{ID: conversionSecond}}})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestConversion_CancellationAfterProviderDiscardsReply(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := conversionFunc(func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
		cancel()
		return request.Values, nil
	})
	runtime := mustRuntime(c, conversionProvider(service))
	result, err := runtime.ConvertFeatures(ctx, schemaext.ConversionRequest{Target: "custom", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{&conversionValue{ID: conversionFirst}}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}

func TestConversion_ValidatesWholeRequestBeforeCallingServices(t *testing.T) {
	c := qt.New(t)
	calls := 0
	service := conversionFunc(func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
		calls++
		return request.Values, nil
	})
	provider := conversionProvider(service)
	provider.Conversions[0].Kinds = []schemaext.Kind{conversionFirst}
	runtime := mustRuntime(c, provider)
	result, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "custom", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{&conversionValue{ID: conversionFirst}, &conversionValue{ID: conversionSecond}}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.IsNil)
	c.Assert(calls, qt.Equals, 0)
}
