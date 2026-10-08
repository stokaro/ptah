package engine_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type propertyService struct {
	decode func(context.Context, schemaext.PropertyDecodeRequest) ([]schemaext.Value, error)
	encode func(context.Context, schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error)
}

func (s propertyService) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	return s.decode(ctx, request)
}

func (s propertyService) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	return s.encode(ctx, request)
}

func propertyProvider(service schemaext.PropertyService) engine.Provider {
	provider := conversionProvider(nil)
	provider.Conversions = nil
	provider.Properties = []engine.PropertySource{{
		Target: "custom", Format: schemaext.TablePlatformProperties, Service: service,
		Definitions: []schemaext.PropertyDefinition{
			{Kind: conversionFirst, Keys: []string{"number", "number.default"}},
			{Kind: conversionSecond, Keys: []string{"size"}},
		},
	}}
	return provider
}

func TestProperties_BatchOrderAndSnapshots(t *testing.T) {
	c := qt.New(t)
	var decodedRequest schemaext.PropertyDecodeRequest
	var encodedRequest schemaext.PropertyEncodeRequest
	var providerFragments []schemaext.PropertyFragment
	var providerValues []schemaext.Value
	decodeCalls, encodeCalls := 0, 0
	service := propertyService{
		decode: func(_ context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
			decodeCalls++
			decodedRequest = request
			request.Fragments[0].Properties["size"] = "mutated"
			providerValues = []schemaext.Value{&conversionValue{ID: conversionSecond, Number: 7}, &conversionValue{ID: conversionFirst, Number: 9}}
			return providerValues, nil
		},
		encode: func(_ context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
			encodeCalls++
			encodedRequest = request
			providerFragments = []schemaext.PropertyFragment{
				{Kind: conversionSecond, Properties: map[string]string{"size": strconv.Itoa(request.Values[0].(*conversionValue).Number)}},
				{Kind: conversionFirst, Properties: map[string]string{"number": ""}},
			}
			request.Values[0].(*conversionValue).Number = 100
			return providerFragments, nil
		},
	}
	provider := propertyProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Properties[0].Definitions[0].Keys[0] = "mutated"
	provider.Properties[0].Service = nil
	definitions, err := runtime.PropertyDefinitions("alternate", schemaext.TablePlatformProperties)
	c.Assert(err, qt.IsNil)
	c.Assert(definitions[0].Keys, qt.DeepEquals, []string{"number", "number.default"})
	definitions[0].Keys[0] = "mutated again"
	input := []schemaext.PropertyFragment{
		{Kind: conversionSecond, Properties: map[string]string{"size": "7"}},
		{Kind: conversionFirst, Properties: map[string]string{"number": "9"}},
	}
	values, err := runtime.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: " ALTERNATE ", Format: schemaext.TablePlatformProperties, Fragments: input})
	c.Assert(err, qt.IsNil)
	c.Assert(decodeCalls, qt.Equals, 1)
	c.Assert(decodedRequest.Target, qt.Equals, "custom")
	c.Assert(input[0].Properties["size"], qt.Equals, "7")
	providerValues[0].(*conversionValue).Number = 100
	c.Assert(values, qt.DeepEquals, []schemaext.Value{&conversionValue{ID: conversionSecond, Number: 7}, &conversionValue{ID: conversionFirst, Number: 9}})
	fragments, err := runtime.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: "alternate", Format: schemaext.TablePlatformProperties, Values: values})
	c.Assert(err, qt.IsNil)
	c.Assert(encodeCalls, qt.Equals, 1)
	c.Assert(encodedRequest.Target, qt.Equals, "custom")
	c.Assert(values[0].(*conversionValue).Number, qt.Equals, 7)
	providerFragments[0].Properties["size"] = "mutated"
	c.Assert(fragments, qt.DeepEquals, []schemaext.PropertyFragment{
		{Kind: conversionSecond, Properties: map[string]string{"size": "7"}},
		{Kind: conversionFirst, Properties: map[string]string{"number": ""}},
	})
}

func TestProperties_RejectRegistrationConflicts(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*engine.Provider)
	}{
		{"nil service", func(p *engine.Provider) { p.Properties[0].Service = nil }},
		{"typed nil service", func(p *engine.Provider) { p.Properties[0].Service = (*propertyService)(nil) }},
		{"target alias", func(p *engine.Provider) { p.Properties[0].Target = "alternate" }},
		{"missing target", func(p *engine.Provider) { p.Properties[0].Target = "unknown" }},
		{"missing format", func(p *engine.Provider) { p.Properties[0].Format = "" }},
		{"empty definitions", func(p *engine.Provider) { p.Properties[0].Definitions = nil }},
		{"missing desired codec", func(p *engine.Provider) { p.Codecs = nil }},
		{"empty keys", func(p *engine.Provider) { p.Properties[0].Definitions[0].Keys = nil }},
		{"invalid key", func(p *engine.Provider) { p.Properties[0].Definitions[0].Keys[0] = "a..b" }},
		{"shared key", func(p *engine.Provider) { p.Properties[0].Definitions[1].Keys[0] = "number" }},
		{"duplicate service", func(p *engine.Provider) { p.Properties = append(p.Properties, p.Properties[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := propertyProvider(propertyService{})
			test.change(&provider)
			runtime, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func TestPropertyDecoderValidatesWholeBatchBeforeDispatch(t *testing.T) {
	for _, properties := range []map[string]string{{"size": "belongs to another kind"}, {"number": "nul\x00"}, {"number": string([]byte{0xff})}} {
		c := qt.New(t)
		called := false
		runtime := mustRuntime(c, propertyProvider(propertyService{decode: func(context.Context, schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
			called = true
			return nil, nil
		}}))
		result, err := runtime.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties,
			Fragments: []schemaext.PropertyFragment{{Kind: conversionFirst, Properties: map[string]string{"number": "1"}}, {Kind: conversionFirst, Properties: properties}}})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(result, qt.IsNil)
		c.Assert(called, qt.IsFalse)
	}
}

func TestPropertyDecoderRejectsInvalidReplies(t *testing.T) {
	for _, reply := range [][]schemaext.Value{nil, {&conversionValue{ID: conversionSecond}}, {(*conversionValue)(nil)}} {
		c := qt.New(t)
		runtime := mustRuntime(c, propertyProvider(propertyService{decode: func(context.Context, schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) { return reply, nil }}))
		result, err := runtime.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties,
			Fragments: []schemaext.PropertyFragment{{Kind: conversionFirst}}})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(result, qt.IsNil)
	}
}

func TestPropertyEncoderRejectsInvalidReplies(t *testing.T) {
	for _, reply := range [][]schemaext.PropertyFragment{nil, {{Kind: conversionSecond}}, {{Kind: conversionFirst, Properties: map[string]string{"size": "not owned"}}}} {
		c := qt.New(t)
		runtime := mustRuntime(c, propertyProvider(propertyService{encode: func(context.Context, schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
			return reply, nil
		}}))
		result, err := runtime.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties,
			Values: []schemaext.Value{&conversionValue{ID: conversionFirst}}})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(result, qt.IsNil)
	}
}

func TestProperties_EmptyBatchStillRequiresSelectedServicesAndContext(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, propertyProvider(propertyService{}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		ctx    context.Context
		target string
		format schemaext.PropertyFormat
		want   error
	}{
		{nil, "custom", schemaext.TablePlatformProperties, schemaext.ErrInvalidValue},
		{ctx, "custom", schemaext.TablePlatformProperties, context.Canceled},
		{t.Context(), "absent", schemaext.TablePlatformProperties, ptaherr.ErrUnsupportedDialect},
		{t.Context(), "custom", "unknown.org/format", ptaherr.ErrUnsupportedFeature},
	} {
		decoded, err := runtime.DecodeProperties(test.ctx, schemaext.PropertyDecodeRequest{Target: test.target, Format: test.format})
		c.Assert(err, qt.ErrorIs, test.want)
		c.Assert(decoded, qt.IsNil)
		encoded, err := runtime.EncodeProperties(test.ctx, schemaext.PropertyEncodeRequest{Target: test.target, Format: test.format})
		c.Assert(err, qt.ErrorIs, test.want)
		c.Assert(encoded, qt.IsNil)
	}
}

func TestProperties_ProviderErrorsDiscardPartialResponses(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider failed")
	runtime := mustRuntime(c, propertyProvider(propertyService{
		decode: func(context.Context, schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
			return []schemaext.Value{&conversionValue{ID: conversionFirst}}, failure
		},
		encode: func(context.Context, schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
			return []schemaext.PropertyFragment{{Kind: conversionFirst}}, failure
		},
	}))
	decoded, err := runtime.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties, Fragments: []schemaext.PropertyFragment{{Kind: conversionFirst}}})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(decoded, qt.IsNil)
	encoded, err := runtime.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties, Values: []schemaext.Value{&conversionValue{ID: conversionFirst}}})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(encoded, qt.IsNil)
}

func TestProperties_CancellationDuringServiceDiscardsResults(t *testing.T) {
	c := qt.New(t)
	decodeCtx, cancelDecode := context.WithCancel(t.Context())
	defer cancelDecode()
	encodeCtx, cancelEncode := context.WithCancel(t.Context())
	defer cancelEncode()
	runtime := mustRuntime(c, propertyProvider(propertyService{
		decode: func(context.Context, schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
			cancelDecode()
			return []schemaext.Value{&conversionValue{ID: conversionFirst}}, nil
		},
		encode: func(context.Context, schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
			cancelEncode()
			return []schemaext.PropertyFragment{{Kind: conversionFirst}}, nil
		},
	}))
	decoded, err := runtime.DecodeProperties(decodeCtx, schemaext.PropertyDecodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties, Fragments: []schemaext.PropertyFragment{{Kind: conversionFirst}}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(decoded, qt.IsNil)
	encoded, err := runtime.EncodeProperties(encodeCtx, schemaext.PropertyEncodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties, Values: []schemaext.Value{&conversionValue{ID: conversionFirst}}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(encoded, qt.IsNil)
}

func TestProperties_LaterBatchFailureDiscardsEarlierSuccess(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("second owner failed")
	var calls []string
	first := propertyService{
		decode: func(context.Context, schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
			calls = append(calls, "decode first")
			return []schemaext.Value{&conversionValue{ID: conversionFirst}}, nil
		},
		encode: func(context.Context, schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
			calls = append(calls, "encode first")
			return []schemaext.PropertyFragment{{Kind: conversionFirst}}, nil
		},
	}
	second := propertyService{
		decode: func(context.Context, schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
			calls = append(calls, "decode second")
			return nil, failure
		},
		encode: func(context.Context, schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
			calls = append(calls, "encode second")
			return nil, failure
		},
	}
	provider := propertyProvider(first)
	definitions := provider.Properties[0].Definitions
	provider.Properties[0].Definitions = definitions[:1]
	provider.Properties = append(provider.Properties, engine.PropertySource{Target: "custom", Format: schemaext.TablePlatformProperties, Definitions: definitions[1:], Service: second})
	runtime := mustRuntime(c, provider)
	decoded, err := runtime.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties,
		Fragments: []schemaext.PropertyFragment{{Kind: conversionSecond}, {Kind: conversionFirst}}})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(decoded, qt.IsNil)
	encoded, err := runtime.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: "custom", Format: schemaext.TablePlatformProperties,
		Values: []schemaext.Value{&conversionValue{ID: conversionSecond}, &conversionValue{ID: conversionFirst}}})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(encoded, qt.IsNil)
	c.Assert(calls, qt.DeepEquals, []string{"decode first", "decode second", "encode first", "encode second"})
}
