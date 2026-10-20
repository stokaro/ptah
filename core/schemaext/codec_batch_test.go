package schemaext_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func batchWidget(kind schemaext.Kind, representation schemaext.Representation, decoder *schemaext.BatchDecoder) schemaext.OwnedCodec {
	codec := widgetCodec(kind, representation)
	codec.Decode = nil
	return schemaext.OwnedCodec{Owner: "example.org/provider", Codec: codec, BatchDecoder: decoder}
}

func TestRegistryResolvesOneOwnerBatchAcrossModels(t *testing.T) {
	c := qt.New(t)
	calls := 0
	var returned []*widget
	decoder := &schemaext.BatchDecoder{Decode: func(_ context.Context, envelopes []schemaext.Envelope) ([]schemaext.Payload, error) {
		calls++
		result := make([]schemaext.Payload, len(envelopes))
		for i, envelope := range envelopes {
			wire, err := schemaext.DecodeJSON[widgetWire](envelope.Payload)
			c.Assert(err, qt.IsNil)
			value := &widget{ID: envelope.Kind, Count: wire.Count}
			returned = append(returned, value)
			result[i] = value
		}
		envelopes[0].Kind = "example.org/mutated"
		envelopes[0].Payload[0] = '!'
		return result, nil
	}}
	registry, err := schemaext.NewRegistry(batchWidget(widgetKind, schemaext.Desired, decoder), batchWidget(otherKind, schemaext.Observed, decoder))
	c.Assert(err, qt.IsNil)
	first, err := registry.Encode(c.Context(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind, Count: 1}, &widget{ID: widgetKind, Count: 3}})
	c.Assert(err, qt.IsNil)
	second, err := registry.Encode(c.Context(), schemaext.Observed, []schemaext.Payload{&widget{ID: otherKind, Count: 2}})
	c.Assert(err, qt.IsNil)
	input := []schemaext.Envelope{first[0], second[0], first[1]}
	frozen, err := json.Marshal(input)
	c.Assert(err, qt.IsNil)
	output, err := registry.Decode(c.Context(), input)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(output, qt.HasLen, 3)
	c.Assert(output[0].(*widget).Count, qt.Equals, uint64(1))
	c.Assert(output[1].(*widget).Count, qt.Equals, uint64(2))
	c.Assert(output[2].(*widget).Count, qt.Equals, uint64(3))
	after, err := json.Marshal(input)
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.DeepEquals, frozen)
	returned[0].Count = 99
	c.Assert(output[0].(*widget).Count, qt.Equals, uint64(1))
	// The registry freezes the callback, rather than retaining this mutable record.
	decoder.Decode = nil
	_, err = registry.Decode(c.Context(), input)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 2)
	_, err = registry.SnapshotPayloads(c.Context(), schemaext.Desired, []schemaext.Payload{output[0]})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 2)
}

func TestRegistryValidatesWholeBatchBeforeOwnerDispatch(t *testing.T) {
	c := qt.New(t)
	calls := 0
	decoder := &schemaext.BatchDecoder{Decode: func(context.Context, []schemaext.Envelope) ([]schemaext.Payload, error) { calls++; return nil, nil }}
	registry, err := schemaext.NewRegistry(batchWidget(widgetKind, schemaext.Desired, decoder))
	c.Assert(err, qt.IsNil)
	inputs, err := registry.Encode(c.Context(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}, &widget{ID: widgetKind}})
	c.Assert(err, qt.IsNil)
	inputs[1].Definition = "foreign"
	result, err := registry.Decode(c.Context(), inputs)
	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
	c.Assert(result, qt.IsNil)
	c.Assert(calls, qt.Equals, 0)
	inputs[1].Definition = inputs[0].Definition
	inputs[1].Payload = json.RawMessage(`{"x":1,"x":2}`)
	result, err = registry.Decode(c.Context(), inputs)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
	c.Assert(calls, qt.Equals, 0)
}

func TestRegistryRejectsMalformedOwnerBatches(t *testing.T) {
	for _, values := range [][]schemaext.Payload{
		nil, {nil}, {&widget{ID: otherKind}}, {&otherWidget{ID: widgetKind}},
		{&widget{ID: widgetKind}, &widget{ID: widgetKind}},
	} {
		t.Run("malformed", func(t *testing.T) {
			c := qt.New(t)
			decoder := &schemaext.BatchDecoder{Decode: func(context.Context, []schemaext.Envelope) ([]schemaext.Payload, error) { return values, nil }}
			registry, err := schemaext.NewRegistry(batchWidget(widgetKind, schemaext.Desired, decoder))
			c.Assert(err, qt.IsNil)
			input, err := registry.Encode(c.Context(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}})
			c.Assert(err, qt.IsNil)
			result, err := registry.Decode(c.Context(), input)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestRegistryDiscardsOwnerPrefixOnFailureOrCancellation(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("owner refused")
	decoder := &schemaext.BatchDecoder{Decode: func(context.Context, []schemaext.Envelope) ([]schemaext.Payload, error) {
		return []schemaext.Payload{&widget{ID: widgetKind}}, failure
	}}
	registry, err := schemaext.NewRegistry(batchWidget(widgetKind, schemaext.Desired, decoder))
	c.Assert(err, qt.IsNil)
	input, err := registry.Encode(c.Context(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}})
	c.Assert(err, qt.IsNil)
	result, err := registry.Decode(c.Context(), input)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.IsNil)
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	decoder.Decode = func(context.Context, []schemaext.Envelope) ([]schemaext.Payload, error) {
		cancel()
		return []schemaext.Payload{&widget{ID: widgetKind}}, nil
	}
	registry, err = schemaext.NewRegistry(batchWidget(widgetKind, schemaext.Desired, decoder))
	c.Assert(err, qt.IsNil)
	result, err = registry.Decode(ctx, input)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}

func TestRegistryRefusesConflictingDecoderRegistration(t *testing.T) {
	c := qt.New(t)
	decoder := &schemaext.BatchDecoder{Decode: func(context.Context, []schemaext.Envelope) ([]schemaext.Payload, error) { return nil, nil }}
	first := batchWidget(widgetKind, schemaext.Desired, decoder)
	first.Codec.Decode = widgetCodec(widgetKind, schemaext.Desired).Decode
	_, err := schemaext.NewRegistry(first)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
	first = batchWidget(widgetKind, schemaext.Desired, &schemaext.BatchDecoder{})
	_, err = schemaext.NewRegistry(first)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
	first = batchWidget(widgetKind, schemaext.Desired, decoder)
	second := batchWidget(otherKind, schemaext.Observed, &schemaext.BatchDecoder{Decode: decoder.Decode})
	_, err = schemaext.NewRegistry(first, second)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
	second.BatchDecoder = nil
	second.Codec.Decode = widgetCodec(otherKind, schemaext.Observed).Decode
	_, err = schemaext.NewRegistry(first, second)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
}
