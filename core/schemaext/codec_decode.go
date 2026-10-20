package schemaext

import (
	"context"
	"fmt"
	"slices"
)

func validDecoder(owned OwnedCodec) bool {
	if owned.BatchDecoder == nil {
		return owned.Codec.Decode != nil
	}
	return owned.Codec.Decode == nil && owned.BatchDecoder.Decode != nil
}

// Decode resolves an ordered batch using exactly the selected model definitions.
// It validates every envelope before invoking owners, then groups external
// models by owner in first-appearance order. Each owner receives one complete
// batch across its kinds and representations. Local codecs remain local.
// Errors, cancellation, and malformed responses return no usable prefix.
// Neither input buffers nor owner-returned payloads alias the returned values.
func (r Registry) Decode(ctx context.Context, envelopes []Envelope) ([]Payload, error) {
	prepared, err := r.prepareDecode(ctx, envelopes)
	if err != nil {
		return nil, err
	}
	result := make([]Payload, len(prepared))
	groups := make(map[string][]int)
	for i, envelope := range prepared {
		if r.batchDecoders[envelope.Owner] != nil {
			groups[envelope.Owner] = append(groups[envelope.Owner], i)
		}
	}
	for i, envelope := range prepared {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if result[i] != nil {
			continue
		}
		if decoder := r.batchDecoders[envelope.Owner]; decoder != nil {
			if err := r.decodeGroup(ctx, prepared, groups[envelope.Owner], decoder, result); err != nil {
				return nil, err
			}
		} else {
			codec := r.codecs[codecKey{kind: envelope.Kind, representation: envelope.Representation}]
			decoded, err := codec.decode(slices.Clone(envelope.Payload))
			if err != nil {
				return nil, err
			}
			result[i], err = decodedSnapshot(codec, envelope.Kind, decoded)
			if err != nil {
				return nil, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r Registry) prepareDecode(ctx context.Context, envelopes []Envelope) ([]Envelope, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	result := make([]Envelope, len(envelopes))
	for i, envelope := range envelopes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		codec, found := r.codecs[codecKey{kind: envelope.Kind, representation: envelope.Representation}]
		if !found {
			return nil, &UnknownCodecError{Kind: envelope.Kind, Representation: envelope.Representation}
		}
		if envelope.Format != envelopeFormat || envelope.Owner != codec.owner || envelope.Version != codec.version || envelope.Definition != codec.definition {
			return nil, fmt.Errorf("%w: %q/%s; decode with the recorded definition or regenerate the artifact", ErrIncompatibleCodec, envelope.Kind, envelope.Representation)
		}
		data, err := CanonicalJSON(envelope.Payload)
		if err != nil {
			return nil, err
		}
		envelope.Payload = data
		result[i] = envelope
	}
	return result, nil
}

func (r Registry) decodeGroup(ctx context.Context, prepared []Envelope, indexes []int, decode func(context.Context, []Envelope) ([]Payload, error), result []Payload) error {
	request := make([]Envelope, len(indexes))
	for i, index := range indexes {
		request[i] = prepared[index]
		request[i].Payload = slices.Clone(request[i].Payload)
	}
	values, err := decode(ctx, request)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(values) != len(indexes) {
		return fmt.Errorf("%w: owner decoder returned an incomplete batch", ErrInvalidValue)
	}
	for i, value := range values {
		index := indexes[i]
		original := prepared[index]
		codec := r.codecs[codecKey{kind: original.Kind, representation: original.Representation}]
		result[index], err = decodedSnapshot(codec, original.Kind, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func decodedSnapshot(codec registeredCodec, kind Kind, decoded Payload) (Payload, error) {
	if err := ValidatePayload(decoded); err != nil {
		return nil, err
	}
	if decoded.Kind() != kind {
		return nil, fmt.Errorf("%w: decoder returned kind %q for %q", ErrInvalidValue, decoded.Kind(), kind)
	}
	return codec.snapshot(decoded)
}
