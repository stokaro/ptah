package schemaext

import (
	"context"
	"fmt"
)

// SnapshotPayloads validates and clones a complete batch through selected local
// codecs. It supports operation payloads as well as schema and change values.
// No encoding or semantic service runs, and errors return no partial snapshot.
func (r Registry) SnapshotPayloads(ctx context.Context, representation Representation, payloads []Payload) ([]Payload, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	if !representation.valid() {
		return nil, fmt.Errorf("%w: unknown representation %q", ErrInvalidCodec, representation)
	}
	result := make([]Payload, 0, len(payloads))
	for _, payload := range payloads {
		if err := codecContext(ctx); err != nil {
			return nil, err
		}
		codec, err := r.codecFor(representation, payload)
		if err != nil {
			return nil, err
		}
		cloned, err := codec.snapshot(payload)
		if err != nil {
			return nil, err
		}
		result = append(result, cloned)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r Registry) codecFor(representation Representation, payload Payload) (registeredCodec, error) {
	if err := ValidatePayload(payload); err != nil {
		return registeredCodec{}, err
	}
	codec, found := r.codecs[codecKey{kind: payload.Kind(), representation: representation}]
	if !found {
		return registeredCodec{}, &UnknownCodecError{Kind: payload.Kind(), Representation: representation}
	}
	return codec, nil
}
