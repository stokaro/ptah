package engine

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
)

// DecodeProperties validates ownership of every source fragment before calling
// any service, then dispatches one ordered batch per owner and restores input
// order. Model codecs validate and isolate returned desired values. Errors and
// cancellation discard all results; fragments confer no inspection coverage.
func (r *Runtime) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	kinds := make([]schemaext.Kind, len(request.Fragments))
	for i, fragment := range request.Fragments {
		kinds[i] = fragment.Kind
	}
	target, batches, err := r.propertyBatches(ctx, request.Target, request.Format, kinds)
	if err != nil {
		return nil, err
	}
	fragments := make([]schemaext.PropertyFragment, len(request.Fragments))
	for i, fragment := range request.Fragments {
		fragments[i], err = r.snapshotPropertyFragment(target, request.Format, fragment)
		if err != nil {
			return nil, err
		}
	}
	result := make([]schemaext.Value, len(fragments))
	for service, indices := range batches {
		if len(indices) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := schemaext.PropertyDecodeRequest{Target: target, Format: request.Format, Fragments: make([]schemaext.PropertyFragment, len(indices))}
		for i, index := range indices {
			batch.Fragments[i] = fragments[index]
		}
		decoded, err := r.propertyServices[service].Service.DecodeProperties(ctx, batch)
		if err != nil {
			return nil, err
		}
		if len(decoded) != len(indices) {
			return nil, fmt.Errorf("%w: property decoder changed the value count", schemaext.ErrInvalidValue)
		}
		decoded, err = r.codecs.SnapshotValues(ctx, schemaext.Desired, decoded)
		if err != nil {
			return nil, err
		}
		for i, index := range indices {
			if decoded[i].Kind() != kinds[index] {
				return nil, fmt.Errorf("%w: property decoder changed an ordered kind", schemaext.ErrInvalidValue)
			}
			result[index] = decoded[i]
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// EncodeProperties snapshots desired input, dispatches one batch per owner, and
// verifies response count, ordered kinds, and property-key ownership. Returned
// maps are independent of provider buffers. Any error or cancellation returns
// no partial output. An unsupported format is an error even for an empty batch.
func (r *Runtime) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := propertyContext(ctx); err != nil {
		return nil, err
	}
	values, err := r.Codecs().SnapshotValues(ctx, schemaext.Desired, request.Values)
	if err != nil {
		return nil, err
	}
	kinds := make([]schemaext.Kind, len(values))
	for i, value := range values {
		kinds[i] = value.Kind()
	}
	target, batches, err := r.propertyBatches(ctx, request.Target, request.Format, kinds)
	if err != nil {
		return nil, err
	}
	result := make([]schemaext.PropertyFragment, len(values))
	for service, indices := range batches {
		if len(indices) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := schemaext.PropertyEncodeRequest{Target: target, Format: request.Format, Values: make([]schemaext.Value, len(indices))}
		for i, index := range indices {
			batch.Values[i] = values[index]
		}
		encoded, err := r.propertyServices[service].Service.EncodeProperties(ctx, batch)
		if err != nil {
			return nil, err
		}
		if len(encoded) != len(indices) {
			return nil, fmt.Errorf("%w: property encoder changed the fragment count", schemaext.ErrInvalidValue)
		}
		for i, index := range indices {
			if encoded[i].Kind != kinds[index] {
				return nil, fmt.Errorf("%w: property encoder changed an ordered kind", schemaext.ErrInvalidValue)
			}
			fragment, err := r.snapshotPropertyFragment(target, request.Format, encoded[i])
			if err != nil {
				return nil, err
			}
			result[index] = fragment
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
