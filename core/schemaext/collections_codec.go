package schemaext

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
)

// EncodeFacets serializes snapshots in deterministic kind order.
func (r Registry) EncodeFacets(ctx context.Context, representation Representation, facets Facets) ([]Envelope, error) {
	if err := schemaRepresentation(representation); err != nil {
		return nil, err
	}
	values, err := facets.Values()
	if err != nil {
		return nil, err
	}
	payloads := make([]Payload, len(values))
	for i, value := range values {
		payloads[i] = value
	}
	return r.Encode(ctx, representation, payloads)
}

// DecodeFacets reconstructs an immutable collection, refusing non-value payloads
// and duplicate kinds even if the duplicate payload bytes are identical.
func (r Registry) DecodeFacets(ctx context.Context, representation Representation, envelopes []Envelope) (Facets, error) {
	if err := requireRepresentations(representation, envelopes); err != nil {
		return Facets{}, err
	}
	payloads, err := r.Decode(ctx, envelopes)
	if err != nil {
		return Facets{}, err
	}
	values := make([]Value, len(payloads))
	for i, payload := range payloads {
		value, ok := payload.(Value)
		if !ok {
			return Facets{}, fmt.Errorf("%w: %q is not a schema value", ErrInvalidValue, payload.Kind())
		}
		values[i] = value
	}
	return NewFacets(values...)
}

// EncodedObject retains structured source and comparison identity beside its
// explicitly encoded value. It contains no interface requiring inferred types.
type EncodedObject struct {
	Ref   objectidentity.ID `json:"ref"`
	Value Envelope          `json:"value"`
}

// EncodeObjects serializes individual objects in structured identity order.
func (r Registry) EncodeObjects(ctx context.Context, representation Representation, objects Objects) ([]EncodedObject, error) {
	if err := schemaRepresentation(representation); err != nil {
		return nil, err
	}
	values, err := objects.All()
	if err != nil {
		return nil, err
	}
	payloads := make([]Payload, len(values))
	for i, object := range values {
		payloads[i] = object.Value
	}
	envelopes, err := r.Encode(ctx, representation, payloads)
	if err != nil {
		return nil, err
	}
	result := make([]EncodedObject, len(values))
	for i, object := range values {
		result[i] = EncodedObject{Ref: object.Ref, Value: envelopes[i]}
	}
	return result, nil
}

// DecodeObjects restores individual identities and rejects collisions and
// value kinds that disagree with their object envelopes.
func (r Registry) DecodeObjects(ctx context.Context, representation Representation, encoded []EncodedObject) (Objects, error) {
	envelopes := make([]Envelope, len(encoded))
	for i, object := range encoded {
		envelopes[i] = object.Value
	}
	if err := requireRepresentations(representation, envelopes); err != nil {
		return Objects{}, err
	}
	payloads, err := r.Decode(ctx, envelopes)
	if err != nil {
		return Objects{}, err
	}
	objects := make([]Object, len(payloads))
	for i, payload := range payloads {
		value, ok := payload.(Value)
		if !ok {
			return Objects{}, fmt.Errorf("%w: %q is not a schema value", ErrInvalidValue, payload.Kind())
		}
		objects[i] = Object{Ref: encoded[i].Ref, Value: value}
	}
	return NewObjects(objects...)
}

func requireRepresentations(representation Representation, envelopes []Envelope) error {
	if err := schemaRepresentation(representation); err != nil {
		return err
	}
	for _, envelope := range envelopes {
		if envelope.Representation != representation {
			return fmt.Errorf("%w: collection mixes desired and observed representations", ErrInvalidValue)
		}
	}
	return nil
}
