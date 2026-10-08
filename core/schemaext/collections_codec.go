package schemaext

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
)

// EncodedFacet carries source selection separately from the owner's payload.
// A nil Value requires a nonempty Targets binding and records deliberate
// exclusion. It is never evidence of absence or a request for target defaults.
type EncodedFacet struct {
	Kind    Kind      `json:"kind"`
	Targets []string  `json:"targets,omitempty"`
	Value   *Envelope `json:"value,omitempty"`
}

// EncodeFacets serializes concrete values and scoped exclusions in kind order.
// Excluded values require no model codec; their payload is no longer captured.
func (r Registry) EncodeFacets(ctx context.Context, representation Representation, facets Facets) ([]EncodedFacet, error) {
	if err := schemaRepresentation(representation); err != nil {
		return nil, err
	}
	result := make([]EncodedFacet, 0, len(facets.DeclaredKinds()))
	for _, kind := range facets.DeclaredKinds() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record := EncodedFacet{Kind: kind, Targets: facets.TargetScope(kind)}
		value, found, err := facets.Get(kind)
		if err != nil {
			return nil, err
		}
		if found {
			encoded, err := r.Encode(ctx, representation, []Payload{value})
			if err != nil {
				return nil, err
			}
			record.Value = &encoded[0]
		}
		result = append(result, record)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// DecodeFacets reconstructs source bindings and immutable values. An exclusion
// is retained without looking up a model codec, then ForTarget checks whether
// it applies to the selected target before an operational caller consumes it.
func (r Registry) DecodeFacets(ctx context.Context, representation Representation, records []EncodedFacet) (Facets, error) {
	if err := schemaRepresentation(representation); err != nil {
		return Facets{}, err
	}
	result := Facets{values: make(map[Kind]Value), scopes: make(map[Kind][]string)}
	// Validate the entire host envelope before dispatching any owner codec.
	seen := make(map[Kind]bool)
	var envelopes []Envelope
	for _, record := range records {
		if !record.Kind.Valid() {
			return Facets{}, fmt.Errorf("%w: invalid encoded facet %q", ErrInvalidValue, record.Kind)
		}
		if seen[record.Kind] {
			return Facets{}, fmt.Errorf("%w: encoded facet %q", ErrDuplicate, record.Kind)
		}
		seen[record.Kind] = true
		scope, err := normalizeFacetScope(record.Targets)
		if err != nil {
			return Facets{}, err
		}
		if len(scope) != 0 {
			result.scopes[record.Kind] = scope
		}
		if record.Value == nil {
			if len(scope) == 0 {
				return Facets{}, fmt.Errorf("%w: excluded facet requires a target binding", ErrInvalidValue)
			}
			continue
		}
		if record.Value.Kind != record.Kind {
			return Facets{}, fmt.Errorf("%w: facet envelope disagrees with its model kind", ErrInvalidValue)
		}
		envelopes = append(envelopes, *record.Value)
	}
	if err := requireRepresentations(representation, envelopes); err != nil {
		return Facets{}, err
	}
	payloads, err := r.Decode(ctx, envelopes)
	if err != nil {
		return Facets{}, err
	}
	for _, payload := range payloads {
		value, ok := payload.(Value)
		if !ok {
			return Facets{}, fmt.Errorf("%w: %q is not a schema value", ErrInvalidValue, payload.Kind())
		}
		value, err = CloneValue(value)
		if err != nil {
			return Facets{}, err
		}
		result.values[value.Kind()] = value
	}
	if err := ctx.Err(); err != nil {
		return Facets{}, err
	}
	return result, nil
}

// SnapshotFacets captures each concrete value through its selected codec while
// retaining source target bindings and deliberate exclusions.
func (r Registry) SnapshotFacets(ctx context.Context, representation Representation, facets Facets) (Facets, error) {
	if err := schemaRepresentation(representation); err != nil {
		return Facets{}, err
	}
	values, err := facets.Values()
	if err != nil {
		return Facets{}, err
	}
	values, err = r.SnapshotValues(ctx, representation, values)
	if err != nil {
		return Facets{}, err
	}
	result := facets
	for _, value := range values {
		result, err = result.Replace(value)
		if err != nil {
			return Facets{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return Facets{}, err
	}
	return result, nil
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
