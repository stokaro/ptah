package schemaext

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ModelCodec describes the codec of one owner model whose wire form is its Go
// value as encoding/json writes it. [ModelCodec.Codec] builds the [Codec].
type ModelCodec[T Value] struct {
	// Prototype is a value of the model's concrete type, as [Codec.Prototype].
	Prototype T
	// Representation is the representation the codec encodes.
	Representation Representation
	// Version and Definition are the wire contract, as in [Codec].
	Version    uint32
	Definition json.RawMessage
	// Shape checks a wire document before it is decoded into T, for what a
	// struct decoder cannot see: a key in another letter case, a null, an
	// omitted value spelled out (see [ObjectShape]). Nil checks nothing beyond
	// [DecodeJSON].
	Shape func(json.RawMessage) error
	// Validate checks the representation's invariants. Nil accepts every value
	// of type T.
	Validate func(T) error
	// Canonical returns the value as it is encoded, without changing its
	// argument; an owner orders sets here. Nil encodes the value as it is.
	Canonical func(T) T
}

// Codec returns the codec m describes. Every boundary validates first:
// cloning, encoding or decoding an invalid value is refused, so the codec
// passes no invalid value on, and the canonical form is the encoding.
//
// Each refusal is an [*InvalidModelError] naming the prototype's kind and
// m.Representation, including a payload of another type and a document the
// shape or the struct decoder refuses. A refusal Validate already typed is
// returned unchanged. Every refusal wraps [ErrInvalidValue].
func (m ModelCodec[T]) Codec() Codec {
	refuse := func(err error) error {
		if typed := (*InvalidModelError)(nil); errors.As(err, &typed) {
			return err
		}
		return &InvalidModelError{Kind: m.Prototype.Kind(), Representation: m.Representation, Message: err.Error()}
	}
	validated := func(payload Payload) (T, error) {
		value, ok := payload.(T)
		if !ok {
			var zero T
			return zero, refuse(fmt.Errorf("%w: expected %T, got %T", ErrInvalidValue, m.Prototype, payload))
		}
		if m.Validate != nil {
			if err := m.Validate(value); err != nil {
				return value, refuse(err)
			}
		}
		return value, nil
	}
	encode := func(payload Payload) (json.RawMessage, error) {
		value, err := validated(payload)
		if err != nil {
			return nil, err
		}
		if m.Canonical != nil {
			value = m.Canonical(value)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, refuse(fmt.Errorf("%w: encode %T: %v", ErrInvalidValue, value, err))
		}
		return encoded, nil
	}
	return Codec{
		Prototype: m.Prototype, Representation: m.Representation, Version: m.Version, Definition: m.Definition,
		Encode: encode, Canonical: encode,
		Clone: func(payload Payload) (Payload, error) {
			value, err := validated(payload)
			if err != nil {
				return nil, err
			}
			return value.Clone(), nil
		},
		Decode: func(data json.RawMessage) (Payload, error) {
			if m.Shape != nil {
				if err := m.Shape(data); err != nil {
					return nil, refuse(err)
				}
			}
			decoded, err := DecodeJSON[T](data)
			if err != nil {
				return nil, refuse(err)
			}
			value, err := validated(decoded)
			if err != nil {
				return nil, err
			}
			return value, nil
		},
	}
}
