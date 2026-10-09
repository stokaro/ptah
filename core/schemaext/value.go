package schemaext

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	// ErrInvalidValue identifies malformed data at a typed payload boundary.
	ErrInvalidValue = errors.New("invalid feature value")
	// ErrDuplicate identifies a second declaration of the same feature identity.
	ErrDuplicate = errors.New("duplicate feature identity")
	// ErrExplicitCodec prevents default JSON from losing concrete payload types.
	ErrExplicitCodec = errors.New("feature data requires an explicit codec registry")
)

// Payload supplies a semantic identity without prescribing a model or an
// operation interface. Codec registrations bind it to a concrete local type.
type Payload interface {
	Kind() Kind
}

// Value is owner-defined schema data. Clone returns an independent snapshot of
// the same kind and concrete type. Equal compares values within this documented
// representation; it does not discover defaults or decide whether to migrate.
// All methods are pure local operations and must not access a database or call
// a remote provider. Target-dependent comparison belongs to a batched service.
type Value interface {
	Payload
	Clone() Value
	Equal(Value) bool
}

// CloneValue validates the interface and its clone's identity. Owners remain
// responsible for copying nested mutable state; their conformance tests must
// prove isolation. Reflection is confined to this dispatch boundary.
func CloneValue(value Value) (Value, error) {
	if err := ValidatePayload(value); err != nil {
		return nil, err
	}
	kind, typeOf := value.Kind(), reflect.TypeOf(value)
	cloned := value.Clone()
	if err := samePayload(kind, typeOf, cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

// ValidatePayload refuses nil, typed-nil, and invalid identities without
// invoking a method on a typed-nil receiver. A payload that implements
// AccessEffectSource must also return a valid assessment. Codec snapshots,
// encoding, decoding, and change and value clones check payloads here, so an
// assessment cannot be lost or left unset at those boundaries.
func ValidatePayload(payload Payload) error {
	if absent(payload) {
		return fmt.Errorf("%w: payload is nil", ErrInvalidValue)
	}
	if !payload.Kind().Valid() {
		return fmt.Errorf("%w: invalid kind %q", ErrInvalidValue, payload.Kind())
	}
	if source, ok := payload.(AccessEffectSource); ok {
		if err := source.AccessEffect().Validate(); err != nil {
			return fmt.Errorf("%q: %w", payload.Kind(), err)
		}
	}
	return nil
}

func samePayload(kind Kind, typeOf reflect.Type, cloned Payload) error {
	if err := ValidatePayload(cloned); err != nil {
		return err
	}
	if typeOf != reflect.TypeOf(cloned) || kind != cloned.Kind() {
		return fmt.Errorf("%w: %q changed type or kind while cloning", ErrInvalidValue, kind)
	}
	return nil
}

func absent(payload any) bool {
	if payload == nil {
		return true
	}
	value := reflect.ValueOf(payload)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// EqualValues compares local representations after validating both snapshots.
// Different kinds or concrete representations are unequal. This is never a
// substitute for the owner's target-aware desired/observed comparison.
func EqualValues(left, right Value) (bool, error) {
	a, err := CloneValue(left)
	if err != nil {
		return false, err
	}
	b, err := CloneValue(right)
	if err != nil {
		return false, err
	}
	if a.Kind() != b.Kind() || reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false, nil
	}
	return a.Equal(b), nil
}
