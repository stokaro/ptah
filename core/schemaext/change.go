package schemaext

import (
	"context"
	"fmt"
	"reflect"

	"ptah.run/core/objectidentity"
)

// ChangeValue is an owner-computed directional change. Its clone is a pure
// local snapshot. Planning and reversal belong to contextual batch services;
// copying a payload never claims that lost data can be recovered.
type ChangeValue interface {
	Payload
	CloneChange() ChangeValue
}

// OwnerReplacement is implemented by a change value whose owner cannot apply it
// while the common object the value is attached to survives. A host that
// receives a change reporting true must replace that common object, dropping
// and recreating it with its effective declaration, or refuse the change; it
// must never plan the change as if the object could keep its state. A value
// that does not implement the interface, or reports false, is applied in place
// by its owner. The answer belongs to the change itself, so it survives
// serialization and holds in the reverse direction as well.
type OwnerReplacement interface {
	ChangeValue
	// ReplacesOwner reports whether applying the change requires replacing the
	// common object it is attached to.
	ReplacesOwner() bool
}

// ReplacesOwner reports whether value is an [OwnerReplacement] that requires
// replacing its common object. Any other value, including nil, does not.
func ReplacesOwner(value ChangeValue) bool {
	replacement, ok := value.(OwnerReplacement)
	return ok && replacement.ReplacesOwner()
}

// ChangeRecord associates a self-contained change with an individual subject.
// A table-owned subject occurs in its table's changes, not also at database scope.
type ChangeRecord struct {
	Subject objectidentity.ID
	Value   ChangeValue
}

// Clone returns an independent change payload and preserves structured identity.
// Nil payloads, clones that change their kind or concrete type, and a payload
// or clone without the access assessment it declares are refused.
func (c ChangeRecord) Clone() (ChangeRecord, error) {
	if err := validChangeSubject(c.Subject); err != nil {
		return ChangeRecord{}, err
	}
	if err := ValidatePayload(c.Value); err != nil {
		return ChangeRecord{}, err
	}
	kind, concrete := c.Value.Kind(), reflect.TypeOf(c.Value)
	cloned := c.Value.CloneChange()
	if err := samePayload(kind, concrete, cloned); err != nil {
		return ChangeRecord{}, err
	}
	// The clone is what the caller receives: an input without its declared
	// assessment yields a clone without it, and so does a defective clone.
	if err := validateAccess(cloned); err != nil {
		return ChangeRecord{}, err
	}
	return ChangeRecord{Subject: c.Subject, Value: cloned}, nil
}

// SnapshotChanges validates registered change representations and captures each
// payload through its local codec. It does not call a planning service.
func (r Registry) SnapshotChanges(ctx context.Context, changes []ChangeRecord) ([]ChangeRecord, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	result := make([]ChangeRecord, 0, len(changes))
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cloned, err := change.Clone()
		if err != nil {
			return nil, err
		}
		codec, found := r.codecs[codecKey{kind: cloned.Value.Kind(), representation: Change}]
		if !found {
			return nil, &UnknownCodecError{Kind: cloned.Value.Kind(), Representation: Change}
		}
		payload, err := codec.snapshot(cloned.Value)
		if err != nil {
			return nil, err
		}
		value, ok := payload.(ChangeValue)
		if !ok {
			return nil, fmt.Errorf("%w: change codec returned %T", ErrInvalidValue, payload)
		}
		result = append(result, ChangeRecord{Subject: change.Subject, Value: value})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// MarshalJSON refuses a summary-only or implicit interface encoding of a change.
// [Registry.EncodeChanges] writes the explicit form.
func (ChangeRecord) MarshalJSON() ([]byte, error) { return nil, ErrExplicitCodec }

// UnmarshalJSON requires a registry that understands the recorded change codec.
func (*ChangeRecord) UnmarshalJSON([]byte) error { return ErrExplicitCodec }
