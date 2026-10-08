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

// ChangeRecord associates a self-contained change with an individual subject.
// A table-owned subject occurs in its table's changes, not also at database scope.
type ChangeRecord struct {
	Subject objectidentity.ID
	Value   ChangeValue
}

// Clone returns an independent change payload and preserves structured identity.
// Nil payloads and clones that change their kind or concrete type are refused.
func (c ChangeRecord) Clone() (ChangeRecord, error) {
	if c.Subject.Kind == "" || c.Subject.Name.Source == "" || c.Subject.Name.Normalized == "" {
		return ChangeRecord{}, fmt.Errorf("%w: a change requires a structured subject", ErrInvalidValue)
	}
	if err := ValidatePayload(c.Value); err != nil {
		return ChangeRecord{}, err
	}
	kind, concrete := c.Value.Kind(), reflect.TypeOf(c.Value)
	cloned := c.Value.CloneChange()
	if err := samePayload(kind, concrete, cloned); err != nil {
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
			return nil, fmt.Errorf("%w: change %q", ErrUnknownCodec, cloned.Value.Kind())
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
func (ChangeRecord) MarshalJSON() ([]byte, error) { return nil, ErrExplicitCodec }

// UnmarshalJSON requires a registry that understands the recorded change codec.
func (*ChangeRecord) UnmarshalJSON([]byte) error { return ErrExplicitCodec }
