package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretKind identifies a change to one YDB secret.
const SecretKind schemaext.Kind = "ptah.run/ydb/secret-change" // #nosec G101 -- a kind identifier, not a credential

// Secret captures both operands of a change to one secret. A nil Before means
// the database holds no secret at the path; a nil After means the declaration
// drops it. A nil side is established absence, never an unread secret.
//
// When both sides are present, After.Rotate asks for ALTER SECRET. Without it
// the change keeps the secret and its value as they are, which only a reversal
// produces: a rotation cannot be undone, because the earlier value was never
// read.
type Secret struct {
	Before *ydbsecret.Observed `json:"before"`
	After  *ydbsecret.Desired  `json:"after"`
}

// Kind returns the stable change identity.
func (*Secret) Kind() schemaext.Kind { return SecretKind }

// CloneChange returns independent before and after snapshots.
func (v *Secret) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Secret)(nil)
	}
	cloned := &Secret{}
	if v.Before != nil {
		cloned.Before = &ydbsecret.Observed{}
	}
	if v.After != nil {
		cloned.After = new(*v.After)
	}
	return cloned
}

// Validate refuses a change without operands, an invalid declaration, and a
// rotation of a secret the database does not hold.
func (v *Secret) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a secret change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.After != nil {
		if err := v.After.Validate(); err != nil {
			return err
		}
		if v.Before == nil && v.After.Rotate {
			return fmt.Errorf("%w: a secret the database does not hold is created, not rotated", schemaext.ErrInvalidValue)
		}
	}
	return nil
}

// Rotates reports whether the change gives a secret the database holds a new
// value.
func (v *Secret) Rotates() bool {
	return v != nil && v.Before != nil && v.After != nil && v.After.Rotate
}

// Effect records what the change does to the secret and to every data source
// that names it. Dropping loses a value nothing can read back.
func (v *Secret) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: ydbsecret.CreateReason}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbsecret.DropReason}
	case v.After.Rotate:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbsecret.RotateReason}
	default:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "keeps the secret and the value it holds"}
	}
}

// SecretCodec describes the complete before/after wire for one secret change.
// Neither operand can carry a value.
func SecretCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := secretValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	definition := `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]}}}`
	models := ydbsecret.Codecs()
	return schemaext.Codec{Prototype: &Secret{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(definition, models[1].Definition, models[0].Definition)),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := secretValue(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneChange(), nil
		},
		Decode: decodeSecretChange,
	}
}

func decodeSecretChange(data json.RawMessage) (schemaext.Payload, error) {
	before, after, err := decodeStandaloneOperands[*ydbsecret.Observed, *ydbsecret.Desired](data, "secret", ydbsecret.Codecs())
	if err != nil {
		return nil, err
	}
	value := &Secret{Before: before, After: after}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

func secretValue(payload schemaext.Payload) (*Secret, error) {
	value, ok := payload.(*Secret)
	if !ok {
		return nil, fmt.Errorf("%w: expected a secret change, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}
