package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// DefaultPoolSettingsKind identifies a declaration that sets named limits on
// YDB's existing default pool without claiming knowledge of its current limits.
const DefaultPoolSettingsKind schemaext.Kind = "ptah.run/ydb/default-pool-settings-operation"

// DefaultPoolSettings sets only the declared limits on the default pool. Unset
// settings are preserved, never reset. It carries no observed before operand.
// The zero value is a valid no-op. Full state transitions use ResourcePool.
type DefaultPoolSettings struct {
	Spec ydbworkload.PoolSpec `json:"spec"`
}

// Kind returns the identity of the declaration operation.
func (*DefaultPoolSettings) Kind() schemaext.Kind { return DefaultPoolSettingsKind }

// Subject identifies the database's existing fallback pool.
func (*DefaultPoolSettings) Subject() objectidentity.ID {
	return ydbworkload.PoolRef(ydbworkload.DefaultPool)
}

// CloneExtension copies every configured setting. Nil remains typed nil.
func (v *DefaultPoolSettings) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*DefaultPoolSettings)(nil)
	}
	return &DefaultPoolSettings{Spec: v.Spec.Clone()}
}

// Validate checks allowed default-pool limits. A nil receiver or an invalid
// setting wraps schemaext.ErrInvalidValue; no target capability is granted.
func (v *DefaultPoolSettings) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: default pool settings are nil", schemaext.ErrInvalidValue)
	}
	return ydbworkload.ValidatePoolRef(v.Subject(), v.Spec)
}

// Effect reports workload impact without claiming that existing limits are known.
// Invalid settings retain unknown effects.
func (v *DefaultPoolSettings) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "sets declared limits on the existing default pool"}
}

// DefaultPoolSettingsCodec describes a set-only request without a before state.
// Its exact version-one wire accepts only spec, preserving absent settings.
func DefaultPoolSettingsCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := defaultPoolSettings(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return schemaext.Codec{Prototype: &DefaultPoolSettings{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["spec"],"additionalProperties":false,"properties":{"spec":%s}}`, ydbworkload.PoolSpecSchema())),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := defaultPoolSettings(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := workloadFields(data, []string{"spec"}, []string{"spec"})
			if err != nil {
				return nil, err
			}
			spec, err := ydbworkload.DecodePoolSpec(fields["spec"])
			if err != nil {
				return nil, err
			}
			return defaultPoolSettings(&DefaultPoolSettings{Spec: spec})
		},
	}
}

func defaultPoolSettings(payload schemaext.Payload) (*DefaultPoolSettings, error) {
	value, ok := payload.(*DefaultPoolSettings)
	if !ok {
		return nil, fmt.Errorf("%w: unexpected default pool settings %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, &schemaext.InvalidModelError{Kind: DefaultPoolSettingsKind, Representation: schemaext.Operation, Message: err.Error()}
	}
	return value, nil
}
