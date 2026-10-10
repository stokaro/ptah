package pgpolicy

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
)

const (
	// PolicyChangeKind identifies a change to one policy.
	PolicyChangeKind schemaext.Kind = "ptah.run/pgpolicy/policy-change"
	// TableStateChangeKind identifies a change to a surviving table's
	// row-security switches.
	TableStateChangeKind schemaext.Kind = "ptah.run/pgpolicy/table-state-change"
)

// PolicyChange records how one policy differs. A nil Before creates it, a nil
// After drops it, and both together change it. Both describe established
// state, never an unread one. Access is the owner's assessment, computed while
// it held both operands and stored as data. CommentOnly records the
// comparison's finding that the policies differ in their comment alone, which
// the operands cannot show: a declared expression and the catalog's spelling
// of it differ as text when they mean the same.
type PolicyChange struct {
	Before      *ObservedPolicy        `json:"before"`
	After       *DesiredPolicy         `json:"after"`
	CommentOnly bool                   `json:"comment_only"`
	Access      schemaext.AccessEffect `json:"access"`
}

// Kind returns the stable change identity.
func (*PolicyChange) Kind() schemaext.Kind { return PolicyChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *PolicyChange) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*PolicyChange)(nil)
	}
	return v.Copy()
}

// Copy is [PolicyChange.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *PolicyChange) Copy() *PolicyChange {
	if v == nil {
		return nil
	}
	return &PolicyChange{Before: v.Before.Copy(), After: v.After.Copy(), CommentOnly: v.CommentOnly, Access: v.Access}
}

// AccessEffect returns the recorded assessment.
func (v *PolicyChange) AccessEffect() schemaext.AccessEffect {
	if v == nil {
		return schemaext.AccessEffect{}
	}
	return v.Access
}

// Validate refuses missing or invalid operands, a comment-only change without
// both operands, and an invalid assessment.
func (v *PolicyChange) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a policy change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.CommentOnly && (v.Before == nil || v.After == nil) {
		return fmt.Errorf("%w: a comment-only policy change requires both operands", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ValidateObservedPolicy(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := ValidateDesiredPolicy(v.After); err != nil {
			return err
		}
	}
	return v.Access.Validate()
}

// Effect records what the change does to the policy object. Who may see or
// write which rows is the access assessment's, not this.
func (v *PolicyChange) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a row-security policy"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "drops a row-security policy; a rollback recreates it from the captured definition"}
	case v.CommentOnly:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "changes only the comment of a row-security policy"}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes a row-security policy"}
	}
}

// TableStateChange records how a surviving table's row-security switches
// differ. Both operands are established: a table whose switches are both off
// carries that state, not an absence. A table created or dropped by the plan
// carries its switches with the table and never appears here.
type TableStateChange struct {
	Before *ObservedTableState    `json:"before"`
	After  *DesiredTableState     `json:"after"`
	Access schemaext.AccessEffect `json:"access"`
}

// Kind returns the stable change identity.
func (*TableStateChange) Kind() schemaext.Kind { return TableStateChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *TableStateChange) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*TableStateChange)(nil)
	}
	return v.Copy()
}

// Copy is [TableStateChange.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *TableStateChange) Copy() *TableStateChange {
	if v == nil {
		return nil
	}
	cloned := &TableStateChange{Access: v.Access}
	if v.Before != nil {
		cloned.Before = new(*v.Before)
	}
	if v.After != nil {
		cloned.After = new(*v.After)
	}
	return cloned
}

// AccessEffect returns the recorded assessment.
func (v *TableStateChange) AccessEffect() schemaext.AccessEffect {
	if v == nil {
		return schemaext.AccessEffect{}
	}
	return v.Access
}

// Validate refuses a missing operand, a pair whose switches agree, and an
// invalid assessment.
func (v *TableStateChange) Validate() error {
	if v == nil || v.Before == nil || v.After == nil {
		return fmt.Errorf("%w: a row-security table change requires both operands", schemaext.ErrInvalidValue)
	}
	if err := ValidateObservedTableState(v.Before); err != nil {
		return err
	}
	if err := ValidateDesiredTableState(v.After); err != nil {
		return err
	}
	if v.Before.Enabled == v.After.Enabled && v.Before.Forced == v.After.Forced {
		return fmt.Errorf("%w: row-security table operands describe the same switches", schemaext.ErrInvalidValue)
	}
	return v.Access.Validate()
}

// Effect records what the change does to the table. Who may see or write which
// rows is the access assessment's, not this.
func (v *TableStateChange) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes whether the table's policies apply"}
}

// ChangeCodecs returns the version-one change codecs.
func ChangeCodecs() []schemaext.Codec {
	return []schemaext.Codec{PolicyChangeCodec(), TableStateChangeCodec()}
}

// PolicyChangeCodec describes the complete wire of a policy change: both
// operands, the comment-only finding and the access assessment.
func PolicyChangeCodec() schemaext.Codec {
	desired, observed := PolicyCodecs()[0], PolicyCodecs()[1]
	return changeCodec(&PolicyChange{}, desired, observed, "policy", `,"comment_only":{"type":"boolean"}`, []string{"comment_only"},
		func(before, after schemaext.Value, access schemaext.AccessEffect, fields map[string]json.RawMessage) (schemaext.ChangeValue, error) {
			value := &PolicyChange{Access: access}
			var ok bool
			if value.Before, ok = operand[*ObservedPolicy](before); !ok {
				return nil, fmt.Errorf("%w: unexpected policy operand %T", schemaext.ErrInvalidValue, before)
			}
			if value.After, ok = operand[*DesiredPolicy](after); !ok {
				return nil, fmt.Errorf("%w: unexpected policy operand %T", schemaext.ErrInvalidValue, after)
			}
			commentOnly, err := schemaext.DecodeJSON[bool](fields["comment_only"])
			if err != nil {
				return nil, err
			}
			value.CommentOnly = commentOnly
			return value, value.Validate()
		}, func(payload schemaext.Payload) (json.RawMessage, error) {
			value, ok := payload.(*PolicyChange)
			if !ok {
				return nil, fmt.Errorf("%w: expected a policy change, got %T", schemaext.ErrInvalidValue, payload)
			}
			if err := value.Validate(); err != nil {
				return nil, err
			}
			canonical := value.Copy()
			if canonical.Before != nil {
				canonical.Before.Roles = sortedRoles(canonical.Before.Roles)
			}
			if canonical.After != nil {
				canonical.After.Roles = sortedRoles(canonical.After.Roles)
			}
			return json.Marshal(canonical)
		})
}

// TableStateChangeCodec describes the complete wire of a table-state change.
func TableStateChangeCodec() schemaext.Codec {
	desired, observed := TableStateCodecs()[0], TableStateCodecs()[1]
	return changeCodec(&TableStateChange{}, desired, observed, "table state", "", nil,
		func(before, after schemaext.Value, access schemaext.AccessEffect, _ map[string]json.RawMessage) (schemaext.ChangeValue, error) {
			value := &TableStateChange{Access: access}
			var ok bool
			if value.Before, ok = operand[*ObservedTableState](before); !ok {
				return nil, fmt.Errorf("%w: unexpected table state operand %T", schemaext.ErrInvalidValue, before)
			}
			if value.After, ok = operand[*DesiredTableState](after); !ok {
				return nil, fmt.Errorf("%w: unexpected table state operand %T", schemaext.ErrInvalidValue, after)
			}
			return value, value.Validate()
		}, func(payload schemaext.Payload) (json.RawMessage, error) {
			value, ok := payload.(*TableStateChange)
			if !ok {
				return nil, fmt.Errorf("%w: expected a row-security table change, got %T", schemaext.ErrInvalidValue, payload)
			}
			if err := value.Validate(); err != nil {
				return nil, err
			}
			return json.Marshal(value)
		})
}

// changeCodec builds a change codec whose wire holds the two operands, the
// access assessment and the family's extra keys, all required.
func changeCodec(prototype schemaext.ChangeValue, desired, observed schemaext.Codec, family, extraDefinition string, extraKeys []string,
	build func(before, after schemaext.Value, access schemaext.AccessEffect, fields map[string]json.RawMessage) (schemaext.ChangeValue, error),
	encode func(schemaext.Payload) (json.RawMessage, error),
) schemaext.Codec {
	keys := append([]string{"before", "after", "access"}, extraKeys...)
	required := `"` + strings.Join(keys, `","`) + `"`
	definition := fmt.Sprintf(`{"type":"object","required":[%s],"additionalProperties":false,"properties":{`+
		`"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]},"access":%s%s}}`,
		required, observed.Definition, desired.Definition, schemaext.AccessEffectSchema(), extraDefinition)
	return schemaext.Codec{
		Prototype: prototype, Representation: schemaext.Change, Version: 1, Definition: json.RawMessage(definition),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if _, err := encode(payload); err != nil {
				return nil, err
			}
			return payload.(schemaext.ChangeValue).CloneChange(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: family + " change",
				Allowed: keys, Required: keys, Nullable: []string{"before", "after"}})
			if err != nil {
				return nil, err
			}
			before, err := decodeOperand(observed, fields["before"])
			if err != nil {
				return nil, err
			}
			after, err := decodeOperand(desired, fields["after"])
			if err != nil {
				return nil, err
			}
			var access schemaext.AccessEffect
			if err := access.UnmarshalJSON(fields["access"]); err != nil {
				return nil, err
			}
			value, err := build(before, after, access, fields)
			if err != nil {
				return nil, err
			}
			return value, nil
		},
	}
}

// operand converts a decoded side to its model type. An absent side is nil
// and acceptable; a value of another type is not.
func operand[T schemaext.Value](value schemaext.Value) (T, bool) {
	var zero T
	if value == nil {
		return zero, true
	}
	typed, ok := value.(T)
	return typed, ok
}

func decodeOperand(codec schemaext.Codec, data json.RawMessage) (schemaext.Value, error) {
	if strings.TrimSpace(string(data)) == "null" {
		return nil, nil
	}
	payload, err := codec.Decode(data)
	if err != nil {
		return nil, err
	}
	value, ok := payload.(schemaext.Value)
	if !ok {
		return nil, fmt.Errorf("%w: unexpected row-security operand %T", schemaext.ErrInvalidValue, payload)
	}
	return value, nil
}
