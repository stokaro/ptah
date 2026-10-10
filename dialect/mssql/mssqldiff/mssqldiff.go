// Package mssqldiff holds the directional change of one SQL Server security
// policy: the policy a read observed, the policy a declaration asks for, and
// the owner's assessment of what the change does to access. It also derives
// the statements a change needs, which the renderer writes and the planner
// sizes its transaction by. It contains no rendering or database access.
package mssqldiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// SecurityPolicyKind identifies a security policy change.
const SecurityPolicyKind schemaext.Kind = "ptah.run/mssql/security-policy-change"

// SecurityPolicy is one policy's change. A nil Before creates the policy, a
// nil After drops it, and both present alter or replace it. Access is the
// owner's assessment, computed by [Assess] from both operands and carried as
// data, so a decoder that lost it refuses the change.
type SecurityPolicy struct {
	Before *mssqlschema.ObservedSecurityPolicy `json:"before"`
	After  *mssqlschema.DesiredSecurityPolicy  `json:"after"`
	Access schemaext.AccessEffect              `json:"access"`
}

// Kind returns the change identity.
func (*SecurityPolicy) Kind() schemaext.Kind { return SecurityPolicyKind }

// CloneChange returns an independent change. A nil receiver remains typed nil.
func (v *SecurityPolicy) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [SecurityPolicy.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *SecurityPolicy) Copy() *SecurityPolicy {
	if v == nil {
		return nil
	}
	return &SecurityPolicy{Before: v.Before.Copy(), After: v.After.Copy(), Access: v.Access}
}

// AccessEffect returns the owner's assessment the change carries.
func (v *SecurityPolicy) AccessEffect() schemaext.AccessEffect {
	if v == nil {
		return schemaext.AccessEffect{}
	}
	return v.Access
}

// Validate requires at least one valid operand and a valid access assessment.
// It does not decide whether the operands differ on the server; the comparison
// produced the change because they do. A refused operand is the model's own
// [schemaext.InvalidModelError]; every other refusal wraps
// [schemaext.ErrInvalidValue].
func (v *SecurityPolicy) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a security policy change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := mssqlschema.ValidateObservedSecurityPolicy(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := mssqlschema.ValidateDesiredSecurityPolicy(v.After); err != nil {
			return err
		}
	}
	return v.Access.Validate()
}

// Effect reports the lifecycle effect, which is separate from access: a policy
// stores no rows, so no change loses data, and every change alters which rows
// its tables admit.
func (v *SecurityPolicy) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "creates a security policy, which changes the rows its tables admit"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "drops a security policy; no data is lost, and its tables stop filtering or blocking rows"}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes a security policy, which changes the rows its tables admit"}
	}
}

var changeShape = schemaext.ObjectShape{Name: "security policy change",
	Allowed: []string{"before", "after", "access"}, Required: []string{"before", "after", "access"}, Nullable: []string{"before", "after"}}

// Codecs returns the version-one change codec.
func Codecs() []schemaext.Codec { return []schemaext.Codec{Codec()} }

// Codec returns the change codec. Each operand takes the model's own observed
// or desired wire form, checked by the model's codec, and null when it is
// absent; the access record is required. Every refusal is a
// [schemaext.InvalidModelError].
func Codec() schemaext.Codec {
	models := mssqlschema.Codecs()
	desired, observed := models[0], models[1]
	definition := fmt.Sprintf(`{"type":"object","required":["before","after","access"],"additionalProperties":false,"properties":{`+
		`"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]},"access":%s}}`,
		observed.Definition, desired.Definition, schemaext.AccessEffectSchema())
	return schemaext.ModelCodec[*SecurityPolicy]{
		Prototype: &SecurityPolicy{}, Representation: schemaext.Change, Version: 1, Definition: json.RawMessage(definition),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, changeShape)
			if err != nil {
				return err
			}
			for _, operand := range []struct {
				key   string
				codec schemaext.Codec
			}{{"before", observed}, {"after", desired}} {
				if string(fields[operand.key]) == "null" {
					continue
				}
				if _, err := operand.codec.Decode(fields[operand.key]); err != nil {
					return err
				}
			}
			return nil
		},
		Validate: (*SecurityPolicy).Validate,
		Clone:    (*SecurityPolicy).Copy,
	}.Codec()
}
