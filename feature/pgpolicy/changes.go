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

// The change wires. Every key is required, and an operand is null where the
// change has none.
var (
	policyChangeShape = schemaext.ObjectShape{Name: "policy change",
		Allowed:  []string{"before", "after", "access", "comment_only"},
		Required: []string{"before", "after", "access", "comment_only"}, Nullable: []string{"before", "after"}}
	tableStateChangeShape = schemaext.ObjectShape{Name: "table state change",
		Allowed:  []string{"before", "after", "access"},
		Required: []string{"before", "after", "access"}, Nullable: []string{"before", "after"}}
)

// PolicyChangeCodec describes the complete wire of a policy change: both
// operands, the comment-only finding and the access assessment. Each operand
// takes its model's own wire form, checked by the model's codec and encoded
// in the model's canonical order, and null when it is absent. Every refusal
// is a [schemaext.InvalidModelError].
func PolicyChangeCodec() schemaext.Codec {
	models := PolicyCodecs()
	desired, observed := models[0], models[1]
	return schemaext.ModelCodec[*PolicyChange]{
		Prototype: &PolicyChange{}, Representation: schemaext.Change, Version: 1,
		Definition: changeDefinition(policyChangeShape, observed, desired, `,"comment_only":{"type":"boolean"}`),
		Shape:      operandsShape(policyChangeShape, observed, desired),
		Validate:   (*PolicyChange).Validate, Canonical: canonicalPolicyChange, Clone: (*PolicyChange).Copy,
	}.Codec()
}

// TableStateChangeCodec describes the complete wire of a table-state change,
// whose operands are checked as [PolicyChangeCodec]'s are.
func TableStateChangeCodec() schemaext.Codec {
	models := TableStateCodecs()
	desired, observed := models[0], models[1]
	return schemaext.ModelCodec[*TableStateChange]{
		Prototype: &TableStateChange{}, Representation: schemaext.Change, Version: 1,
		Definition: changeDefinition(tableStateChangeShape, observed, desired, ""),
		Shape:      operandsShape(tableStateChangeShape, observed, desired),
		Validate:   (*TableStateChange).Validate, Clone: (*TableStateChange).Copy,
	}.Codec()
}

// changeDefinition is the schema of a change wire: the shape's required keys,
// each operand as null or its model's definition, the access assessment, and
// the family's extra properties.
func changeDefinition(shape schemaext.ObjectShape, observed, desired schemaext.Codec, extra string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"type":"object","required":["%s"],"additionalProperties":false,"properties":{`+
		`"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]},"access":%s%s}}`,
		strings.Join(shape.Required, `","`), observed.Definition, desired.Definition, schemaext.AccessEffectSchema(), extra))
}

// operandsShape checks a change's keys, then each operand it holds with the
// model's codec. A struct decoder alone would accept an operand key in
// another letter case or an omitted value spelled out.
func operandsShape(shape schemaext.ObjectShape, observed, desired schemaext.Codec) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, shape)
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
	}
}

// canonicalPolicyChange orders each operand as its model's codec does, so an
// operand encodes to the same bytes inside a change as on its own.
func canonicalPolicyChange(value *PolicyChange) *PolicyChange {
	canonical := &PolicyChange{CommentOnly: value.CommentOnly, Access: value.Access}
	if value.Before != nil {
		canonical.Before = canonicalObservedPolicy(value.Before)
	}
	if value.After != nil {
		canonical.After = canonicalDesiredPolicy(value.After)
	}
	return canonical
}
