package pgextension

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
)

// Change records how one extension differs. A nil Before installs it, a nil
// After drops it, and both together change a surviving one. Both describe
// established state, never an unread one.
//
// For a surviving extension the change also records what the comparison
// established and the operands cannot show: Schema, the schema the extension
// moves to, resolved under the target's default schema and empty when it
// stays; and Comment, set when the declared comment differs from the one the
// target stores and reads back. A version change needs no record: the
// declaration names a version the extension does not have.
type Change struct {
	Before  *ObservedExtension `json:"before"`
	After   *DesiredExtension  `json:"after"`
	Schema  string             `json:"schema"`
	Comment bool               `json:"comment"`
}

// Kind returns the stable change identity.
func (*Change) Kind() schemaext.Kind { return ChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Change) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Change)(nil)
	}
	return v.Copy()
}

// Copy is [Change.CloneChange] without the interface: it shares no operand
// with v, and a nil receiver returns nil.
func (v *Change) Copy() *Change {
	if v == nil {
		return nil
	}
	return &Change{Before: v.Before.Copy(), After: v.After.Copy(), Schema: v.Schema, Comment: v.Comment}
}

// RemovesState reports a drop: a change without a declaration removes the
// extension.
func (v *Change) RemovesState() bool { return v != nil && v.After == nil }

// UpdatesVersion reports whether a surviving extension moves to the version
// the declaration names.
func (v *Change) UpdatesVersion() bool {
	if v == nil || v.Before == nil || v.After == nil {
		return false
	}
	declared := strings.TrimSpace(v.After.Version)
	return declared != "" && declared != strings.TrimSpace(v.Before.Version)
}

// Validate refuses missing or invalid operands, a move or a comment on an
// extension the change installs or drops, and a change of a surviving
// extension that changes nothing.
func (v *Change) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: an extension change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ValidateObserved(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := ValidateDesired(v.After); err != nil {
			return err
		}
	}
	if err := validTexts("schema", v.Schema); err != nil {
		return err
	}
	if v.Before == nil || v.After == nil {
		if v.Schema != "" || v.Comment {
			return fmt.Errorf("%w: only a surviving extension moves or changes its comment", schemaext.ErrInvalidValue)
		}
		return nil
	}
	if v.Schema == "" && !v.Comment && !v.UpdatesVersion() {
		return fmt.Errorf("%w: an extension change of a surviving extension changes its version, its schema or its comment", schemaext.ErrInvalidValue)
	}
	return nil
}

// Effect records what the change does to the extension and the objects it
// owns.
func (v *Change) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "installs an extension"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP EXTENSION removes database objects owned by the extension"}
	case v.Schema != "" || v.UpdatesVersion():
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes the version or the schema of an extension and of the objects it owns"}
	default:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "changes only the comment of an extension"}
	}
}

// changeShape is the change wire. Every key is required, and an operand is
// null where the change has none.
var changeShape = schemaext.ObjectShape{Name: "extension change",
	Allowed:  []string{"before", "after", "schema", "comment"},
	Required: []string{"before", "after", "schema", "comment"}, Nullable: []string{"before", "after"}}

// ChangeCodec describes the complete wire of an extension change: both
// operands, each in its model's own wire form and null when absent, and what
// the comparison established. Every refusal is a
// [schemaext.InvalidModelError].
func ChangeCodec() schemaext.Codec {
	models := Codecs()
	desired, observed := models[0], models[1]
	return schemaext.ModelCodec[*Change]{
		Prototype: &Change{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["%s"],"additionalProperties":false,"properties":{`+
			`"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]},`+
			`"schema":{"type":"string"},"comment":{"type":"boolean"}}}`,
			strings.Join(changeShape.Required, `","`), observed.Definition, desired.Definition)),
		Shape:     operandsShape(observed, desired),
		Validate:  (*Change).Validate,
		Canonical: canonicalChange,
		Clone:     (*Change).Copy,
	}.Codec()
}

// operandsShape checks a change's keys, then each operand it holds with the
// model's codec. A struct decoder alone would accept an operand key in
// another letter case or an omitted value spelled out.
func operandsShape(observed, desired schemaext.Codec) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
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
	}
}

// canonicalChange orders the observed operand as its codec does, so it
// encodes to the same bytes inside a change as on its own.
func canonicalChange(value *Change) *Change {
	canonical := value.Copy()
	if canonical.Before != nil {
		canonical.Before = canonicalObserved(canonical.Before)
	}
	return canonical
}
