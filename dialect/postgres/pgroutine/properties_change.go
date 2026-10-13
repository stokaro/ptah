package pgroutine

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

// PropertiesChangeKind identifies a change of a function's evaluation properties.
const PropertiesChangeKind schemaext.Kind = "ptah.run/postgres/routine-properties-change"

// PropertiesChange retains both operands for migration reports and reversal.
// A nil operand means server defaults; at least one operand must be present.
type PropertiesChange struct {
	Before *ObservedProperties `json:"before"`
	After  *DesiredProperties  `json:"after"`
}

// Kind returns the owned change identity.
func (*PropertiesChange) Kind() schemaext.Kind { return PropertiesChangeKind }

// ReplacesOwner asks the host to write the function with CREATE OR REPLACE.
func (*PropertiesChange) ReplacesOwner() bool { return true }

// Effect describes the change in evaluation behavior.
func (*PropertiesChange) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "function evaluation changes with LEAKPROOF, STRICT or PARALLEL"}
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (c *PropertiesChange) CloneChange() schemaext.ChangeValue { return c.Copy() }

// Copy returns independent operands, or nil for a nil receiver.
func (c *PropertiesChange) Copy() *PropertiesChange {
	if c == nil {
		return nil
	}
	result := &PropertiesChange{}
	if c.Before != nil {
		result.Before = new(*c.Before)
	}
	if c.After != nil {
		result.After = new(*c.After)
	}
	return result
}

// ValidatePropertiesChange refuses nil, invalid operands and an unchanged pair.
// Every error wraps schemaext.ErrInvalidValue.
func ValidatePropertiesChange(c *PropertiesChange) error {
	if c == nil || c.Before == nil && c.After == nil {
		return fmt.Errorf("%w: function properties change requires an operand", schemaext.ErrInvalidValue)
	}
	if c.Before != nil {
		if err := ValidateObservedProperties(c.Before); err != nil {
			return err
		}
	}
	if c.After != nil {
		if err := ValidateDesiredProperties(c.After); err != nil {
			return err
		}
	}
	if c.Before != nil && c.After != nil && Properties(*c.Before) == Properties(*c.After) {
		return fmt.Errorf("%w: function properties change requires different operands", schemaext.ErrInvalidValue)
	}
	return nil
}

// Reverse returns the inverse change without mutating c. Invalid operands are
// refused under ValidatePropertiesChange's contract.
func (c *PropertiesChange) Reverse() (*PropertiesChange, error) {
	if err := ValidatePropertiesChange(c); err != nil {
		return nil, err
	}
	result := &PropertiesChange{}
	if c.Before != nil {
		result.After = new(DesiredProperties(*c.Before))
	}
	if c.After != nil {
		result.Before = new(ObservedProperties(*c.After))
	}
	return result, nil
}

// PropertiesChangeCodec returns an independent version-one change codec. Both
// keys are required; absent operands are null. Operand codecs enforce their
// exact representations, including the refusal of explicit default values.
func PropertiesChangeCodec() schemaext.Codec {
	return schemaext.ModelCodec[*PropertiesChange]{
		Prototype: &PropertiesChange{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(`{"before":"observed function properties or null","after":"declared function properties or null"}`),
		Shape:      propertiesChangeShape, Validate: ValidatePropertiesChange, Clone: (*PropertiesChange).Copy,
	}.Codec()
}
func propertiesChangeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "function properties change", Allowed: []string{"before", "after"}, Required: []string{"before", "after"}, Nullable: []string{"before", "after"}})
	if err != nil {
		return err
	}
	for _, codec := range PropertiesCodecs() {
		key := "after"
		if codec.Representation == schemaext.Observed {
			key = "before"
		}
		if string(fields[key]) == "null" {
			continue
		}
		if _, err := codec.Decode(fields[key]); err != nil {
			return err
		}
	}
	return nil
}
