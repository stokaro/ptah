package pgtable

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// DeleteColumnsChangeKind identifies a change to a foreign key's delete selection.
const DeleteColumnsChangeKind schemaext.Kind = "ptah.run/postgres/foreign-key-delete-columns-change"

// DeleteColumnsChange retains both selections for planning and reversal. A nil
// operand selects all columns of the common key on that side.
type DeleteColumnsChange struct {
	Before *ObservedDeleteColumns `json:"before"`
	After  *DesiredDeleteColumns  `json:"after"`
}

// Kind returns the owned change identity.
func (*DeleteColumnsChange) Kind() schemaext.Kind { return DeleteColumnsChangeKind }

// Effect describes the change to rows affected by a referenced-row deletion.
func (*DeleteColumnsChange) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "the foreign key's ON DELETE action changes which columns it sets"}
}

// CloneChange returns independent operands, retaining a typed nil receiver.
func (c *DeleteColumnsChange) CloneChange() schemaext.ChangeValue { return c.Copy() }

// Copy returns independent operands, or nil for a nil receiver.
func (c *DeleteColumnsChange) Copy() *DeleteColumnsChange {
	if c == nil {
		return nil
	}
	result := &DeleteColumnsChange{}
	if c.Before != nil {
		result.Before = &ObservedDeleteColumns{Columns: slices.Clone(c.Before.Columns)}
	}
	if c.After != nil {
		result.After = &DesiredDeleteColumns{Columns: slices.Clone(c.After.Columns)}
	}
	return result
}

// ValidateDeleteColumnsChange refuses nil, invalid names and identical operand
// lists. Equivalence to a complete key is decided with captured constraint data.
// Errors wrap schemaext.ErrInvalidValue.
func ValidateDeleteColumnsChange(c *DeleteColumnsChange) error {
	if c == nil || c.Before == nil && c.After == nil {
		return fmt.Errorf("%w: ON DELETE column selection change requires an operand", schemaext.ErrInvalidValue)
	}
	var before, after []string
	if c.Before != nil {
		if err := ValidateObservedDeleteColumns(c.Before); err != nil {
			return err
		}
		before = c.Before.Columns
	}
	if c.After != nil {
		if err := ValidateDesiredDeleteColumns(c.After); err != nil {
			return err
		}
		after = c.After.Columns
	}
	if slices.Equal(before, after) {
		return fmt.Errorf("%w: ON DELETE column selection change requires different operands", schemaext.ErrInvalidValue)
	}
	return nil
}

// Reverse swaps the operands without changing the input. Invalid changes fail
// under ValidateDeleteColumnsChange's contract.
func (c *DeleteColumnsChange) Reverse() (*DeleteColumnsChange, error) {
	if err := ValidateDeleteColumnsChange(c); err != nil {
		return nil, err
	}
	result := &DeleteColumnsChange{}
	if c.Before != nil {
		result.After = &DesiredDeleteColumns{Columns: slices.Clone(c.Before.Columns)}
	}
	if c.After != nil {
		result.Before = &ObservedDeleteColumns{Columns: slices.Clone(c.After.Columns)}
	}
	return result, nil
}

const deleteColumnsOperandDefinition = `{"anyOf":[{"type":"null"},` + deleteColumnsDefinition + `]}`
const deleteColumnsChangeDefinition = `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":` + deleteColumnsOperandDefinition + `,"after":` + deleteColumnsOperandDefinition + `}}`

// DeleteColumnsChangeCodec returns the version-one change codec. Both operand
// keys are required; null selects all columns of the common key on that side.
func DeleteColumnsChangeCodec() schemaext.Codec {
	return schemaext.ModelCodec[*DeleteColumnsChange]{Prototype: &DeleteColumnsChange{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(deleteColumnsChangeDefinition), Shape: deleteColumnsChangeShape,
		Validate: ValidateDeleteColumnsChange, Clone: (*DeleteColumnsChange).Copy}.Codec()
}
func deleteColumnsChangeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "ON DELETE column selection change", Allowed: []string{"before", "after"}, Required: []string{"before", "after"}, Nullable: []string{"before", "after"}})
	if err != nil {
		return err
	}
	for _, key := range []string{"before", "after"} {
		if string(fields[key]) != "null" {
			if err := deleteColumnsShape(fields[key]); err != nil {
				return err
			}
		}
	}
	return nil
}
