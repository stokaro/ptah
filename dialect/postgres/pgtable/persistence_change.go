package pgtable

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

// PersistenceChangeKind identifies a change of a table's persistence.
const PersistenceChangeKind schemaext.Kind = "ptah.run/postgres/table-persistence-change"

// PersistenceChange retains both operands for migration reports and reversal.
// A nil operand means server defaults; at least one operand must be present.
type PersistenceChange struct {
	Before *ObservedPersistence `json:"before"`
	After  *DesiredPersistence  `json:"after"`
}

// Kind returns the owned change identity.
func (*PersistenceChange) Kind() schemaext.Kind { return PersistenceChangeKind }

// Effect describes the change in table durability.
func (*PersistenceChange) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "table durability changes with write-ahead logging"}
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (c *PersistenceChange) CloneChange() schemaext.ChangeValue { return c.Copy() }

// Copy returns independent operands, or nil for a nil receiver.
func (c *PersistenceChange) Copy() *PersistenceChange {
	if c == nil {
		return nil
	}
	result := &PersistenceChange{}
	if c.Before != nil {
		result.Before = new(*c.Before)
	}
	if c.After != nil {
		result.After = new(*c.After)
	}
	return result
}

// ValidatePersistenceChange refuses nil, invalid operands and an unchanged pair.
// Every error wraps schemaext.ErrInvalidValue.
func ValidatePersistenceChange(c *PersistenceChange) error {
	if c == nil || c.Before == nil && c.After == nil {
		return fmt.Errorf("%w: table persistence change requires an operand", schemaext.ErrInvalidValue)
	}
	if c.Before != nil {
		if err := ValidateObservedPersistence(c.Before); err != nil {
			return err
		}
	}
	if c.After != nil {
		if err := ValidateDesiredPersistence(c.After); err != nil {
			return err
		}
	}
	if equalPersistence(c.After, c.Before) {
		return fmt.Errorf("%w: table persistence change requires different operands", schemaext.ErrInvalidValue)
	}
	return nil
}

// Reverse returns the inverse change without mutating c. Invalid operands are
// refused under ValidatePersistenceChange's contract.
func (c *PersistenceChange) Reverse() (*PersistenceChange, error) {
	if err := ValidatePersistenceChange(c); err != nil {
		return nil, err
	}
	result := &PersistenceChange{}
	if c.Before != nil {
		result.After = new(DesiredPersistence(*c.Before))
	}
	if c.After != nil {
		result.Before = new(ObservedPersistence(*c.After))
	}
	return result, nil
}

// PersistenceChangeCodec returns an independent version-one change codec. Both
// keys are required; absent operands are null. Operand codecs enforce their
// exact representations, including explicit default values.
func PersistenceChangeCodec() schemaext.Codec {
	return schemaext.ModelCodec[*PersistenceChange]{
		Prototype: &PersistenceChange{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(persistenceChangeDefinition),
		Shape:      persistenceChangeShape, Validate: ValidatePersistenceChange, Clone: (*PersistenceChange).Copy,
	}.Codec()
}
func persistenceChangeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "table persistence change", Allowed: []string{"before", "after"}, Required: []string{"before", "after"}, Nullable: []string{"before", "after"}})
	if err != nil {
		return err
	}
	for _, codec := range PersistenceCodecs() {
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

const persistenceOperandDefinition = `{"anyOf":[{"type":"null"},` + persistenceDefinition + `]}`
const persistenceChangeDefinition = `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":` + persistenceOperandDefinition + `,"after":` + persistenceOperandDefinition + `}}`
