package pgtable

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

// PersistenceOperationKind identifies ALTER TABLE SET LOGGED or SET UNLOGGED.
const PersistenceOperationKind schemaext.Kind = "ptah.run/postgres/alter-table-persistence"

// AlterPersistence retains both operands of an in-place persistence change.
// The containing ALTER TABLE supplies the qualified table name.
type AlterPersistence struct {
	Change PersistenceChange `json:"change"`
}

// Kind returns the owned operation identity.
func (*AlterPersistence) Kind() schemaext.Kind { return PersistenceOperationKind }

// CloneExtension returns independent operands. A nil receiver remains typed nil.
func (op *AlterPersistence) CloneExtension() ast.ExtensionPayload {
	return clonePersistenceOperation(op)
}

func clonePersistenceOperation(op *AlterPersistence) *AlterPersistence {
	if op == nil {
		return (*AlterPersistence)(nil)
	}
	return &AlterPersistence{Change: *op.Change.Copy()}
}

// Effect describes the change in table durability.
func (op *AlterPersistence) Effect() schemaext.Effect { return op.Change.Effect() }

// ValidateAlterPersistence refuses nil and invalid or equivalent operands.
// Every error wraps schemaext.ErrInvalidValue.
func ValidateAlterPersistence(op *AlterPersistence) error {
	if op == nil {
		return fmt.Errorf("%w: nil table persistence operation", schemaext.ErrInvalidValue)
	}
	return ValidatePersistenceChange(&op.Change)
}

// PersistenceOperationCodec returns a strict version-one operation codec.
// Its change member uses the same operand contract as PersistenceChangeCodec.
func PersistenceOperationCodec() schemaext.Codec {
	return schemaext.ModelCodec[*AlterPersistence]{Prototype: &AlterPersistence{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["change"],"additionalProperties":false,"properties":{"change":` + persistenceChangeDefinition + `}}`), Shape: persistenceOperationShape,
		Validate: ValidateAlterPersistence, Clone: clonePersistenceOperation}.Codec()
}

func persistenceOperationShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "table persistence operation", Allowed: []string{"change"}, Required: []string{"change"}})
	if err != nil {
		return err
	}
	_, err = PersistenceChangeCodec().Decode(fields["change"])
	return err
}
