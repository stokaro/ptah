package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
)

// AlterColumnFamiliesKind identifies an in-place change to a row table's
// column families.
const AlterColumnFamiliesKind schemaext.Kind = "ptah.run/ydb/alter-column-families"

// AlterColumnFamilies retains the families the table holds and the families it
// ends up holding, so the statement it lowers to is a function of the two
// states alone: one ALTER TABLE that adds the families only After names, sets
// each setting After states and Before holds otherwise, and moves each column
// whose family differs. A column the plan drops is left out of both: it goes
// with its DROP COLUMN.
type AlterColumnFamilies struct {
	Change ydbdiff.ColumnFamilies `json:"change"`
}

// Kind returns the stable operation identity.
func (*AlterColumnFamilies) Kind() schemaext.Kind { return AlterColumnFamiliesKind }

// CloneExtension returns independent operands; a nil receiver remains typed
// nil.
func (v *AlterColumnFamilies) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [AlterColumnFamilies.CloneExtension] without the interface: it
// shares no operand with v, and a nil receiver returns nil.
func (v *AlterColumnFamilies) Copy() *AlterColumnFamilies {
	if v == nil {
		return nil
	}
	return &AlterColumnFamilies{Change: *v.Change.Copy()}
}

// Effect reports that a family change rewrites no value: it changes where and
// how YDB stores the table's columns.
func (*AlterColumnFamilies) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Additive,
		Reason: "column families decide where and how YDB stores a table's columns; changing them rewrites no value"}
}

// Actions are the actions of the ALTER TABLE the operation lowers to; see
// [ydbfamily.AlterActions].
func (v *AlterColumnFamilies) Actions() []string {
	var before []ydbschema.ColumnFamily
	if v.Change.Before != nil {
		before = v.Change.Before.Families
	}
	return ydbfamily.AlterActions(v.Change.After.Families, before)
}

// Validate requires a valid change that writes at least one action.
func (v *AlterColumnFamilies) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: YDB column family operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ydbdiff.ValidateColumnFamilies(&v.Change); err != nil {
		return err
	}
	if len(v.Actions()) == 0 {
		return fmt.Errorf("%w: YDB column family operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}

// columnFamiliesShape is the operation object's one key.
var columnFamiliesShape = schemaext.ObjectShape{Name: "YDB column family operation", Allowed: []string{"change"}, Required: []string{"change"}}

// ColumnFamiliesCodec returns the version-one operation codec. The wire shape
// is the owner's change, checked by the change codec, never a serialized
// common Go AST. A transition that writes nothing is refused. Every refusal
// is a [schemaext.InvalidModelError].
func ColumnFamiliesCodec() schemaext.Codec {
	change := ydbdiff.ColumnFamiliesCodec()
	return schemaext.ModelCodec[*AlterColumnFamilies]{
		Prototype: &AlterColumnFamilies{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"change":%s}`, change.Definition)),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, columnFamiliesShape)
			if err != nil {
				return err
			}
			_, err = change.Decode(fields["change"])
			return err
		},
		Validate: (*AlterColumnFamilies).Validate,
		Clone:    (*AlterColumnFamilies).Copy,
	}.Codec()
}
