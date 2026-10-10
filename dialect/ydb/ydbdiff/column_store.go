package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// ColumnStoreKind identifies a change to the column storage of a table both
// sides hold.
const ColumnStoreKind schemaext.Kind = "ptah.run/ydb/column-store-change"

// ColumnStore retains the column storage a table holds and the storage the
// declaration asks for. A nil side is row storage, so a change with one nil
// side converts the table between row and column storage. Table creation and
// removal carry the storage with the table and never produce this change.
type ColumnStore struct {
	Before *ydbschema.ObservedColumnStore `json:"before,omitzero"`
	After  *ydbschema.DesiredColumnStore  `json:"after,omitzero"`
}

// Kind returns the owned change identity.
func (*ColumnStore) Kind() schemaext.Kind { return ColumnStoreKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *ColumnStore) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [ColumnStore.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *ColumnStore) Copy() *ColumnStore {
	if v == nil {
		return nil
	}
	result := &ColumnStore{}
	if v.Before != nil {
		result.Before = &ydbschema.ObservedColumnStore{ColumnStore: v.Before.ColumnStore.Clone()}
	}
	if v.After != nil {
		result.After = &ydbschema.DesiredColumnStore{ColumnStore: v.After.ColumnStore.Clone()}
	}
	return result
}

// ValidateColumnStore requires at least one operand and valid operands.
// Invalid operands wrap schemaext.ErrInvalidValue. It does not decide whether
// the operands differ on the server; the comparison owner produced the change
// because they do.
func ValidateColumnStore(v *ColumnStore) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a YDB column storage change requires an operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbschema.ValidateObservedColumnStore(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return ydbschema.ValidateDesiredColumnStore(v.After)
	}
	return nil
}

// columnStoreShape is the change object's keys: an omitted side is row
// storage.
var columnStoreShape = schemaext.ObjectShape{Name: "YDB column storage change", Allowed: []string{"before", "after"}}

// ColumnStoreCodec returns the column storage change codec. Each operand
// takes the exact desired or observed wire form, checked by that model's
// codec. An omitted operand is row storage, null is refused, and so is a
// change with neither operand. Every refusal is a
// [schemaext.InvalidModelError].
func ColumnStoreCodec() schemaext.Codec {
	return schemaext.ModelCodec[*ColumnStore]{
		Prototype: &ColumnStore{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"optional observed column storage; omitted when the table has row storage",`+
			`"after":"optional desired column storage; omitted when the declaration asks for row storage","constraint":"at least one operand is present"}`,
			ydbschema.ColumnStoreWireDefinition())),
		Shape:    columnStoreChangeShape,
		Validate: ValidateColumnStore,
		Clone:    (*ColumnStore).Copy,
	}.Codec()
}

// columnStoreChangeShape checks the change's keys and decodes each operand
// through its model's codec, which holds it to the strict wire form.
func columnStoreChangeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, columnStoreShape)
	if err != nil {
		return err
	}
	for _, codec := range ydbschema.ColumnStoreCodecs() {
		key := "after"
		if codec.Representation == schemaext.Observed {
			key = "before"
		}
		raw, found := fields[key]
		if !found {
			continue
		}
		if _, err := codec.Decode(raw); err != nil {
			return err
		}
	}
	return nil
}
