package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// ColumnFamiliesKind identifies a change to a surviving row table's column
// families.
const ColumnFamiliesKind schemaext.Kind = "ptah.run/ydb/column-families-change"

// ColumnFamilies retains the families the table holds and the families it
// holds once the declaration is applied: every family Before holds, with each
// setting the declaration states written over the one it holds, each family
// only the declaration names, and the columns where the declaration places
// them. After is never nil, since YQL drops no family. A nil Before is a table
// the read covered and found without families. Table creation and removal
// carry the families with the table and never produce this change.
type ColumnFamilies struct {
	Before *ydbschema.ObservedColumnFamilies `json:"before,omitzero"`
	After  *ydbschema.DesiredColumnFamilies  `json:"after"`
}

// Kind returns the owned change identity.
func (*ColumnFamilies) Kind() schemaext.Kind { return ColumnFamiliesKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *ColumnFamilies) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [ColumnFamilies.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *ColumnFamilies) Copy() *ColumnFamilies {
	if v == nil {
		return nil
	}
	result := &ColumnFamilies{}
	if v.Before != nil {
		result.Before = &ydbschema.ObservedColumnFamilies{Families: ydbschema.CloneColumnFamilies(v.Before.Families)}
	}
	if v.After != nil {
		result.After = &ydbschema.DesiredColumnFamilies{Families: ydbschema.CloneColumnFamilies(v.After.Families)}
	}
	return result
}

// ValidateColumnFamilies requires a valid After and a valid Before when one is
// present. Invalid operands wrap schemaext.ErrInvalidValue. It does not decide
// whether the operands differ on the server; the comparison owner produced
// the change because they do.
func ValidateColumnFamilies(v *ColumnFamilies) error {
	if v == nil || v.After == nil {
		return fmt.Errorf("%w: a YDB column family change requires the families the table ends up holding", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbschema.ValidateObservedColumnFamilies(v.Before); err != nil {
			return err
		}
	}
	return ydbschema.ValidateDesiredColumnFamilies(v.After)
}

// columnFamiliesShape is the change object's keys: after is required, and
// before is omitted where the table had no families.
var columnFamiliesShape = schemaext.ObjectShape{Name: "YDB column family change",
	Allowed: []string{"before", "after"}, Required: []string{"after"}}

// ColumnFamiliesCodec returns the column family change codec. Each operand
// takes the exact desired or observed wire form, checked by that model's
// codec. An omitted before is a known absence; null is refused, and after is
// required. Every refusal is a [schemaext.InvalidModelError].
func ColumnFamiliesCodec() schemaext.Codec {
	return schemaext.ModelCodec[*ColumnFamilies]{
		Prototype: &ColumnFamilies{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"optional observed column families; omitted when the table had none",`+
			`"after":"the desired column families the table holds once the change runs","constraint":"after is present"}`,
			ydbschema.ColumnFamiliesWireDefinition())),
		Shape:    columnFamiliesChangeShape,
		Validate: ValidateColumnFamilies,
		Clone:    (*ColumnFamilies).Copy,
	}.Codec()
}

// columnFamiliesChangeShape checks the change's keys and decodes each operand
// through its model's codec, which holds it to the strict wire form.
func columnFamiliesChangeShape(data json.RawMessage) error {
	return decodeOperands(data, columnFamiliesShape, ydbschema.ColumnFamiliesCodecs())
}
