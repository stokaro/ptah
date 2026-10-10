package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TablePartitioningKind identifies a change to a surviving row table's
// settings: how it splits into partitions, its read replicas and its key
// bloom filter.
const TablePartitioningKind schemaext.Kind = "ptah.run/ydb/table-partitioning-change"

// TablePartitioning retains what the table holds and what the declaration
// states. After is never nil: a declaration stating nothing keeps every
// setting the table holds and changes nothing. A nil Before is a table the
// read covered and found holding YDB's documented defaults. The statement is
// a function of the two: After read over Before, each setting After leaves
// out keeping the held value. Table creation and removal carry the settings
// with the table and never produce this change.
type TablePartitioning struct {
	Before *ydbschema.ObservedTablePartitioning `json:"before,omitzero"`
	After  *ydbschema.DesiredTablePartitioning  `json:"after"`
}

// Kind returns the owned change identity.
func (*TablePartitioning) Kind() schemaext.Kind { return TablePartitioningKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *TablePartitioning) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [TablePartitioning.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *TablePartitioning) Copy() *TablePartitioning {
	if v == nil {
		return nil
	}
	return &TablePartitioning{Before: v.Before.Copy(), After: v.After.Copy()}
}

// ValidateTablePartitioning requires a valid After and a valid Before when one
// is present. Invalid operands wrap schemaext.ErrInvalidValue. It does not
// decide whether the operands differ on the server; the comparison owner
// produced the change because they do.
func ValidateTablePartitioning(v *TablePartitioning) error {
	if v == nil || v.After == nil {
		return fmt.Errorf("%w: a YDB table partitioning change requires the settings the declaration states", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbschema.ValidateObservedTablePartitioning(v.Before); err != nil {
			return err
		}
	}
	return ydbschema.ValidateDesiredTablePartitioning(v.After)
}

// tablePartitioningShape is the change object's keys: after is required, and
// before is omitted where the table holds YDB's documented defaults.
var tablePartitioningShape = schemaext.ObjectShape{Name: "YDB table partitioning change",
	Allowed: []string{"before", "after"}, Required: []string{"after"}}

// TablePartitioningCodec returns the table partitioning change codec. Each
// operand takes the exact desired or observed wire form, checked by that
// model's codec. An omitted before is a known table holding the defaults;
// null is refused, and after is required. Every refusal is a
// [schemaext.InvalidModelError].
func TablePartitioningCodec() schemaext.Codec {
	return schemaext.ModelCodec[*TablePartitioning]{
		Prototype: &TablePartitioning{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"optional observed settings; omitted when the table holds YDB's defaults",`+
			`"after":"the desired settings","constraint":"after is present"}`, ydbschema.TablePartitioningWireDefinition())),
		Shape:    tablePartitioningChangeShape,
		Validate: ValidateTablePartitioning,
		Clone:    (*TablePartitioning).Copy,
	}.Codec()
}

// tablePartitioningChangeShape checks the change's keys and decodes each
// operand through its model's codec, which holds it to the strict wire form.
func tablePartitioningChangeShape(data json.RawMessage) error {
	return decodeOperands(data, tablePartitioningShape, ydbschema.TablePartitioningCodecs())
}

// decodeOperands checks a before/after change object against shape and
// decodes each operand present through codecs: the observed codec reads
// before, and the desired one after.
func decodeOperands(data json.RawMessage, shape schemaext.ObjectShape, codecs []schemaext.Codec) error {
	fields, err := schemaext.DecodeObject(data, shape)
	if err != nil {
		return err
	}
	for _, codec := range codecs {
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
