package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// IndexPartitioningKind identifies a change to a surviving global index's
// partitioning and read replicas.
const IndexPartitioningKind schemaext.Kind = "ptah.run/ydb/index-partitioning-change"

// IndexPartitioning retains what the index holds and what the declaration
// states; see [TablePartitioning], whose rules it follows. A nil Before is an
// index holding YDB's documented defaults. Index creation and removal carry
// the settings with the index and never produce this change.
type IndexPartitioning struct {
	Before *ydbschema.ObservedIndexPartitioning `json:"before,omitzero"`
	After  *ydbschema.DesiredIndexPartitioning  `json:"after"`
}

// Kind returns the owned change identity.
func (*IndexPartitioning) Kind() schemaext.Kind { return IndexPartitioningKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *IndexPartitioning) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [IndexPartitioning.CloneChange] without the interface. A nil
// receiver returns nil.
func (v *IndexPartitioning) Copy() *IndexPartitioning {
	if v == nil {
		return nil
	}
	return &IndexPartitioning{Before: v.Before.Copy(), After: v.After.Copy()}
}

// ValidateIndexPartitioning requires a valid After and a valid Before when one
// is present. Invalid operands wrap schemaext.ErrInvalidValue.
func ValidateIndexPartitioning(v *IndexPartitioning) error {
	if v == nil || v.After == nil {
		return fmt.Errorf("%w: a YDB index partitioning change requires the settings the declaration states", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbschema.ValidateObservedIndexPartitioning(v.Before); err != nil {
			return err
		}
	}
	return ydbschema.ValidateDesiredIndexPartitioning(v.After)
}

var indexPartitioningShape = schemaext.ObjectShape{Name: "YDB index partitioning change",
	Allowed: []string{"before", "after"}, Required: []string{"after"}}

// IndexPartitioningCodec returns the index partitioning change codec, which
// holds each operand to its model's wire form as [TablePartitioningCodec]
// does.
func IndexPartitioningCodec() schemaext.Codec {
	return schemaext.ModelCodec[*IndexPartitioning]{
		Prototype: &IndexPartitioning{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"optional observed settings; omitted when the index holds YDB's defaults",`+
			`"after":"the desired settings","constraint":"after is present"}`, ydbschema.IndexPartitioningWireDefinition())),
		Shape: func(data json.RawMessage) error {
			return decodeOperands(data, indexPartitioningShape, ydbschema.IndexPartitioningCodecs())
		},
		Validate: ValidateIndexPartitioning,
		Clone:    (*IndexPartitioning).Copy,
	}.Codec()
}
