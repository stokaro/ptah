package mysqldiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// IndexBlockSizeKind identifies a change to the KEY_BLOCK_SIZE hint of an
// index that survives. The server keeps no other way to change it, so the
// owner's plan replaces the index.
const IndexBlockSizeKind schemaext.Kind = "ptah.run/mysql/index-block-size-change"

// IndexBlockSize retains the hint an index holds and the hint a declaration
// states for it. Both are required, and Before is retained: a comparison
// reports no change the server would discard.
type IndexBlockSize struct {
	Before *mysqlschema.ObservedIndexBlockSize `json:"before"`
	After  *mysqlschema.DesiredIndexBlockSize  `json:"after"`
}

// Kind returns the owned change identity.
func (*IndexBlockSize) Kind() schemaext.Kind { return IndexBlockSizeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *IndexBlockSize) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Effect reports the cost of applying the change: the index is replaced to
// store the new hint, and on MySQL the replacement copies the whole table.
func (*IndexBlockSize) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "the index is dropped and built again to store its new KEY_BLOCK_SIZE; on MySQL the statement asks for ALGORITHM=COPY, which rebuilds the whole table and blocks writes to it while it runs; its rows are unchanged"}
}

// Copy is [IndexBlockSize.CloneChange] without the interface. A nil receiver
// returns nil.
func (v *IndexBlockSize) Copy() *IndexBlockSize {
	if v == nil {
		return nil
	}
	result := &IndexBlockSize{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// ValidateIndexBlockSize requires both operands, each valid in its
// representation, an observation the server retains, and two different
// hints. Invalid operands wrap schemaext.ErrInvalidValue.
func ValidateIndexBlockSize(v *IndexBlockSize) error {
	if v == nil || v.Before == nil || v.After == nil {
		return fmt.Errorf("%w: a MySQL index block size change requires both operands", schemaext.ErrInvalidValue)
	}
	if err := mysqlschema.ValidateObservedIndexBlockSize(v.Before); err != nil {
		return err
	}
	if err := mysqlschema.ValidateDesiredIndexBlockSize(v.After); err != nil {
		return err
	}
	if !v.Before.Retained {
		return fmt.Errorf("%w: a MySQL index block size change requires a hint the server retains", schemaext.ErrInvalidValue)
	}
	if v.Before.KeyBlockSize == v.After.KeyBlockSize {
		return fmt.Errorf("%w: a MySQL index block size change requires two different hints", schemaext.ErrInvalidValue)
	}
	return nil
}

var indexBlockSizeShape = schemaext.ObjectShape{
	Name: "MySQL index block size change", Allowed: []string{"before", "after"}, Required: []string{"before", "after"},
}

// IndexBlockSizeCodec returns the index block-size change codec. Each operand
// takes the exact desired or observed wire form, checked by that model's
// codec. Every refusal is a [schemaext.InvalidModelError].
func IndexBlockSizeCodec() schemaext.Codec {
	return schemaext.ModelCodec[*IndexBlockSize]{
		Prototype: &IndexBlockSize{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"the observed hint","after":"the desired hint"}`,
			mysqlschema.IndexBlockSizeWireDefinition())),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, indexBlockSizeShape)
			if err != nil {
				return err
			}
			for _, codec := range mysqlschema.IndexBlockSizeCodecs() {
				key := "after"
				if codec.Representation == schemaext.Observed {
					key = "before"
				}
				if _, err := codec.Decode(fields[key]); err != nil {
					return err
				}
			}
			return nil
		},
		Validate: ValidateIndexBlockSize,
		Clone:    (*IndexBlockSize).Copy,
	}.Codec()
}

// IndexBlockSizeReversal returns the change that undoes v. The hint v
// declares becomes the observation, retained because v was planned on an
// index that retains its hint, and the hint v replaced becomes the
// declaration. An invalid v is refused with schemaext.ErrInvalidValue.
func IndexBlockSizeReversal(v *IndexBlockSize) (*IndexBlockSize, error) {
	if err := ValidateIndexBlockSize(v); err != nil {
		return nil, err
	}
	return &IndexBlockSize{
		Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: v.After.KeyBlockSize, Retained: true},
		After:  v.Before.Desired(),
	}, nil
}
