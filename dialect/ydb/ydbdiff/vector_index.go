package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// VectorIndexKind identifies a change to the vector settings of a surviving
// index.
const VectorIndexKind schemaext.Kind = "ptah.run/ydb/vector-index-change"

// VectorIndex retains the settings an index was built with and the settings a
// declaration states for it. A nil Before is an index the read found with no
// vector settings, which is an index of another kind; a nil After is a
// declaration that states none, which only a reversal writes. YDB changes no
// setting of a built vector index, so a plan builds the index again.
type VectorIndex struct {
	Before *ydbschema.ObservedVectorIndex `json:"before,omitzero"`
	After  *ydbschema.DesiredVectorIndex  `json:"after,omitzero"`
}

// Kind returns the owned change identity.
func (*VectorIndex) Kind() schemaext.Kind { return VectorIndexKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *VectorIndex) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [VectorIndex.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *VectorIndex) Copy() *VectorIndex {
	if v == nil {
		return nil
	}
	result := &VectorIndex{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// ValidateVectorIndex requires at least one operand, each valid in its
// representation. Invalid operands wrap schemaext.ErrInvalidValue. It does
// not decide whether the operands differ on the server; the comparison owner
// produced the change because they do.
func ValidateVectorIndex(v *VectorIndex) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a YDB vector index change requires an operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbschema.ValidateObservedVectorIndex(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return ydbschema.ValidateDesiredVectorIndex(v.After)
	}
	return nil
}

// vectorIndexShape is the change object's keys; each operand is omitted where
// it is absent.
var vectorIndexShape = schemaext.ObjectShape{Name: "YDB vector index change", Allowed: []string{"before", "after"}}

// VectorIndexCodec returns the vector index change codec. Each operand takes
// the exact desired or observed wire form, checked by that model's codec. An
// omitted operand is a known absence; null is refused. Every refusal is a
// [schemaext.InvalidModelError].
func VectorIndexCodec() schemaext.Codec {
	return schemaext.ModelCodec[*VectorIndex]{
		Prototype: &VectorIndex{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"optional observed vector settings; omitted when the index held none",`+
			`"after":"optional desired vector settings; omitted when the declaration states none","constraint":"at least one operand is present"}`,
			ydbschema.VectorIndexWireDefinition())),
		Shape:    vectorIndexChangeShape,
		Validate: ValidateVectorIndex,
		Clone:    (*VectorIndex).Copy,
	}.Codec()
}

// vectorIndexChangeShape checks the change's keys and decodes each operand
// through its model's codec, which holds it to the strict wire form.
func vectorIndexChangeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, vectorIndexShape)
	if err != nil {
		return err
	}
	for _, codec := range ydbschema.VectorIndexCodecs() {
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
