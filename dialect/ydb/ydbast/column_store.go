package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// AlterColumnStoreTTLKind identifies an in-place change to a column table's
// tiered TTL.
const AlterColumnStoreTTLKind schemaext.Kind = "ptah.run/ydb/alter-column-store-ttl"

// AlterColumnStoreTTL sets a column table's tiered TTL, `ALTER TABLE t SET
// (TTL = ...)`, or removes the TTL the table holds, `ALTER TABLE t RESET
// (TTL)`, when Policy is nil.
type AlterColumnStoreTTL struct {
	Policy *ydbschema.TieredTTL `json:"policy,omitzero"`
}

// Kind returns the stable operation identity.
func (*AlterColumnStoreTTL) Kind() schemaext.Kind { return AlterColumnStoreTTLKind }

// CloneExtension returns an independent operation; a nil receiver remains
// typed nil.
func (v *AlterColumnStoreTTL) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [AlterColumnStoreTTL.CloneExtension] without the interface: it
// shares nothing with v, and a nil receiver returns nil.
func (v *AlterColumnStoreTTL) Copy() *AlterColumnStoreTTL {
	if v == nil {
		return nil
	}
	return &AlterColumnStoreTTL{Policy: v.Policy.Clone()}
}

// Effect reports that the policy decides which rows YDB moves out of the
// table and deletes.
func (*AlterColumnStoreTTL) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "a tiered TTL decides which rows YDB moves to external storage or deletes; restoring a prior TTL brings no row back"}
}

// Validate requires a valid policy where one is set.
func (v *AlterColumnStoreTTL) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: YDB column storage TTL operation is nil", schemaext.ErrInvalidValue)
	}
	if v.Policy == nil {
		return nil
	}
	return ydbschema.ValidateDesiredColumnStore(&ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{TTL: v.Policy}})
}

// columnStoreTTLShape is the operation object's one optional key.
var columnStoreTTLShape = schemaext.ObjectShape{Name: "YDB column storage TTL operation", Allowed: []string{"policy"}}

// ColumnStoreTTLCodec returns the version-one operation codec. The wire shape
// is the owner's tiered TTL, never a serialized common Go AST. An omitted
// policy removes the TTL; null is refused. Every refusal is a
// [schemaext.InvalidModelError].
func ColumnStoreTTLCodec() schemaext.Codec {
	return schemaext.ModelCodec[*AlterColumnStoreTTL]{
		Prototype: &AlterColumnStoreTTL{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"policy":"optional; the ttl object of the column storage, omitted to remove the table's TTL"}`,
			ydbschema.ColumnStoreWireDefinition())),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, columnStoreTTLShape)
			if err != nil {
				return err
			}
			raw, found := fields["policy"]
			if !found {
				return nil
			}
			wrapped, err := json.Marshal(map[string]json.RawMessage{"ttl": raw})
			if err != nil {
				return err
			}
			_, err = ydbschema.ColumnStoreCodecs()[0].Decode(wrapped)
			return err
		},
		Validate: (*AlterColumnStoreTTL).Validate,
		Clone:    (*AlterColumnStoreTTL).Copy,
	}.Codec()
}
