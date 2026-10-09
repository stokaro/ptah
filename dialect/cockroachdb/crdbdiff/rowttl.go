// Package crdbdiff owns captured directional changes to CockroachDB row-level
// TTL. A change carries both operands so planning and reversal need no other
// state.
package crdbdiff

import (
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// RowTTLKind identifies a change to a surviving table's row-level TTL.
const RowTTLKind schemaext.Kind = "ptah.run/cockroachdb/row-ttl-change"

// RowTTL retains the observed policy and the requested one. A nil Before is a
// table the read covered and found without a TTL; a nil After requests that
// the policy be removed. Both nil is not a change. Table creation and removal
// carry the policy with the table and never produce this change.
type RowTTL struct {
	Before *crdbschema.ObservedRowTTL `json:"before,omitzero"`
	After  *crdbschema.DesiredRowTTL  `json:"after,omitzero"`
}

// Kind returns the owned change identity.
func (*RowTTL) Kind() schemaext.Kind { return RowTTLKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *RowTTL) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*RowTTL)(nil)
	}
	result := &RowTTL{}
	if v.Before != nil {
		result.Before = &crdbschema.ObservedRowTTL{Policy: v.Before.Policy.Clone()}
	}
	if v.After != nil {
		result.After = &crdbschema.DesiredRowTTL{Policy: v.After.Policy.Clone()}
	}
	return result
}

// Validate requires at least one valid operand. Invalid operands wrap
// schemaext.ErrInvalidValue. It does not decide whether the operands differ on
// the server; the comparison owner produced the change because they do.
func Validate(v *RowTTL) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a CockroachDB row-level TTL change requires an operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := crdbschema.ValidateObserved(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return crdbschema.ValidateDesired(v.After)
	}
	return nil
}
