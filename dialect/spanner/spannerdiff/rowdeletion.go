// Package spannerdiff owns captured directional changes to a Spanner table's
// row deletion policy. A change carries both operands so planning and reversal
// need no other state.
package spannerdiff

import (
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
)

// RowDeletionKind identifies a change to a surviving table's row deletion
// policy.
const RowDeletionKind schemaext.Kind = "ptah.run/spanner/row-deletion-policy-change"

// RowDeletion retains the observed policy and the requested one. A nil Before
// is a table the read covered and found without a policy; a nil After requests
// that the policy be removed. Both nil is not a change. Table creation and
// removal carry the policy with the table and never produce this change.
type RowDeletion struct {
	Before *spannerschema.ObservedRowDeletion `json:"before,omitzero"`
	After  *spannerschema.DesiredRowDeletion  `json:"after,omitzero"`
}

// Kind returns the owned change identity.
func (*RowDeletion) Kind() schemaext.Kind { return RowDeletionKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *RowDeletion) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*RowDeletion)(nil)
	}
	result := &RowDeletion{}
	if v.Before != nil {
		result.Before = &spannerschema.ObservedRowDeletion{Policy: v.Before.Policy}
	}
	if v.After != nil {
		result.After = &spannerschema.DesiredRowDeletion{Policy: v.After.Policy}
	}
	return result
}

// Validate requires at least one valid operand. Invalid operands wrap
// schemaext.ErrInvalidValue. It does not decide whether the operands differ on
// the server; the comparison owner produced the change because they do.
func Validate(v *RowDeletion) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a Spanner row deletion policy change requires an operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := spannerschema.ValidateObserved(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return spannerschema.ValidateDesired(v.After)
	}
	return nil
}
