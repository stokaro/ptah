// Package spannerast defines owned Spanner operations for common AST extension
// envelopes. Its versioned payloads carry data without rendering or I/O.
package spannerast

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerschema"
)

// AlterRowDeletionKind identifies an in-place change to a table's row deletion
// policy.
const AlterRowDeletionKind schemaext.Kind = "ptah.run/spanner/alter-row-deletion-policy"

// AlterRowDeletion retains the complete transition, so the statement it lowers
// to is a function of the two states alone: `DROP TTL` for a removal, `ADD TTL`
// for a table without a policy, and `ALTER TTL` for one that has a policy.
// Measured, each verb is refused in the other's position.
type AlterRowDeletion struct {
	Change spannerdiff.RowDeletion `json:"change"`
}

// Kind returns the stable operation identity.
func (*AlterRowDeletion) Kind() schemaext.Kind { return AlterRowDeletionKind }

// CloneExtension returns independent operands; a nil receiver remains typed nil.
func (v *AlterRowDeletion) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*AlterRowDeletion)(nil)
	}
	return &AlterRowDeletion{Change: *v.Change.CloneChange().(*spannerdiff.RowDeletion)}
}

// Effect reports that the policy decides which rows the server deletes.
// Restoring a prior policy cannot recover rows already deleted.
func (*AlterRowDeletion) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "a row deletion policy decides which rows the server deletes; restoring a prior policy cannot recover deleted rows"}
}

// ValidateChange requires a valid change whose operands differ: a policy
// replaced by one with the same column and the same number of hours is not a
// change. The column is compared exactly, which is the conservative reading.
func ValidateChange(change *spannerdiff.RowDeletion) error {
	if err := spannerdiff.Validate(change); err != nil {
		return err
	}
	if change.Before != nil && change.After != nil && spannerschema.Equivalent(change.After.Policy, change.Before.Policy, func(s string) string { return s }) {
		return fmt.Errorf("%w: Spanner row deletion policy operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}
