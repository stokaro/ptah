// Package crdbast defines owned CockroachDB operations for common AST
// extension envelopes. Its versioned payloads carry data without rendering or
// I/O.
package crdbast

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbdiff"
)

// AlterRowTTLKind identifies an in-place change to a table's row-level TTL.
const AlterRowTTLKind schemaext.Kind = "ptah.run/cockroachdb/alter-row-ttl"

// AlterRowTTL retains the complete transition, so the statements it lowers to
// are a function of the two states alone: `RESET (ttl)` for a removal, and
// otherwise `SET` of every parameter the new policy names followed by `RESET`
// of the parameters it stops naming.
type AlterRowTTL struct {
	Change crdbdiff.RowTTL `json:"change"`
}

// Kind returns the stable operation identity.
func (*AlterRowTTL) Kind() schemaext.Kind { return AlterRowTTLKind }

// CloneExtension returns independent operands; a nil receiver remains typed nil.
func (v *AlterRowTTL) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*AlterRowTTL)(nil)
	}
	return &AlterRowTTL{Change: *v.Change.CloneChange().(*crdbdiff.RowTTL)}
}

// Effect reports that the policy decides which rows a background job deletes.
// Restoring a prior policy cannot recover rows the job already deleted.
func (*AlterRowTTL) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "row-level TTL decides which rows a background job deletes; restoring a prior policy cannot recover deleted rows"}
}
