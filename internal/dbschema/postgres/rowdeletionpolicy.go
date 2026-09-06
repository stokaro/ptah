package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/spannerttl"
)

// rowDeletionPolicyExpr renders the projection carrying a table's row deletion
// policy, and a constant for a target that has none.
//
// It reads a column of information_schema.tables, which this query already
// selects from, so it adds no join. That is not a stylistic preference here:
// PGAdapter refuses a catalog query past twenty joins, and this reader runs
// against it (stokaro/ptah#2236).
func (r *Reader) rowDeletionPolicyExpr() string {
	if !r.caps.Has(capability.RowDeletionPolicy) {
		return "'' AS row_deletion_policy"
	}
	return "COALESCE(t.row_deletion_policy_expression, '') AS row_deletion_policy"
}

// readRowDeletionPolicy decodes the projection above into the policy the model
// carries.
//
// A policy this cannot read is an error rather than a table reported without
// one. The failure being prevented is silent: a table whose policy stopped
// existing keeps every row it was declared to delete, and that is discovered on
// the storage bill rather than by anything Ptah prints.
func readRowDeletionPolicy(expression string) (*ast.RowDeletionPolicySpec, error) {
	return spannerttl.Parse(expression)
}
