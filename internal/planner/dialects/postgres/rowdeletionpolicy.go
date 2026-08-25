package postgres

import (
	"fmt"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// applyRowDeletionPolicyChanges appends the statement each table's row deletion
// policy transition needs.
//
// Three transitions, and the first two are different verbs on the same clause.
// Measured against the Cloud Spanner emulator behind PGAdapter 0.55.2, reading
// information_schema.tables.row_deletion_policy_expression back after each
// (stokaro/ptah#2236):
//
//	no policy -> a policy   ALTER TABLE t ADD TTL INTERVAL '10 days' ON ts
//	a policy  -> a policy   ALTER TABLE t ALTER TTL INTERVAL '20 days' ON ts
//	a policy  -> no policy  ALTER TABLE t DROP TTL
//
// Which of the first two applies is not derivable from the desired state alone,
// which is why the diff carries both sides.
func (p *Planner) applyRowDeletionPolicyChanges(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, tableDiff := range diff.TablesModified {
		change := tableDiff.RowDeletionPolicyChange
		if change == nil {
			continue
		}
		result = append(result, ast.NewComment(
			fmt.Sprintf("Row deletion policy on table: %s", tableDiff.TableName)))
		result = append(result, &ast.AlterTableNode{
			Name:       tableDiff.TableName,
			Operations: []ast.AlterOperation{rowDeletionPolicyOperation(change)},
		})
	}
	return result
}

// rowDeletionPolicyOperation turns one transition into the operation reaching
// it.
func rowDeletionPolicyOperation(change *types.RowDeletionPolicyChange) ast.AlterOperation {
	if change.Desired.IsZero() {
		return &ast.DropRowDeletionPolicyOperation{}
	}
	return &ast.SetRowDeletionPolicyOperation{
		Column:   change.Desired.Column,
		Interval: change.Desired.Interval,
		Replace:  !change.Current.IsZero(),
	}
}

// planningRowDeletionPolicy reports whether this target may plan such a change
// at all.
//
// A diff carrying one on a target without the capability means the comparison
// saw a declared policy the renderer will refuse, so nothing is emitted here and
// the refusal arrives from the renderer with its measured explanation rather
// than as an ALTER the server rejects. This mirrors planningRowTTL exactly.
func (p *Planner) planningRowDeletionPolicy() bool {
	return p.capabilities().Has(capability.RowDeletionPolicy)
}
