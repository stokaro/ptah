package postgres

import (
	"fmt"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/crdbttl"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// applyRowTTLChanges appends the statements each table's row-level TTL
// transition needs.
//
// Three transitions exist and they are three different statements:
//
//	no policy -> a policy      ALTER TABLE t SET (ttl_expiration_expression = '...')
//	a policy  -> a policy      SET for what changed, RESET for what was dropped
//	a policy  -> no policy     ALTER TABLE t RESET (ttl)
//
// The middle one is the reason the diff carries both sides. A parameter present
// on the target and absent from the declaration has to be RESET by name: a SET
// naming only the remaining parameters leaves it in place, because SET replaces
// what it names and nothing else. Measured on v26.2.5, `SET (ttl_job_cron =
// '@hourly')` on a table already carrying ttl_select_batch_size leaves that
// batch size untouched.
//
// `RESET (ttl)` removes the whole configuration in one statement, which is why
// the last transition is one statement and not one per parameter. Measured, it
// also succeeds against a table that never had a TTL, so the removal is
// idempotent -- worth knowing because a plan may be replayed.
func (p *Planner) applyRowTTLChanges(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, tableDiff := range diff.TablesModified {
		change := tableDiff.RowTTLChange
		if change == nil {
			continue
		}
		operations := rowTTLOperations(change)
		if len(operations) == 0 {
			continue
		}
		result = append(result, ast.NewComment(
			fmt.Sprintf("Row-level TTL on table: %s", tableDiff.TableName)))
		result = append(result, &ast.AlterTableNode{
			Name:       tableDiff.TableName,
			Operations: operations,
		})
	}
	return result
}

// rowTTLOperations turns one transition into the operations that reach it.
//
// The RESET comes before the SET when both are needed. Either order converges,
// but a fixed one is what makes the statement text a function of the two states
// alone -- the migration layer fingerprints the plan, and a plan whose text
// depended on map iteration would need re-approving on every run.
func rowTTLOperations(change *types.RowTTLChange) []ast.AlterOperation {
	if change.Desired.IsZero() {
		// The whole policy goes, whatever it consisted of.
		return []ast.AlterOperation{
			&ast.ResetRowTTLOperation{Parameters: []string{crdbttl.MarkerParameter}},
		}
	}

	var operations []ast.AlterOperation
	if dropped := crdbttl.DroppedParameters(change.Desired, change.Current); len(dropped) > 0 {
		operations = append(operations, &ast.ResetRowTTLOperation{Parameters: dropped})
	}
	options := crdbttl.Options(change.Desired)
	rendered := make([]string, 0, len(options))
	for _, option := range options {
		rendered = append(rendered, option.Name+" = "+option.Value)
	}
	if len(rendered) > 0 {
		operations = append(operations, &ast.SetRowTTLOperation{Options: rendered})
	}
	return operations
}

// planningRowTTL reports whether this target may plan a TTL change at all.
//
// A diff carrying one on a target without the capability means the comparison
// saw a declared policy the renderer will refuse, so nothing is emitted here and
// the refusal arrives from the renderer with its measured explanation rather
// than as an ALTER the server rejects.
func (p *Planner) planningRowTTL() bool {
	return p.capabilities().Has(capability.RowLevelTTL)
}
