package ydb

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbpartition"
)

// setTablePartitioning writes the ALTER TABLE ... SET that changes a row
// table's settings in place, refusing a change YDB cannot make that way.
//
// The statement is written from both sides of the change, because setting one
// setting resets others; see [ydbpartition.TableClause]. Each setting the
// table holds or takes needs its capability key, so a change that removes read
// replicas is refused on a target without [capability.ReadReplicas] as one
// that adds them is.
func (r *Renderer) setTablePartitioning(table string, op *ast.SetYDBTablePartitioningOperation) ([]string, error) {
	subject := fmt.Sprintf("table %q", table)
	if err := r.refusePartitioningKeys(subject, op.Partitioning); err != nil {
		return nil, err
	}
	if err := r.refusePartitioningKeys(subject, op.Previous); err != nil {
		return nil, err
	}
	previous, err := ydbpartition.HeldTable(op.Previous)
	if err != nil {
		return nil, refuseFact(subject, "the settings it holds: "+err.Error())
	}
	desired, err := ydbpartition.ResolveTable(op.Partitioning, previous)
	if err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	if reason := ydbpartition.TableChangeRefusal(op.Partitioning, desired, previous); reason != "" {
		return nil, refuseFact(subject, reason)
	}
	clause := ydbpartition.TableClause(desired, previous)
	if len(clause) == 0 {
		return nil, nil
	}
	return []string{fmt.Sprintf("ALTER TABLE %s SET (%s);", tablePath(table), strings.Join(clause, ", "))}, nil
}
