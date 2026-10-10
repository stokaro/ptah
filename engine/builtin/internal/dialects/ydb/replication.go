package ydb

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbreplication"
)

// renderCreateAsyncReplication writes one CREATE ASYNC REPLICATION with the
// replication's items, connection and the settings its declaration names. A
// declaration YDB would refuse or keep differently, or one the line cannot
// hold, is refused through [ydbreplication.CheckReplication].
func (r *Renderer) renderCreateAsyncReplication(node *ast.CreateAsyncReplicationNode) error {
	if err := replicationRefusal(ydbreplication.CheckReplication(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	r.w.WriteLine(ydbreplication.CreateReplicationStatement(node.Name, node.Spec))
	return nil
}

// renderAlterAsyncReplication writes the ALTER ASYNC REPLICATION that moves a
// paused replication's connection and credential to its declaration. A change
// YDB makes in no replication -- its items, its consistency level, its commit
// interval -- is refused rather than written, because the planner never asks
// for one; the node arrives here only through a caller that built it by hand.
func (r *Renderer) renderAlterAsyncReplication(node *ast.AlterAsyncReplicationNode) error {
	if node.SourceSettings != nil {
		return refuseFact("ALTER replication or transfer", "resolve the desired declaration before rendering its settings patch")
	}

	if err := replicationRefusal(ydbreplication.CheckReplication(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	if changes := ydbreplication.CompareReplication(node.Spec, node.Previous); len(changes.CreateOnly) > 0 {
		return replicationRefusal(ydbreplication.ReplicationChangeRefusal(node.Name, node.Spec, node.Previous, ""))
	}
	if statement := ydbreplication.AlterReplicationStatement(node.Name, node.Spec, node.Previous); statement != "" {
		r.w.WriteLine(statement)
	}
	return nil
}

// renderDropAsyncReplication writes one DROP ASYNC REPLICATION, with CASCADE
// where the node asks for its replica tables to go too.
func (r *Renderer) renderDropAsyncReplication(node *ast.DropAsyncReplicationNode) error {
	if !r.caps.Has(capability.AsyncReplication) {
		return refuseKey(capability.AsyncReplication, "DROP ASYNC REPLICATION "+node.Name)
	}
	if strings.TrimSpace(node.Name) == "" {
		return refuseFact("DROP ASYNC REPLICATION", "it names no replication")
	}
	r.w.WriteLine(ydbreplication.DropReplicationStatement(node.Name, node.Cascade))
	return nil
}

// renderCreateTransfer writes one CREATE TRANSFER with the transfer's lambda
// as declared and the settings its declaration names.
func (r *Renderer) renderCreateTransfer(node *ast.CreateTransferNode) error {
	if err := replicationRefusal(ydbreplication.CheckTransfer(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	r.w.WriteLine(ydbreplication.CreateTransferStatement(node.Name, node.Spec))
	return nil
}

// renderAlterTransfer writes the ALTER TRANSFER that moves a transfer's lambda,
// batch settings, connection and credential to its declaration. A source, a
// target or a consumer that differs is refused, since YDB changes none of
// them in place.
func (r *Renderer) renderAlterTransfer(node *ast.AlterTransferNode) error {
	if node.SourceSettings != nil {
		return refuseFact("ALTER replication or transfer", "resolve the desired declaration before rendering its settings patch")
	}

	if err := replicationRefusal(ydbreplication.CheckTransfer(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	if changes := ydbreplication.CompareTransfer(node.Spec, node.Previous); len(changes.CreateOnly) > 0 {
		return replicationRefusal(ydbreplication.TransferChangeRefusal(node.Name, node.Spec, node.Previous, ""))
	}
	if statement := ydbreplication.AlterTransferStatement(node.Name, node.Spec, node.Previous); statement != "" {
		r.w.WriteLine(statement)
	}
	return nil
}

// renderDropTransfer writes one DROP TRANSFER.
func (r *Renderer) renderDropTransfer(node *ast.DropTransferNode) error {
	if !r.caps.Has(capability.Transfers) {
		return refuseKey(capability.Transfers, "DROP TRANSFER "+node.Name)
	}
	if strings.TrimSpace(node.Name) == "" {
		return refuseFact("DROP TRANSFER", "it names no transfer")
	}
	r.w.WriteLine(ydbreplication.DropTransferStatement(node.Name))
	return nil
}

// replicationRefusal turns a replication or transfer refusal into the
// renderer's error: by the capability key it names, or by the reason YDB
// refuses it on every line.
func replicationRefusal(refusal *ydbreplication.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return refuseKey(refusal.Key, refusal.Subject)
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
