package postgres

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/spannerttl"
	"ptah.run/internal/tableref"
)

// renderRowDeletionPolicy returns the ` TTL INTERVAL '…' ON …` clause a CREATE
// TABLE carries for its row deletion policy, and the empty string for a table
// declaring none.
//
// Like the CockroachDB row-level TTL clause it refuses rather than drops, and
// for the same reason: the clause deletes rows, so a renderer that quietly
// omitted it on a target without the capability would emit a CREATE TABLE the
// server accepts, report success, and leave a table whose declared retention
// simply does not exist.
// The two clauses are separate because they are different syntax on different
// engines -- one is a table clause, the other a storage parameter -- and no
// engine has both (stokaro/ptah#2236).
func (r *Renderer) renderRowDeletionPolicy(node *ast.CreateTableNode) (string, error) {
	if node.RowDeletionPolicy.IsZero() {
		return "", nil
	}
	if !r.capabilities().Has(capability.RowDeletionPolicy) {
		return "", r.rowDeletionPolicyUnsupported(node.Name)
	}
	if err := r.refuseEpochColumn(node.Name, node.RowDeletionPolicy.Unit); err != nil {
		return "", err
	}
	return spannerttl.Render(node.RowDeletionPolicy, r.escapeIdentifier), nil
}

// refuseEpochColumn refuses a policy that reads an integer column in a unit,
// which Spanner's clause has no spelling for: written without the unit, the
// clause would read the column as something it is not.
func (r *Renderer) refuseEpochColumn(table, unit string) error {
	if strings.TrimSpace(unit) == "" || r.capabilities().Has(capability.RowDeletionPolicyEpochColumn) {
		return nil
	}
	return unsupportedFeaturef("%s: %s declares a row deletion policy on an integer column counting %s, which "+
		"requires target capability %s; this target's clause reads a timestamp column only",
		r.dialect, tableref.Phrase(table), unit, capability.RowDeletionPolicyEpochColumn)
}

// rowDeletionPolicyUnsupported is the refusal a target without the capability
// gets. It names the alternative, because the operator's next question is
// whether some other spelling would work on this engine.
func (r *Renderer) rowDeletionPolicyUnsupported(table string) error {
	return unsupportedFeaturef(
		"%s: %s declares a row deletion policy: it is a Spanner table clause, and this target "+
			"does not have it — on a PostgreSQL-wire engine that is not Spanner the row-expiry "+
			"spelling is CockroachDB's row-level TTL storage parameters, so Ptah refuses the "+
			"declaration rather than emitting a statement that silently keeps every row",
		r.dialect, tableref.Phrase(table))
}

// writeRowExpiryOperation renders the row deletion policy operations and the
// owned operations, and nothing for any other node.
//
// They are unrelated -- a row deletion policy is a Spanner table clause and an
// owned operation, such as CockroachDB row-level TTL, belongs to its feature --
// but they reach renderAlterTable through the same switch, whose complexity is
// budgeted, so one arm dispatches all three.
func (r *Renderer) writeRowExpiryOperation(node *ast.AlterTableNode, operation ast.AlterOperation) error {
	switch op := operation.(type) {
	case *ast.SetRowDeletionPolicyOperation:
		return r.writeSetRowDeletionPolicy(node, op)
	case *ast.DropRowDeletionPolicyOperation:
		return r.writeDropRowDeletionPolicy(node)
	case *ast.ExtensionAlterOperation:
		return r.writeExtensionOperation(node, op)
	default:
		return nil
	}
}

// writeSetRowDeletionPolicy emits ADD or ALTER, which are the same clause with
// different verbs: ADD puts a policy on a table that has none, ALTER replaces
// the one a table already carries. Measured, each is refused in the other's
// position, so the verb is not interchangeable.
func (r *Renderer) writeSetRowDeletionPolicy(node *ast.AlterTableNode, op *ast.SetRowDeletionPolicyOperation) error {
	if op.Column == "" || op.Interval == "" {
		return nil
	}
	if !r.capabilities().Has(capability.RowDeletionPolicy) {
		return r.rowDeletionPolicyUnsupported(node.Name)
	}
	if err := r.refuseEpochColumn(node.Name, op.Unit); err != nil {
		return err
	}
	verb := "ADD"
	if op.Replace {
		verb = "ALTER"
	}
	r.w.WriteLinef("ALTER TABLE %s %s TTL INTERVAL '%s' ON %s;",
		r.escapeQualifiedIdentifier(node.Name), verb, op.Interval, r.escapeIdentifier(op.Column))
	return nil
}

// writeDropRowDeletionPolicy emits the removal, which names no column: the
// clause goes and the timestamp column it referred to stays.
func (r *Renderer) writeDropRowDeletionPolicy(node *ast.AlterTableNode) error {
	if !r.capabilities().Has(capability.RowDeletionPolicy) {
		return r.rowDeletionPolicyUnsupported(node.Name)
	}
	r.w.WriteLinef("ALTER TABLE %s DROP TTL;", r.escapeQualifiedIdentifier(node.Name))
	return nil
}
