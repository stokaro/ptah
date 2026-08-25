package postgres

import (
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/crdbttl"
	"go.5x5.cz/ptah/internal/spannerttl"
)

// renderRowTTL returns the ` WITH (...)` clause a CREATE TABLE carries for its
// row-level TTL, and the empty string for a table declaring none.
//
// The capability gate is a refusal rather than a silent drop, and that is the
// whole point of gating it here. Row-level TTL deletes rows. A renderer that
// quietly omitted the clause on a target without [capability.RowLevelTTL] would
// emit a CREATE TABLE the server accepts, report success, and leave a table
// whose declared retention policy simply does not exist — the operator would
// have to notice by finding rows that should have expired. Measured on
// PostgreSQL 18.4 the server itself refuses the parameter, but YugabyteDB 2026.1
// answers `WARNING: storage parameter ttl_expiration_expression is unsupported,
// ignoring` first, which is exactly the outcome the refusal exists to prevent
// (stokaro/ptah#1027).
func (r *Renderer) renderRowTTL(node *ast.CreateTableNode) (string, error) {
	if node.RowTTL.IsZero() {
		return "", nil
	}
	if !r.capabilities().Has(capability.RowLevelTTL) {
		return "", r.rowTTLUnsupported(node.Name)
	}
	options := crdbttl.Options(node.RowTTL)
	rendered := make([]string, 0, len(options))
	for _, option := range options {
		rendered = append(rendered, option.Name+" = "+option.Value)
	}
	return " WITH (" + strings.Join(rendered, ", ") + ")", nil
}

// rowTTLUnsupported is the refusal a target without the capability gets.
//
// It names the dialect and the alternative rather than only saying no: the
// operator's next question is always whether some other spelling would work,
// and on a PostgreSQL-wire engine that is not CockroachDB the answer is that
// there is none.
func (r *Renderer) rowTTLUnsupported(table string) error {
	return unsupportedFeaturef(
		"%s: table %q declares row-level TTL: it is a CockroachDB table storage parameter, and this "+
			"target does not have it — PostgreSQL answers `unrecognized parameter %q` and YugabyteDB "+
			"warns that it is ignoring the parameter, so Ptah refuses the declaration rather than "+
			"emitting a statement whose retention policy the server may drop",
		r.dialect, table, crdbttl.ExpirationExpressionParameter)
}

// writeRowTTLOperation renders whichever of the two row-level TTL operations it
// is handed, and nothing for any other node.
//
// It exists so [Renderer.VisitAlterTable] carries one arm for the pair rather
// than two: that switch already renders every ALTER shape every PostgreSQL-wire
// dialect has, and its complexity is budgeted.
func (r *Renderer) writeRowTTLOperation(node *ast.AlterTableNode, operation ast.AlterOperation) error {
	switch op := operation.(type) {
	case *ast.SetRowTTLOperation:
		return r.writeSetRowTTL(node, op)
	case *ast.ResetRowTTLOperation:
		return r.writeResetRowTTL(node, op)
	default:
		return nil
	}
}

// writeSetRowTTL emits `ALTER TABLE ... SET (...)`, which both adds a policy
// and changes one.
//
// Measured on v25.4.14 and v26.2.5, the statement is the same either way: `SET`
// against a table with no TTL turns one on, and against a table that has one it
// replaces the named parameters and leaves the rest. Each mutating form starts a
// schema-change job and the server prints a NOTICE naming it.
func (r *Renderer) writeSetRowTTL(node *ast.AlterTableNode, op *ast.SetRowTTLOperation) error {
	if len(op.Options) == 0 {
		return nil
	}
	if !r.capabilities().Has(capability.RowLevelTTL) {
		return r.rowTTLUnsupported(node.Name)
	}
	r.w.WriteLinef("ALTER TABLE %s SET (%s);",
		r.escapeQualifiedIdentifier(node.Name), strings.Join(op.Options, ", "))
	return nil
}

// writeResetRowTTL emits `ALTER TABLE ... RESET (...)`, which removes a whole
// policy or individual parameters depending on what it is handed.
//
// The parameter names are NOT identifier-quoted, and that is a choice rather
// than a requirement: measured on v26.2.5, `RESET ("ttl_job_cron")` is accepted
// exactly as the bare form is. They are left bare because they are storage
// parameter keywords rather than object names, and because nothing here comes
// from user input -- every name emitted is a constant in internal/crdbttl, so
// there is no identifier to escape.
func (r *Renderer) writeResetRowTTL(node *ast.AlterTableNode, op *ast.ResetRowTTLOperation) error {
	if len(op.Parameters) == 0 {
		return nil
	}
	if !r.capabilities().Has(capability.RowLevelTTL) {
		return r.rowTTLUnsupported(node.Name)
	}
	r.w.WriteLinef("ALTER TABLE %s RESET (%s);",
		r.escapeQualifiedIdentifier(node.Name), strings.Join(op.Parameters, ", "))
	return nil
}

// renderRowDeletionPolicy returns the ` TTL INTERVAL '…' ON …` clause a CREATE
// TABLE carries for its row deletion policy, and the empty string for a table
// declaring none.
//
// Like renderRowTTL it refuses rather than drops, and for the same reason: the
// clause deletes rows, so a renderer that quietly omitted it on a target
// without the capability would emit a CREATE TABLE the server accepts, report
// success, and leave a table whose declared retention simply does not exist.
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
	return spannerttl.Render(node.RowDeletionPolicy, r.escapeIdentifier), nil
}

// rowDeletionPolicyUnsupported is the refusal a target without the capability
// gets. It names the alternative, because the operator's next question is
// whether some other spelling would work on this engine.
func (r *Renderer) rowDeletionPolicyUnsupported(table string) error {
	return unsupportedFeaturef(
		"%s: table %q declares a row deletion policy: it is a Spanner table clause, and this target "+
			"does not have it — on a PostgreSQL-wire engine that is not Spanner the row-expiry "+
			"spelling is CockroachDB's row-level TTL storage parameters, so Ptah refuses the "+
			"declaration rather than emitting a statement that silently keeps every row",
		r.dialect, table)
}

// writeRowExpiryOperation renders whichever row-expiry operation it is handed,
// and nothing for any other node.
//
// The two pairs are unrelated -- a CockroachDB row-level TTL is a set of
// storage parameters and a row deletion policy is a Spanner table clause -- but
// they reach VisitAlterTable through the same switch, whose complexity is
// budgeted, so one arm dispatches all four.
func (r *Renderer) writeRowExpiryOperation(node *ast.AlterTableNode, operation ast.AlterOperation) error {
	switch op := operation.(type) {
	case *ast.SetRowDeletionPolicyOperation:
		return r.writeSetRowDeletionPolicy(node, op)
	case *ast.DropRowDeletionPolicyOperation:
		return r.writeDropRowDeletionPolicy(node)
	default:
		return r.writeRowTTLOperation(node, operation)
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
