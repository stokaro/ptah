package postgres

import (
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/crdbttl"
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
