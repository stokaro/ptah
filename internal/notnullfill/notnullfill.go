// Package notnullfill decides whether a PostgreSQL-family column modification
// fills the column's NULL rows with its declared default before SET NOT NULL.
//
// Two parts of Ptah have to agree on the answer. The PostgreSQL renderer writes
// the fill, an UPDATE inside a DO block, and migration/safety judges the
// statements that come out: the fill rewrites rows, and the SET NOT NULL after
// it meets no NULL the fill could reach. The answer lives here, once, because
// two copies of the condition agree when the second is written and stop
// agreeing when the first changes. The report would then call a plan's SET
// NOT NULL safe while nothing filled the rows before it.
package notnullfill

import "ptah.run/core/ast"

// FillsNullRows reports whether the PostgreSQL family renders op with an
// UPDATE that fills the column's NULL rows from its declared default before
// SET NOT NULL.
//
// It does when the modification sets NOT NULL, the column declares a default,
// and the operation does not ask to omit the fill. A modification that states
// its changed properties sets NOT NULL only when nullability is one of them;
// one that states none restates every property. A primary key column is NOT
// NULL whatever its flag says, as the renderer writes it.
//
// A nil operation, or one without a column, fills nothing.
func FillsNullRows(op *ast.ModifyColumnOperation) bool {
	if op == nil || op.Column == nil || op.OmitNullBackfill {
		return false
	}
	if op.HasChanged && !op.Changed.Nullability {
		return false
	}
	column := op.Column
	if column.Nullable && !column.Primary {
		return false
	}
	return DeclaresDefault(column)
}

// DeclaresDefault reports whether the column carries a default the fill can
// write: an expression, or a literal.
func DeclaresDefault(column *ast.ColumnNode) bool {
	return column != nil && column.Default != nil &&
		(column.Default.Expression != "" || column.Default.HasLiteral())
}
