package parser

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/defaultlit"
)

// refuseRelativeTimeDefault refuses a string default on a column of a built-in
// date or time type that the engine reads as a moment relative to when it
// reads it, such as PostgreSQL's 'now', written as a value or as an expression
// such as ('now'). The engine reads the default once, when it creates the
// table: measured on PostgreSQL 18, DEFAULT 'now' and DEFAULT ('now') on a
// timestamptz store that timestamp, so every row gets the moment the table
// was created, and the declaration differs from the stored default on every
// comparison. A column typed by a domain is left to the model, which sees
// every domain the schema declares, including one declared after the table.
func (p *Parser) refuseRelativeTimeDefault(column *ast.ColumnNode, value *ast.DefaultValue, start int) error {
	text := value.Expression
	if value.ValueSet {
		text = value.Value
	}
	if !defaultlit.DateTimeType(column.Type) || !defaultlit.RelativeTimeLiteral(text, p.target.grammar.Columns.RelativeTimeLiterals) {
		return nil
	}
	return ptaherr.At(start, fmt.Errorf(
		"column %s: DEFAULT %s on a %s column is read once, when the table is created, so every row gets that moment and "+
			"every plan changes the column again; write a function the server calls for each row, such as now() or CURRENT_DATE",
		column.Name, text, column.Type))
}
