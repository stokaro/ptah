package parser

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
)

// refuseRelativeTimeDefault refuses a string default on a date or time column
// that the engine reads as a moment relative to when it reads it, such as
// PostgreSQL's 'now'. The engine reads the default once, when it creates the
// table: measured on PostgreSQL 18, DEFAULT 'now' on a timestamptz stores
// that timestamp, so every row gets the moment the table was created, and the
// declaration differs from the stored default on every comparison.
func (p *Parser) refuseRelativeTimeDefault(column *ast.ColumnNode, value *ast.DefaultValue, start int) error {
	literals := p.target.grammar.Columns.RelativeTimeLiterals
	typeName := strings.ToLower(column.Type)
	if len(literals) == 0 || !value.ValueSet || (!strings.Contains(typeName, "date") && !strings.Contains(typeName, "time")) {
		return nil
	}
	text := value.Value
	if cast := strings.Index(text, "'::"); cast >= 0 {
		text = text[:cast+1]
	}
	if len(text) < 2 || text[0] != '\'' || text[len(text)-1] != '\'' {
		return nil
	}
	word := strings.ToUpper(strings.TrimSpace(text[1 : len(text)-1]))
	if !slices.Contains(literals, word) {
		return nil
	}
	return ptaherr.At(start, fmt.Errorf(
		"column %s: DEFAULT %s on a %s column is read once, when the table is created, so every row gets that moment and "+
			"every plan changes the column again; write a function the server calls for each row, such as now() or CURRENT_DATE",
		column.Name, value.Value, column.Type))
}
