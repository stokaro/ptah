package parser

import (
	"ptah.run/core/ast"
	"ptah.run/core/sqlext"
)

// recordTableDrops exposes shared DROP boundaries as data. Owners receive
// their claims in one batch, so a name alone cannot distinguish a table from
// another table created with that name after a DROP.
func (p *Parser) recordTableDrops(node ast.Node, offset int) {
	switch statement := node.(type) {
	case *ast.DropTableNode:
		for _, name := range statement.TableNames() {
			p.droppedTables = append(p.droppedTables, sqlext.DroppedTable{Name: name, Offset: offset})
		}
	case *ast.StatementList:
		for _, child := range statement.Statements {
			p.recordTableDrops(child, offset)
		}
	}
}
