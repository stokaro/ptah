package mysql

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
)

// joinedStatement is one statement [withTableCollation] joins with the
// change of the table's default collation, and the comments that describe
// it.
type joinedStatement struct {
	comments []ast.Node
	alter    *ast.AlterTableNode
}

// inHeldColumnOrder puts the statements that only restate columns in the
// order held lists the columns, the order the table holds them in and the
// order the community binary writes its MODIFY COLUMN clauses in. The
// comparison reports changed columns by name, so the planner writes them in
// that order. Every other statement, one that adds a column or sets the
// table's comment, keeps its place, and so does a statement whose column held
// does not list. Column names compare without regard to case, as MySQL and
// MariaDB compare them.
func inHeldColumnOrder(statements []joinedStatement, held []catalog.Column) []joinedStatement {
	positions := make(map[string]int, len(held))
	for at, column := range held {
		positions[strings.ToLower(column.Name)] = at
	}
	positionOf := func(statement joinedStatement) (int, bool) {
		at := -1
		for _, operation := range statement.alter.Operations {
			modify, ok := operation.(*ast.ModifyColumnOperation)
			if !ok || modify.Column == nil {
				return 0, false
			}
			position, known := positions[strings.ToLower(modify.Column.Name)]
			if !known {
				return 0, false
			}
			if at < 0 {
				at = position
			}
		}
		return at, at >= 0
	}
	var slots []int
	var restated []joinedStatement
	for at, statement := range statements {
		if _, ok := positionOf(statement); ok {
			slots = append(slots, at)
			restated = append(restated, statement)
		}
	}
	slices.SortStableFunc(restated, func(a, b joinedStatement) int {
		left, _ := positionOf(a)
		right, _ := positionOf(b)
		return left - right
	})
	ordered := slices.Clone(statements)
	for at, slot := range slots {
		ordered[slot] = restated[at]
	}
	return ordered
}
