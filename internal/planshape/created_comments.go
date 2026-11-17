package planshape

import "ptah.run/core/ast"

// createdTableCommentsAfterIndexes takes the comments of each table nodes create
// out of its CREATE TABLE and writes them after the indexes that follow it, as
// statements of their own: the table's own first, then its columns' in the
// table's column order. The PostgreSQL renderer writes a new table's comments
// right after its CREATE TABLE, and the community binary writes them after the
// table's indexes, as it does for a table the plan changes. A comment is read by
// nothing the plan runs, so it moves over an index without changing what the
// file does.
//
// The table is copied rather than changed, so a caller holding the planner's
// node still reads its comments.
func createdTableCommentsAfterIndexes(nodes []ast.Node) []ast.Node {
	out := make([]ast.Node, 0, len(nodes))
	var comments []ast.Node
	table := ""
	for _, node := range nodes {
		if len(comments) > 0 && isIndexOf(node, table) {
			out = append(out, node)
			continue
		}
		out = append(out, comments...)
		comments = nil
		created, ok := node.(*ast.CreateTableNode)
		if !ok {
			out = append(out, node)
			continue
		}
		bare, own := withoutComments(created)
		out = append(out, bare)
		comments, table = own, created.Name
	}
	return append(out, comments...)
}

// withoutComments is table without its comments, and the statements that set
// them. A table with no comment is returned as it is.
func withoutComments(table *ast.CreateTableNode) (*ast.CreateTableNode, []ast.Node) {
	var comments []ast.Node
	if table.Comment != "" {
		comments = append(comments, commentStatement(table.Name, "", table.Comment))
	}
	for _, column := range table.Columns {
		if column.Comment != "" {
			comments = append(comments, commentStatement(table.Name, column.Name, column.Comment))
		}
	}
	if len(comments) == 0 {
		return table, nil
	}
	bare := *table
	bare.Comment = ""
	bare.Columns = make([]*ast.ColumnNode, len(table.Columns))
	for i, column := range table.Columns {
		copied := *column
		copied.Comment = ""
		bare.Columns[i] = &copied
	}
	return &bare, comments
}

// commentStatement sets the comment of table, or of its column when column is
// not empty.
func commentStatement(table, column, comment string) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.SetCommentOperation{Column: column, Comment: comment}}}
}
