package planshape

import "ptah.run/core/ast"

// createdTableCommentsAfterIndexes takes the comments of each table nodes create
// out of its CREATE TABLE and its indexes, and writes them after the last of the
// table's indexes as statements of their own: the table's own first, then its
// columns' in the table's column order, then its indexes' in the order nodes create
// them. A table nodes create no index for takes them right after its CREATE TABLE.
// The PostgreSQL renderer writes a new table's comments right after its CREATE TABLE
// and an index's right after its CREATE INDEX, and the community binary writes them
// all after the table's indexes, as it does for a table the plan changes.
//
// The comments follow the table's last index even where a statement the index has
// to wait for, such as a view, stands between the table and its index. A comment is
// read by nothing the plan runs, and it names only the table and the index, so it
// moves past any statement after them without changing what the file does.
//
// The tables and the indexes are copied rather than changed, so a caller holding
// the planner's nodes still reads their comments.
func createdTableCommentsAfterIndexes(nodes []ast.Node) []ast.Node {
	tables := commentAnchors(nodes)
	if len(tables) == 0 {
		return nodes
	}
	anchored := make(map[int]*createdComments, len(tables))
	for _, table := range tables {
		anchored[table.anchor] = table
	}
	out := make([]ast.Node, 0, len(nodes))
	for at, node := range nodes {
		switch n := node.(type) {
		case *ast.CreateTableNode:
			if table := tables[tableKey(n.Name)]; table != nil && table.created == at {
				node = table.takeOwnComments(n)
			}
		case *ast.IndexNode:
			if table := tables[tableKey(n.Table)]; table != nil && table.created < at {
				node = table.takeIndexComment(n)
			}
		}
		out = append(out, node)
		if table := anchored[at]; table != nil {
			out = append(out, table.comments()...)
		}
	}
	return out
}

// createdComments is where the comments of one table nodes create are written, and
// the statements that set them.
type createdComments struct {
	// created is the position of the table's CREATE TABLE in nodes.
	created int
	// anchor is the position the comments follow: the table's last index, or its
	// CREATE TABLE when nodes create no index for it.
	anchor int
	// own are the comments of the table and of its columns, in that order.
	own []ast.Node
	// indexes are the comments of the table's indexes, in the order nodes create
	// the indexes.
	indexes []ast.Node
}

// commentAnchors is, for each table nodes create, where its comments go. A plan
// creates a table once, so the name is the key, and each anchor is a statement of
// one table only.
func commentAnchors(nodes []ast.Node) map[string]*createdComments {
	tables := make(map[string]*createdComments)
	for at, node := range nodes {
		switch n := node.(type) {
		case *ast.CreateTableNode:
			tables[tableKey(n.Name)] = &createdComments{created: at, anchor: at}
		case *ast.IndexNode:
			if table := tables[tableKey(n.Table)]; table != nil {
				table.anchor = at
			}
		}
	}
	return tables
}

// takeOwnComments is table without its comments and its columns', which it keeps to
// write at the anchor. A table with no comment is returned as it is.
func (t *createdComments) takeOwnComments(table *ast.CreateTableNode) *ast.CreateTableNode {
	if table.Comment != "" {
		t.own = append(t.own, commentStatement(table.Name, "", table.Comment))
	}
	for _, column := range table.Columns {
		if column.Comment != "" {
			t.own = append(t.own, commentStatement(table.Name, column.Name, column.Comment))
		}
	}
	if len(t.own) == 0 {
		return table
	}
	bare := *table
	bare.Comment = ""
	bare.Columns = make([]*ast.ColumnNode, len(table.Columns))
	for i, column := range table.Columns {
		copied := *column
		copied.Comment = ""
		bare.Columns[i] = &copied
	}
	return &bare
}

// takeIndexComment is index without its comment, which it keeps to write at the
// anchor. An index with no comment is returned as it is.
func (t *createdComments) takeIndexComment(index *ast.IndexNode) *ast.IndexNode {
	if index.Comment == "" {
		return index
	}
	t.indexes = append(t.indexes, ast.NewObjectComment(ast.CommentedIndex, index.Name, index.Comment).SetTable(index.Table))
	bare := *index
	bare.Comment = ""
	return &bare
}

// comments is every comment the table's statements carried, in the order the
// community binary writes them.
func (t *createdComments) comments() []ast.Node {
	return append(t.own, t.indexes...)
}

// commentStatement sets the comment of table, or of its column when column is not
// empty.
func commentStatement(table, column, comment string) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.SetCommentOperation{Column: column, Comment: comment}}}
}
