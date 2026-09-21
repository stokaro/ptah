package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
)

// Every ALTER TABLE statement a node renders carries the online request, not
// only the ones an early version of the renderer happened to route through the
// helper. MySQL reads ALGORITHM and LOCK per statement, so a branch that wrote
// the bare statement would run under whatever the server chose while the plan
// said otherwise.
func TestRenderSQL_MySQLOnlineClauseOnEveryAlterShape(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.AlterOperation
	}{
		{name: "add column", operation: &ast.AddColumnOperation{
			Column: &ast.ColumnNode{Name: "nickname", Type: "VARCHAR(64)", Nullable: true},
		}},
		{name: "drop column", operation: &ast.DropColumnOperation{ColumnName: "nickname"}},
		{name: "modify column", operation: &ast.ModifyColumnOperation{
			Column: &ast.ColumnNode{Name: "nickname", Type: "VARCHAR(128)", Nullable: true},
		}},
		{name: "rename column", operation: &ast.RenameColumnOperation{OldName: "nick", NewName: "nickname"}},
		{name: "rename table", operation: &ast.RenameTableOperation{NewName: "people"}},
		{name: "table comment", operation: &ast.SetCommentOperation{Comment: "people"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{
				Name:       "users",
				Operations: []ast.AlterOperation{test.operation},
				Algorithm:  "INPLACE",
				Lock:       "NONE",
			}

			sql, err := renderer.RenderSQLWithCapabilities("mysql", capability.MySQL84(), node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, ", ALGORITHM=INPLACE, LOCK=NONE;")
		})
	}
}

// The control: a node that asked for nothing renders what it always did.
func TestRenderSQL_MySQLWritesNoClauseWhenTheNodeAsksForNothing(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "nickname", Type: "VARCHAR(64)", Nullable: true}},
		},
	}

	sql, err := renderer.RenderSQLWithCapabilities("mysql", capability.MySQL84(), node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "ALGORITHM=")
}
