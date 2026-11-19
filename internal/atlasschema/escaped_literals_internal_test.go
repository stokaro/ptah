package atlasschema

// White-box testing required: which literals a plan renders from the model is
// read from the plan's nodes into unexported state before any connection is
// touched. The public API shows it only as a refusal on a live MySQL-family
// session, which integration/backslash_escapes_session_e2e_test.go measures.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestEscapedLiterals_HappyPath names each literal the MySQL-family renderer
// writes from the model that holds a backslash, and none of the bodies the
// user wrote.
func TestEscapedLiterals_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want []string
	}{
		{
			name: "a new table",
			node: &ast.CreateTableNode{
				Name: "notes",
				Columns: []*ast.ColumnNode{
					{Name: "a", Type: "VARCHAR(30)", Default: &ast.DefaultValue{Value: `a\b`}, Comment: `c\d`},
					{Name: "e", Type: `enum('e\\f','g')`},
					{Name: "x", Type: "VARCHAR(30)", Default: &ast.DefaultValue{Expression: `concat('a\\b')`}},
					{Name: "plain", Type: "VARCHAR(30)", Default: &ast.DefaultValue{Value: "it's"}, Comment: "c'd"},
				},
				Indexes: []*ast.IndexNode{{Name: "notes_a", Table: "notes", Comment: `i\j`}},
				Comment: `t\u`,
			},
			want: []string{
				"the default of column notes.a",
				"the comment of column notes.a",
				"the type of column notes.e",
				"the comment of index notes_a on notes",
				"the comment of table notes",
			},
		},
		{
			name: "a changed table",
			node: &ast.AlterTableNode{
				Name: "notes",
				Operations: []ast.AlterOperation{
					&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "b", Type: "VARCHAR(30)", Default: &ast.DefaultValue{Value: `'b\\c'`}}},
					&ast.ModifyColumnOperation{Column: &ast.ColumnNode{Name: "c", Type: "VARCHAR(30)", Comment: `c\d`}},
					&ast.SetCommentOperation{Comment: `t\u`},
					&ast.SetCommentOperation{Column: "d", Comment: `d\e`},
					&ast.AddIndexOperation{Index: &ast.IndexNode{Name: "notes_b", Table: "notes", Comment: `i\j`}},
				},
			},
			want: []string{
				"the default of column notes.b",
				"the comment of column notes.c",
				"the comment of table notes",
				"the comment of column notes.d",
				"the comment of index notes_b on notes",
			},
		},
		{
			name: "an index",
			node: &ast.IndexNode{Name: "notes_a", Table: "notes", Comment: `i\j`},
			want: []string{"the comment of index notes_a on notes"},
		},
		{
			name: "a view body",
			node: &ast.CreateViewNode{Name: "slashes", Body: `SELECT 'a\\b' AS x`},
		},
		{
			name: "a function body",
			node: &ast.CreateFunctionNode{Name: "path_of", Body: `RETURN 'C:\path'`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := escapedLiterals([]ast.Node{tt.node}, []int{0})

			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}
