package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin/internal/dialects/mssql"
)

// SQL Server renames a column and a table through sp_rename. Each part of the
// old name is bracketed, because sp_rename splits an unquoted name at every
// dot and refuses a column whose name holds one (Msg 15248). The new name is
// one part, taken literally, so it is unquoted: sp_rename would keep the
// brackets as characters of the name. Every literal is N'...', which keeps a
// name outside the database's code page.
func TestMSSQL_Rename(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		operation ast.AlterOperation
		want      string
	}{
		{
			name:      "column",
			table:     "dbo.users",
			operation: &ast.RenameColumnOperation{OldName: "email", NewName: "mail"},
			want:      "EXEC sp_rename N'[dbo].[users].[email]', N'mail', N'COLUMN';\n",
		},
		{
			name:      "column of an unqualified table",
			table:     "users",
			operation: &ast.RenameColumnOperation{OldName: "email", NewName: "mail"},
			want:      "EXEC sp_rename N'[users].[email]', N'mail', N'COLUMN';\n",
		},
		{
			name:      "column with quoted names",
			table:     "[dbo].[o]]k]",
			operation: &ast.RenameColumnOperation{OldName: "[it's.a]", NewName: "[b]]c]"},
			want:      "EXEC sp_rename N'[dbo].[o]]k].[it''s.a]', N'b]c', N'COLUMN';\n",
		},
		{
			name:      "table",
			table:     "dbo.users",
			operation: &ast.RenameTableOperation{NewName: "people"},
			want:      "EXEC sp_rename N'[dbo].[users]', N'people', N'OBJECT';\n",
		},
		{
			name:      "table with a quote and a dot in the names",
			table:     `"app"."it's.t"`,
			operation: &ast.RenameTableOperation{NewName: "o'k.t"},
			want:      "EXEC sp_rename N'[app].[it''s.t]', N'o''k.t', N'OBJECT';\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{Name: test.table, Operations: []ast.AlterOperation{test.operation}}

			sql, err := mssql.New().Render(alter)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
