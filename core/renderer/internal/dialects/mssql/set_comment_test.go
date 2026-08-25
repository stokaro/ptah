package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// SQL Server keeps a comment as an extended property, and adding, changing and
// dropping one are three different procedures -- stokaro/ptah#2168.
//
// This is the dialect the operation's HasCurrent exists for. Everywhere else
// one statement sets, changes and clears a comment alike; here calling the
// wrong procedure is an error rather than a no-op, so which one to call is a
// fact about the current state and not the desired one.
func TestVisitAlterTable_SetCommentChoosesTheProcedure(t *testing.T) {
	tests := []struct {
		name      string
		operation *ast.SetCommentOperation
		want      string
	}{
		{
			name:      "a table gaining a comment",
			operation: &ast.SetCommentOperation{Comment: "customers of record"},
			want: "EXEC sp_addextendedproperty @name = N'MS_Description', " +
				"@value = N'customers of record', @level0type = N'SCHEMA', @level0name = N'dbo', " +
				"@level1type = N'TABLE', @level1name = N'users';",
		},
		{
			name: "a table whose comment changes",
			operation: &ast.SetCommentOperation{
				Comment:    "customers of record",
				HasCurrent: true,
			},
			want: "EXEC sp_updateextendedproperty @name = N'MS_Description', " +
				"@value = N'customers of record', @level0type = N'SCHEMA', @level0name = N'dbo', " +
				"@level1type = N'TABLE', @level1name = N'users';",
		},
		{
			// A drop passes no @value: sp_dropextendedproperty does not take
			// one, and passing it answers "too many arguments specified".
			name:      "a table losing its comment",
			operation: &ast.SetCommentOperation{HasCurrent: true},
			want: "EXEC sp_dropextendedproperty @name = N'MS_Description', " +
				"@level0type = N'SCHEMA', @level0name = N'dbo', " +
				"@level1type = N'TABLE', @level1name = N'users';",
		},
		{
			name: "a column whose comment changes",
			operation: &ast.SetCommentOperation{
				Column:     "email",
				Comment:    "primary contact",
				HasCurrent: true,
			},
			want: "EXEC sp_updateextendedproperty @name = N'MS_Description', " +
				"@value = N'primary contact', @level0type = N'SCHEMA', @level0name = N'dbo', " +
				"@level1type = N'TABLE', @level1name = N'users', " +
				"@level2type = N'COLUMN', @level2name = N'email';",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.SQLServer, &ast.AlterTableNode{
				Name:       "dbo.users",
				Operations: []ast.AlterOperation{tt.operation},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, tt.want)
		})
	}
}

// An unqualified table lands in dbo, and the property's address has to spell
// that out: the levels are string literals, so there is no name for the server
// to resolve the way it would in a statement.
func TestVisitAlterTable_SetCommentAddressesAnUnqualifiedTableThroughDbo(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL(platform.SQLServer, &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.SetCommentOperation{Comment: "customers of record"},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "@level0name = N'dbo'")
	c.Assert(sql, qt.Contains, "@level1name = N'users'")
}
