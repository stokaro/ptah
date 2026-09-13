package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
)

// TestVisitAlterTable_CommentsReachTheExtendedPropertyHandler pins that a
// comment transition on SQL Server comes out as an extended-property call.
//
// SQL Server is the one target whose ALTER TABLE handler renders a node by
// handing it back to a visitor method: it builds an ExtendedPropertyNode for
// the comment and calls its own VisitExtendedProperty on it. Routing that call
// somewhere else -- or dropping it while collapsing the visitor onto one
// dispatch method -- takes the comment out of the output with no error and no
// compile failure.
//
// The assertion is the whole output rather than a fragment, and it carries a
// table comment and a column comment in one ALTER TABLE: a substring check
// passes on a render that kept the first call and lost the second.
func TestVisitAlterTable_CommentsReachTheExtendedPropertyHandler(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL(platform.SQLServer, &ast.AlterTableNode{
		Name: "dbo.users",
		Operations: []ast.AlterOperation{
			&ast.SetCommentOperation{Comment: "customers of record"},
			&ast.SetCommentOperation{Column: "email", Comment: "primary contact", HasCurrent: true},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "EXEC sp_addextendedproperty @name = N'MS_Description', "+
		"@value = N'customers of record', @level0type = N'SCHEMA', @level0name = N'dbo', "+
		"@level1type = N'TABLE', @level1name = N'users';\n"+
		"EXEC sp_updateextendedproperty @name = N'MS_Description', "+
		"@value = N'primary contact', @level0type = N'SCHEMA', @level0name = N'dbo', "+
		"@level1type = N'TABLE', @level1name = N'users', "+
		"@level2type = N'COLUMN', @level2name = N'email';\n")
}
