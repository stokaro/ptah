package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mssql"
)

func TestRenderer_IndexPartDirection(t *testing.T) {
	c := qt.New(t)
	index := ast.NewIndex("idx_users_lookup", "dbo.users", "email", "status").
		SetParts([]ast.IndexPart{
			{Name: "email", Desc: true},
			{Name: "status"},
		})

	sql, err := mssql.New().Render(index)

	c.Assert(err, qt.IsNil)
	c.Assert(
		sql,
		qt.Equals,
		"CREATE INDEX [idx_users_lookup] ON [dbo].[users] ([email] DESC, [status]);\n",
	)
}
