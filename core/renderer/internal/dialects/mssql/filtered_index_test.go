package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/mssql"
)

func TestRenderer_FilteredIndexRendersWherePredicate(t *testing.T) {
	c := qt.New(t)
	index := ast.NewIndex("idx_active_users", "dbo.users", "status")
	index.Condition = "[status] = (1)"

	sql, err := mssql.New().Render(index)

	c.Assert(err, qt.IsNil)
	c.Assert(
		sql,
		qt.Equals,
		"CREATE INDEX [idx_active_users] ON [dbo].[users] ([status]) WHERE [status] = (1);\n",
	)
}

func TestRenderer_UniqueFilteredIndexRendersWherePredicate(t *testing.T) {
	c := qt.New(t)
	index := ast.NewIndex("uq_users_email_live", "dbo.users", "email").SetUnique()
	index.Condition = "[deleted_at] IS NULL"

	sql, err := mssql.New().Render(index)

	c.Assert(err, qt.IsNil)
	c.Assert(
		sql,
		qt.Equals,
		"CREATE UNIQUE INDEX [uq_users_email_live] ON [dbo].[users] ([email]) WHERE [deleted_at] IS NULL;\n",
	)
}

func TestRenderer_FilteredIndexKeepsGuardAndWherePredicate(t *testing.T) {
	c := qt.New(t)
	index := ast.NewIndex("idx_active_users", "dbo.users", "status").SetIfNotExists()
	index.Condition = "[status] = (1)"

	sql, err := mssql.New().Render(index)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, ""+
		"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_active_users' AND object_id = OBJECT_ID('dbo.users'))\n"+
		"CREATE INDEX [idx_active_users] ON [dbo].[users] ([status]) WHERE [status] = (1);\n")
}

func TestRenderer_UnfilteredIndexStaysWithoutWhere(t *testing.T) {
	tests := []struct {
		name      string
		condition string
	}{
		{name: "empty condition", condition: ""},
		{name: "whitespace-only condition", condition: "   "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			index := ast.NewIndex("idx_users_status", "dbo.users", "status")
			index.Condition = test.condition

			sql, err := mssql.New().Render(index)

			c.Assert(err, qt.IsNil)
			c.Assert(
				sql,
				qt.Equals,
				"CREATE INDEX [idx_users_status] ON [dbo].[users] ([status]);\n",
			)
		})
	}
}
