package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/mssql"
)

// dropDefault is the statement that drops the default of dbo.users.status,
// whatever the server named it.
const dropDefault = "EXEC sp_executesql N'DECLARE @drop nvarchar(max) = (SELECT N''ALTER TABLE [dbo].[users] DROP CONSTRAINT '' + QUOTENAME(dc.name)" +
	" FROM sys.default_constraints AS dc WHERE dc.parent_object_id = OBJECT_ID(N''[dbo].[users]'')" +
	" AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, N''status'', ''ColumnId'')); EXEC (@drop);"

// A default is a constraint in SQL Server, and the one Ptah creates is named by
// the server, so both actions read its name from sys.default_constraints when
// they run. Setting one drops the default the column has first, because a
// column holds one at most. The literal is spelled the way CREATE TABLE spells
// it, doubled once more for the literal sp_executesql runs (stokaro/ptah#3650).
func TestMSSQL_AlterColumnDefault_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		operation *ast.AlterColumnOperation
		want      string
	}{
		{
			name:      "DROP DEFAULT",
			table:     "dbo.users",
			operation: &ast.AlterColumnOperation{ColumnName: "status", Action: ast.AlterColumnDropDefault},
			want:      dropDefault + "';\n",
		},
		{
			name:  "SET DEFAULT a literal",
			table: "dbo.users",
			operation: &ast.AlterColumnOperation{
				ColumnName: "status", Action: ast.AlterColumnSetDefault, Default: &ast.DefaultValue{Value: "active", ValueSet: true},
			},
			want: dropDefault + " ALTER TABLE [dbo].[users] ADD DEFAULT ''active'' FOR [status];';\n",
		},
		{
			name:  "SET DEFAULT an expression",
			table: "dbo.users",
			operation: &ast.AlterColumnOperation{
				ColumnName: "status", Action: ast.AlterColumnSetDefault, Default: &ast.DefaultValue{Expression: "getdate()"},
			},
			want: dropDefault + " ALTER TABLE [dbo].[users] ADD DEFAULT getdate() FOR [status];';\n",
		},
		{
			// A quoted name is the same column: the catalog is asked for its
			// name, and the statement writes it quoted once.
			name:      "a bracketed column name",
			table:     "dbo.users",
			operation: &ast.AlterColumnOperation{ColumnName: "[status]", Action: ast.AlterColumnDropDefault},
			want:      dropDefault + "';\n",
		},
		{
			// Each level of quoting doubles the quote once more: the table's
			// name is a literal inside a literal, and its bracket is escaped
			// by the identifier's own rule.
			name:  "a quote in the names",
			table: "dbo.o'k]t",
			operation: &ast.AlterColumnOperation{
				ColumnName: "it's", Action: ast.AlterColumnSetDefault, Default: &ast.DefaultValue{Value: "it's", ValueSet: true},
			},
			want: "EXEC sp_executesql N'DECLARE @drop nvarchar(max) = (SELECT N''ALTER TABLE [dbo].[o''''k]]t] DROP CONSTRAINT '' + QUOTENAME(dc.name)" +
				" FROM sys.default_constraints AS dc WHERE dc.parent_object_id = OBJECT_ID(N''[dbo].[o''''k]]t]'')" +
				" AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, N''it''''s'', ''ColumnId'')); EXEC (@drop);" +
				" ALTER TABLE [dbo].[o''k]]t] ADD DEFAULT ''it''''s'' FOR [it''s];';\n",
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

// The ALTER COLUMN actions other than the default change what SQL Server's
// ALTER COLUMN states together with the rest of the column, so they are
// refused rather than rendered as a restatement that guesses the rest.
func TestMSSQL_AlterColumnDefault_FailurePath(t *testing.T) {
	for _, operation := range []*ast.AlterColumnOperation{
		{ColumnName: "status", Action: ast.AlterColumnSetNotNull},
		{ColumnName: "status", Action: ast.AlterColumnDropNotNull},
		{ColumnName: "status", Action: ast.AlterColumnSetType, Type: "BIGINT"},
	} {
		t.Run(string(operation.Action), func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{Name: "dbo.users", Operations: []ast.AlterOperation{operation}}

			sql, err := mssql.New().Render(alter)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*ALTER COLUMN `+string(operation.Action)+`: SQL Server changes a column's type and nullability by restating the column`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// CREATE TABLE and ADD DEFAULT spell a default through one function, so a
// default a migration sets reads back the way one created with the table does.
// A literal the declaration leaves unquoted is quoted in both.
func TestMSSQL_AlterColumnDefault_SpellsTheDefaultLikeCreateTable(t *testing.T) {
	c := qt.New(t)
	create := &ast.CreateTableNode{Name: "dbo.users", Columns: []*ast.ColumnNode{ast.NewColumn("n", "INT").SetDefault("5")}}
	set := &ast.AlterTableNode{Name: "dbo.users", Operations: []ast.AlterOperation{&ast.AlterColumnOperation{
		ColumnName: "n", Action: ast.AlterColumnSetDefault, Default: ast.NewColumn("n", "INT").SetDefault("5").Default,
	}}}

	created, err := mssql.New().Render(create)
	c.Assert(err, qt.IsNil)
	altered, err := mssql.New().Render(set)
	c.Assert(err, qt.IsNil)

	c.Assert(created, qt.Contains, "[n] INT DEFAULT '5'")
	c.Assert(altered, qt.Contains, "ADD DEFAULT ''5'' FOR [n];")
}
