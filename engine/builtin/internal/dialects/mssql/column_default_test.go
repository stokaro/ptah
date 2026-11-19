package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/mssql"
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
// A string literal the declaration leaves unquoted is quoted in both, and a
// number beside a number type is bare in both: ADD DEFAULT names no type, so
// the planner hands it the number as an expression.
func TestMSSQL_AlterColumnDefault_SpellsTheDefaultLikeCreateTable(t *testing.T) {
	tests := []struct {
		name        string
		column      *ast.ColumnNode
		set         *ast.DefaultValue
		wantCreated string
		wantAltered string
	}{
		{
			name:        "a string",
			column:      ast.NewColumn("n", "NVARCHAR(10)").SetDefault("active"),
			set:         &ast.DefaultValue{Value: "active"},
			wantCreated: "[n] NVARCHAR(10) DEFAULT 'active'",
			wantAltered: "ADD DEFAULT ''active'' FOR [n];",
		},
		{
			name:        "a number",
			column:      ast.NewColumn("n", "INT").SetDefault("-1"),
			set:         &ast.DefaultValue{Expression: "-1"},
			wantCreated: "[n] INT DEFAULT -1",
			wantAltered: "ADD DEFAULT -1 FOR [n];",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			create := &ast.CreateTableNode{Name: "dbo.users", Columns: []*ast.ColumnNode{test.column}}
			set := &ast.AlterTableNode{Name: "dbo.users", Operations: []ast.AlterOperation{&ast.AlterColumnOperation{
				ColumnName: "n", Action: ast.AlterColumnSetDefault, Default: test.set,
			}}}

			created, err := mssql.New().Render(create)
			c.Assert(err, qt.IsNil)
			altered, err := mssql.New().Render(set)
			c.Assert(err, qt.IsNil)

			c.Assert(created, qt.Contains, test.wantCreated)
			c.Assert(altered, qt.Contains, test.wantAltered)
		})
	}
}

// TestMSSQL_NumberDefault_HappyPath writes a plain number beside a number type
// bare, as SQL Server stores DEFAULT -1 as ((-1)), and keeps the quotes beside
// any other type and on anything that is not a plain number: on an NVARCHAR
// column '007' and 007 are different defaults.
func TestMSSQL_NumberDefault_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{name: "a negative integer", column: ast.NewColumn("n", "INTEGER").SetDefault("-1"), want: "[n] INT DEFAULT -1"},
		{name: "a positive integer", column: ast.NewColumn("n", "BIGINT").SetDefault("5"), want: "[n] BIGINT DEFAULT 5"},
		{name: "a decimal", column: ast.NewColumn("n", "DECIMAL(5,2)").SetDefault("-1.50"), want: "[n] DECIMAL(5,2) DEFAULT -1.50"},
		{name: "a number already quoted", column: ast.NewColumn("n", "INT").SetDefault("'5'"), want: "[n] INT DEFAULT '5'"},
		{name: "a number beside a string type", column: ast.NewColumn("n", "NVARCHAR(10)").SetDefault("007"), want: "[n] NVARCHAR(10) DEFAULT '007'"},
		{name: "a word beside a number type", column: ast.NewColumn("n", "INT").SetDefault("NaN"), want: "[n] INT DEFAULT 'NaN'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			created, err := mssql.New().Render(&ast.CreateTableNode{Name: "dbo.t", Columns: []*ast.ColumnNode{test.column}})

			c.Assert(err, qt.IsNil)
			c.Assert(created, qt.Contains, test.want)
		})
	}
}
