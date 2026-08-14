package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mssql"
)

// TestMSSQL_ModifyColumn_KeyColumnKeepsNotNull guards the SQL Server twin of the
// PostgreSQL hole measured in stokaro/ptah#1235.
//
// renderColumnForAlter is the only SQL Server path that decides nullability
// from ColumnNode.Nullable alone; the CREATE TABLE path writes PRIMARY KEY and
// takes the branch that never asks. Once SetPrimary() stopped clearing the flag
// so SQLite could answer for itself, this path respelled a key column as
// `[id] BIGINT NULL`, and SQL Server refuses to make a column nullable while a
// primary key constraint depends on it. PostgreSQL's identical hole was
// measured live and refused with SQLSTATE 42P16; this dialect is guarded by
// inspection, with no live SQL Server available to measure against.
func TestMSSQL_ModifyColumn_KeyColumnKeepsNotNull(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{
			name:   "nullable key column is still not null",
			column: ast.NewColumn("id", "BIGINT").SetPrimary(),
			want:   "ALTER TABLE [users] ALTER COLUMN [id] BIGINT NOT NULL;\n",
		},
		{
			name:   "key column declared not null is unchanged",
			column: ast.NewColumn("id", "BIGINT").SetPrimary().SetNotNull(),
			want:   "ALTER TABLE [users] ALTER COLUMN [id] BIGINT NOT NULL;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{
				Name:       "users",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: test.column}},
			}
			sql, err := mssql.New().Render(alter)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestMSSQL_ModifyColumn_OrdinaryColumnStaysNullable pins the opposite
// direction: over-correcting renderColumnForAlter to always write NOT NULL
// reddens here.
func TestMSSQL_ModifyColumn_OrdinaryColumnStaysNullable(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{
			name:   "nullable non-key column stays nullable",
			column: ast.NewColumn("nickname", "TEXT"),
			want:   "ALTER TABLE [users] ALTER COLUMN [nickname] NVARCHAR(MAX) NULL;\n",
		},
		{
			name:   "non-key column declared not null stays not null",
			column: ast.NewColumn("nickname", "TEXT").SetNotNull(),
			want:   "ALTER TABLE [users] ALTER COLUMN [nickname] NVARCHAR(MAX) NOT NULL;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{
				Name:       "users",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: test.column}},
			}
			sql, err := mssql.New().Render(alter)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
