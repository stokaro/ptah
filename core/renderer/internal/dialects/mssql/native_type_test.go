package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mssql"
)

// TestCreateTable_ATypeNamedForThisTargetIsWrittenAsItStands pins which types
// go through the portable mapping and which do not.
//
// mapColumnType exists for a portable declaration: VARCHAR there is the
// spelling a schema written for several engines uses, and PostgreSQL's and
// MySQL's varchar hold Unicode, so NVARCHAR preserves the meaning. Applied to a
// type somebody named for SQL Server itself it produces a different column --
// varchar is one byte per character, nvarchar is two.
//
// Measured on SQL Server 2025, sql() came back converted:
//
//	column "a" { type = sql("VARCHAR(50)") }   ->   NVARCHAR(50)
//
// which is the one form whose whole purpose is to be written as it stands. The
// SQLite renderer already answers this the same way and for the same reason:
// canonicalizing is right for a declaration a person wrote and wrong for a
// description of a database that already has one (stokaro/ptah#2040, #2147).
func TestCreateTable_ATypeNamedForThisTargetIsWrittenAsItStands(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{
			name:   "a native varchar named with sql()",
			column: &ast.ColumnNode{Name: "a", Type: "VARCHAR(50)", TypeRawSQL: true},
			want:   "  [a] VARCHAR(50)",
		},
		{
			name:   "a native char named with sql()",
			column: &ast.ColumnNode{Name: "a", Type: "CHAR(8)", TypeRawSQL: true},
			want:   "  [a] CHAR(8)",
		},
		{
			// The portable case, and the one a too-wide fix breaks. A bare
			// VARCHAR is what a schema written for several engines says, and it
			// still becomes NVARCHAR.
			name:   "a portable varchar",
			column: &ast.ColumnNode{Name: "a", Type: "VARCHAR(50)"},
			want:   "  [a] NVARCHAR(50)",
		},
		{
			name:   "a portable char",
			column: &ast.ColumnNode{Name: "a", Type: "CHAR(8)"},
			want:   "  [a] NCHAR(8)",
		},
		{
			// A type SQL Server does not have at all keeps being mapped even
			// when it is named with sql(), because there is nothing else it
			// could become -- the raw marker says "write this", and this is
			// what writing it means for a spelling the server would refuse.
			// Asserting it here is what keeps the marker from being read as
			// "skip the renderer".
			name:   "a portable boolean",
			column: &ast.ColumnNode{Name: "a", Type: "BOOLEAN"},
			want:   "  [a] BIT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := &ast.CreateTableNode{
				Name:    "dbo.t",
				Columns: []*ast.ColumnNode{test.column},
			}

			sql, err := mssql.New().Render(table)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
