package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// A type named for ClickHouse itself is written as it stands, and a portable
// one is still mapped -- stokaro/ptah#2142.
//
// mapColumnType exists for a portable declaration: DATETIME is the spelling a
// schema written for several engines uses, ClickHouse has no such type, and
// DateTime64(3) preserves a timestamp with subsecond precision. Applied to a
// type somebody named for ClickHouse itself it produces a different column --
// DateTime is second precision and four bytes wide, DateTime64(3) is
// millisecond precision and eight.
//
// Measured on ClickHouse 26.7, sql() came back widened:
//
//	column "created_at" { type = sql("DateTime") }   ->   DateTime64(3)
//
// which is the one form whose whole purpose is to be written as it stands. The
// SQL Server renderer answers this the same way and for the same reason
// (stokaro/ptah#2147), as does SQLite for the types its catalog keeps verbatim
// (stokaro/ptah#2040).
func TestCreateTable_ATypeNamedForThisTargetIsWrittenAsItStands(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{
			name:   "a native DateTime named with sql()",
			column: &ast.ColumnNode{Name: "created_at", Type: "DateTime", TypeRawSQL: true},
			want:   "created_at DateTime\n",
		},
		{
			name:   "a native DateTime64 with its own precision",
			column: &ast.ColumnNode{Name: "created_at", Type: "DateTime64(6)", TypeRawSQL: true},
			want:   "created_at DateTime64(6)\n",
		},
		{
			// The portable case, and the one a too-wide fix breaks. A bare
			// DATETIME is what a schema written for several engines says, and
			// it still becomes DateTime64(3).
			name:   "a portable DATETIME",
			column: &ast.ColumnNode{Name: "created_at", Type: "DATETIME"},
			want:   "created_at DateTime64(3)\n",
		},
		{
			name:   "a portable TIMESTAMP",
			column: &ast.ColumnNode{Name: "created_at", Type: "TIMESTAMP"},
			want:   "created_at DateTime64(3)\n",
		},
		{
			// A nullable native type is still wrapped: the escape hatch names
			// the type, not the column's nullability.
			name:   "a nullable native type",
			column: &ast.ColumnNode{Name: "created_at", Type: "DateTime", TypeRawSQL: true, Nullable: true},
			want:   "created_at Nullable(DateTime)\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			table := &ast.CreateTableNode{
				Name:    "events",
				Options: map[string]string{"engine": "MergeTree"},
				Columns: []*ast.ColumnNode{
					{Name: "id", Type: "INT", Primary: true},
					tt.column,
				},
			}

			sql, err := renderer.RenderSQL(platform.ClickHouse, table)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, tt.want)
		})
	}
}

// The escape hatch names a type; an empty one names nothing, and the marker
// must not carry it into DDL.
//
// Without the emptiness guard the column would be written with no type at all,
// which ClickHouse answers with a syntax error at apply time rather than here.
// The mapping refuses it instead, and says which column.
func TestCreateTable_AnEmptyNativeTypeIsRefusedRatherThanWritten(t *testing.T) {
	c := qt.New(t)
	table := &ast.CreateTableNode{
		Name:    "events",
		Options: map[string]string{"engine": "MergeTree"},
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "created_at", Type: "", TypeRawSQL: true},
		},
	}

	_, err := renderer.RenderSQL(platform.ClickHouse, table)

	c.Assert(err, qt.ErrorMatches, `.*created_at.*`)
}
