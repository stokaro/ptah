package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

// A default arrives at the renderer either bare, the way a struct tag supplies
// it, or as the literal a schema file was written with. Escaping the second
// form again changes the value it stands for:
//
//	'x'  ->  '''x'''
//
// which is the three characters `'x'` rather than the one meant
// (stokaro/ptah#1582).
//
// TestCreateTable_DefaultsAndCommentsRendered and
// TestCreateTable_DefaultValueEscapesQuotes above cover the bare spelling; this
// covers the literal one.
func TestCreateTable_DefaultLiteralIsQuotedExactlyOnce(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "a literal keeps its own quotes", value: `'active'`, want: `DEFAULT 'active'`},
		{name: "an empty literal stays empty", value: `''`, want: `DEFAULT ''`},
		{
			name:  "a literal with an escaped quote is not escaped again",
			value: `'it''s fine'`, want: `DEFAULT 'it''s fine'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			tbl := ast.NewCreateTable("lit").
				AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
			col := ast.NewColumn("status", "VARCHAR(20)").SetNotNull()
			col.Default = &ast.DefaultValue{Value: tt.value}
			tbl.AddColumn(col)

			out := render(t, tbl)

			c.Assert(out, qt.Contains, tt.want)
			c.Assert(out, qt.Not(qt.Contains), `'''`)
		})
	}
}
