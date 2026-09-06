package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestCreateTable_ComputedColumnsKeepTheirExpression pins how a column computed
// from others is written.
//
// Nothing wrote either keyword before, so the expression was dropped and the
// replayed column was a plain one that holds the empty string forever where the
// source computes it per row. Measured on clickhouse/clickhouse-server:26.7,
// source against replay in system.columns:
//
//	src   full_name  String  MATERIALIZED  concat(email, ' ')
//	rep   full_name  String
//
// `schema apply --dry-run` reported `Schema is synced` throughout and the
// replay reported success (stokaro/ptah#2142).
//
// The kinds are separate rows because the two ClickHouse has differ in whether
// the data EXISTS: MATERIALIZED is computed and stored when a row is inserted,
// ALIAS is computed on read and never stored. A column guessed into the wrong
// one is not a spelling difference.
func TestCreateTable_ComputedColumnsKeepTheirExpression(t *testing.T) {
	tests := []struct {
		name string
		kind string
		want string
	}{
		{
			// What the catalog answers for a ClickHouse column: the native
			// keyword, which passes through.
			name: "the native stored kind",
			kind: "MATERIALIZED",
			want: "  full_name String MATERIALIZED concat(email, ' ')",
		},
		{
			name: "the native read-time kind",
			kind: "ALIAS",
			want: "  full_name String ALIAS concat(email, ' ')",
		},
		{
			// What a DECLARED schema carries instead. STORED means the value is
			// written, which is MATERIALIZED.
			name: "the declared stored kind",
			kind: "STORED",
			want: "  full_name String MATERIALIZED concat(email, ' ')",
		},
		{
			name: "the declared read-time kind",
			kind: "VIRTUAL",
			want: "  full_name String ALIAS concat(email, ' ')",
		},
		{
			// An unnamed kind takes the one that stores. The alternative is a
			// column absent from every part on disk, which is found out by
			// noticing a query got cheaper.
			name: "no kind at all",
			kind: "",
			want: "  full_name String MATERIALIZED concat(email, ' ')",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := render(t, &ast.CreateTableNode{
				Name: "customers",
				Columns: []*ast.ColumnNode{
					{Name: "id", Type: "UInt64", Primary: true},
					{Name: "email", Type: "String"},
					{
						Name:                "full_name",
						Type:                "String",
						GeneratedExpression: "concat(email, ' ')",
						GeneratedKind:       test.kind,
					},
				},
			})

			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestCreateTable_AComputedColumnTakesNoDefault is the constraint that makes
// the clause a switch rather than something appended.
//
// The keyword takes the PLACE of DEFAULT in ClickHouse, and a column carrying
// both is refused by the server. A column can reach the renderer with both set
// -- the catalog fills default_expression for a MATERIALIZED column too, which
// is where the value comes from -- so this is a shape that occurs rather than
// one that has to be constructed.
func TestCreateTable_AComputedColumnTakesNoDefault(t *testing.T) {
	c := qt.New(t)

	sql := render(t, &ast.CreateTableNode{
		Name: "customers",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "UInt64", Primary: true},
			{Name: "email", Type: "String"},
			{
				Name:                "full_name",
				Type:                "String",
				GeneratedExpression: "concat(email, ' ')",
				GeneratedKind:       "MATERIALIZED",
				Default:             &ast.DefaultValue{Expression: "concat(email, ' ')"},
			},
		},
	})

	c.Assert(sql, qt.Contains, "  full_name String MATERIALIZED concat(email, ' ')")
	c.Assert(sql, qt.Not(qt.Contains), "DEFAULT")
}

// TestCreateTable_APlainDefaultIsUntouched is the control.
//
// A column with a default and no expression must render exactly what it
// rendered before, or the switch above has eaten the ordinary case.
func TestCreateTable_APlainDefaultIsUntouched(t *testing.T) {
	c := qt.New(t)

	sql := render(t, &ast.CreateTableNode{
		Name: "customers",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "UInt64", Primary: true},
			{Name: "status", Type: "String", Default: &ast.DefaultValue{Value: "new"}},
			{Name: "seen_at", Type: "DateTime", Default: &ast.DefaultValue{Expression: "now()"}},
		},
	})

	c.Assert(sql, qt.Contains, "  status String DEFAULT 'new'")
	c.Assert(sql, qt.Contains, "DEFAULT now()")
	c.Assert(sql, qt.Not(qt.Contains), "MATERIALIZED")
	c.Assert(sql, qt.Not(qt.Contains), "ALIAS")
}
