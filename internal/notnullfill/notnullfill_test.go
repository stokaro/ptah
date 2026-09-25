package notnullfill_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/notnullfill"
)

// FillsNullRows answers for the operations the PostgreSQL renderer fills: a
// column made NOT NULL that declares a default, unless the operation asks to
// omit the fill. Each false row differs from the first true row in one input.
func TestFillsNullRows(t *testing.T) {
	nullability := ast.ColumnProperties{Nullability: true}
	tests := []struct {
		name string
		op   *ast.ModifyColumnOperation
		want bool
	}{
		{
			name: "NOT NULL set, a literal default declared",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("9"), Changed: nullability, HasChanged: true},
			want: true,
		},
		{
			name: "NOT NULL set, an expression default declared",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("c", "TIMESTAMPTZ").SetNotNull().SetDefaultExpression("now()"), Changed: nullability, HasChanged: true},
			want: true,
		},
		{
			name: "changes not stated, so the column is restated",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("9")},
			want: true,
		},
		{
			name: "a key column is NOT NULL whatever its flag says",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("c", "INTEGER").SetPrimary().SetDefault("9"), Changed: nullability, HasChanged: true},
			want: true,
		},
		{
			name: "the fill omitted",
			op: &ast.ModifyColumnOperation{
				Column: ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("9"), Changed: nullability, HasChanged: true, OmitNullBackfill: true,
			},
			want: false,
		},
		{
			name: "no default declared",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("c", "INTEGER").SetNotNull(), Changed: nullability, HasChanged: true},
			want: false,
		},
		{
			name: "nullability not among the changes",
			op: &ast.ModifyColumnOperation{
				Column: ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("9"), Changed: ast.ColumnProperties{Default: true}, HasChanged: true,
			},
			want: false,
		},
		{
			name: "NOT NULL dropped",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("c", "INTEGER").SetDefault("9"), Changed: nullability, HasChanged: true},
			want: false,
		},
		{name: "no column", op: &ast.ModifyColumnOperation{Changed: nullability, HasChanged: true}, want: false},
		{name: "no operation", op: nil, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := notnullfill.FillsNullRows(test.op)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
