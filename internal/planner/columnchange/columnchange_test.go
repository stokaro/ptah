package columnchange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/planner/columnchange"
	"ptah.run/migration/schemadiff/difftypes"
)

// Properties reads each comparator key an ALTER COLUMN clause carries, a
// default under either of its two keys, and leaves out the changes no such
// clause carries.
func TestProperties(t *testing.T) {
	tests := []struct {
		name    string
		changes map[string]string
		want    ast.ColumnProperties
	}{
		{name: "a type", changes: map[string]string{"type": "int -> bigint"}, want: ast.ColumnProperties{Type: true}},
		{name: "nullability", changes: map[string]string{"nullable": "true -> false"}, want: ast.ColumnProperties{Nullability: true}},
		{name: "a literal default", changes: map[string]string{"default": "1 -> 2"}, want: ast.ColumnProperties{Default: true}},
		{name: "an expression default", changes: map[string]string{"default_expr": " -> now()"}, want: ast.ColumnProperties{Default: true}},
		{name: "UNIQUE and PRIMARY KEY", changes: map[string]string{"unique": "false -> true", "primary_key": "false -> true"}, want: ast.ColumnProperties{}},
		{
			name:    "all three at once",
			changes: map[string]string{"type": "int -> bigint", "nullable": "true -> false", "default_expr": " -> 1"},
			want:    ast.ColumnProperties{Type: true, Nullability: true, Default: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnchange.Properties(difftypes.ColumnDiff{ColumnName: "c", Changes: test.changes})

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
