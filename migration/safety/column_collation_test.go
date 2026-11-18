package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/migration/safety"
)

// TestAssess_ACollationChange pins the reason a change of a column's collation
// alone carries, and that one made with another change is judged by the
// other change.
func TestAssess_ACollationChange(t *testing.T) {
	tests := []struct {
		name    string
		changed ast.ColumnProperties
		want    string
	}{
		{name: "the collation alone", changed: ast.ColumnProperties{Collation: true},
			want: "the collation change rebuilds every index on the column and changes how its values sort and compare"},
		{name: "the collation and the default", changed: ast.ColumnProperties{Collation: true, Default: true},
			want: "DROP DEFAULT can break writers that leave the column out"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn("c", "text").SetCollate("C")
			column.Nullable = true

			got := safety.Assess([]ast.Node{modifyColumn(&ast.ModifyColumnOperation{
				Column: column, Changed: test.changed, HasChanged: true,
			})})

			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Severity, qt.Equals, safety.Warning)
			c.Assert(got[0].Reason, qt.Equals, test.want)
		})
	}
}
