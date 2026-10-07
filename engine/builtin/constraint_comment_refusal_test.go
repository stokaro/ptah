package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// A dialect with no statement that comments a constraint refuses the
// operation rather than writing something else in its place: a renderer that
// read only the comment would set the table's (stokaro/ptah#3678). The
// comparison never asks these dialects for one, so reaching this is a caller
// building the node by hand.
func TestRenderSQL_ConstraintCommentOperation_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: "mysql", want: `unsupported feature: mysql: this renderer has no ALTER TABLE spelling for \*ast.SetConstraintCommentOperation`},
		{dialect: "mariadb", want: `unsupported feature: mariadb: this renderer has no ALTER TABLE spelling for \*ast.SetConstraintCommentOperation`},
		{dialect: "sqlite", want: `.*unsupported alter table operation \*ast.SetConstraintCommentOperation`},
		{dialect: "sqlserver", want: `.*unsupported alter table operation \*ast.SetConstraintCommentOperation`},
		{dialect: "oracle", want: `.*unsupported alter table operation \*ast.SetConstraintCommentOperation`},
		{dialect: "clickhouse", want: `unsupported feature: clickhouse: this renderer has no ALTER TABLE spelling for \*ast.SetConstraintCommentOperation`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{
				Name:       "t",
				Operations: []ast.AlterOperation{&ast.SetConstraintCommentOperation{Constraint: "c", Comment: "x"}},
			}

			sql, err := builtin.RenderSQL(test.dialect, alter)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
