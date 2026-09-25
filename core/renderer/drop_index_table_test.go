package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// MySQL and MariaDB name an index only inside its table, so a DROP INDEX node
// with no table has no statement on either. The node is refused rather than
// rendered as `DROP INDEX `+"`app.i`"+`;`, which both servers reject and which
// quotes the schema into the index name.
func TestRenderSQL_DropIndexWithoutTable_FailurePath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, &ast.DropIndexNode{Name: "app.i"})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// The control for the refusal above: the same drop with its table renders on
// every target that requires one, so the refusal is about the missing table and
// not about the node.
func TestRenderSQL_DropIndexWithTable_HappyPath(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: "mysql", want: "DROP INDEX `i` ON `app`.`t`;\n"},
		{dialect: "mariadb", want: "DROP INDEX `i` ON `app`.`t`;\n"},
		{dialect: "sqlserver", want: "DROP INDEX [i] ON [app].[t];\n"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(test.dialect, &ast.DropIndexNode{Name: "i", Table: "app.t"})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
