package planshape_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemacapture"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planshape"
)

// setComment is a COMMENT ON for table, or for one of its columns when column is not
// empty.
func setComment(table, column, comment string) *ast.AlterTableNode {
	return alter(table, &ast.SetCommentOperation{Column: column, Comment: comment})
}

// commentedTasks is a table holding id, title, old and body, to which a plan adds due
// and owner.
func commentedTasks() *difftypes.SchemaDiff {
	held := make([]catalog.Column, 0, 4)
	for _, name := range []string{"id", "title", "old", "body"} {
		held = append(held, catalog.Column{Name: name})
	}
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:    "tasks",
		Current:      schemacapture.TableObservation{Table: catalog.Table{Name: "tasks", Columns: held}},
		ColumnsAdded: difftypes.ColumnChanges{{Name: "due"}, {Name: "owner"}},
	}}}
}

// TestApply_WritesATablesCommentsAfterItsIndexes_HappyPath pins the comments of a
// table the plan changes where the community binary writes them on PostgreSQL: after
// the table's one ALTER TABLE and its new indexes, the table's own comment first, then
// its columns' in the order the table holds them. A comment the planner wrote between
// two of the table's column changes would split that ALTER TABLE.
func TestApply_WritesATablesCommentsAfterItsIndexes_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		setComment("tasks", "body", "b2"),
		alter("tasks", addColumn("due")),
		setComment("tasks", "", "t2"),
		alter("tasks", modifyColumn("title", "text")),
		setComment("tasks", "title", "v2"),
		alter("tasks", dropColumn("old")),
		ast.NewIndex("tasks_due", "tasks", "due"),
		alter("tasks", addColumn("owner")),
		setComment("tasks", "owner", "who"),
	}

	got := planshape.Apply(platform.Postgres, nodes, commentedTasks())

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals,
		"ALTER TABLE \"tasks\" ALTER COLUMN \"title\" TYPE text, ALTER COLUMN \"title\" DROP NOT NULL, ALTER COLUMN \"title\" DROP DEFAULT, "+
			"DROP COLUMN \"old\", ADD COLUMN \"due\" integer NULL, ADD COLUMN \"owner\" integer NULL;\n\n"+
			"CREATE INDEX \"tasks_due\" ON \"tasks\" (\"due\");\n"+
			"COMMENT ON TABLE \"tasks\" IS 't2';\n\n"+
			"COMMENT ON COLUMN \"tasks\".\"title\" IS 'v2';\n\n"+
			"COMMENT ON COLUMN \"tasks\".\"body\" IS 'b2';\n\n"+
			"COMMENT ON COLUMN \"tasks\".\"owner\" IS 'who';\n\n")
}

// TestApply_LeavesACommentWhereItCannotMove_HappyPath is a comment the pass keeps in
// the planner's order. On PostgreSQL a column's comment the planner wrote ahead of a
// change to that column holds the change behind it, so the ALTER TABLE stays split
// around the comment. The MySQL family writes a comment into the column or the table
// options, so its comment statements stay where the planner put them.
func TestApply_LeavesACommentWhereItCannotMove_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		nodes   []ast.Node
		want    string
	}{
		{
			name:    "a column's comment ahead of a change to the column on PostgreSQL",
			dialect: platform.Postgres,
			nodes: []ast.Node{
				alter("tasks", addColumn("due")),
				setComment("tasks", "title", "v2"),
				alter("tasks", modifyColumn("title", "text")),
			},
			want: "ALTER TABLE \"tasks\" ADD COLUMN \"due\" integer NULL;\n\n" +
				"COMMENT ON COLUMN \"tasks\".\"title\" IS 'v2';\n\n" +
				"ALTER TABLE \"tasks\" ALTER COLUMN \"title\" TYPE text, ALTER COLUMN \"title\" DROP NOT NULL, ALTER COLUMN \"title\" DROP DEFAULT;\n\n",
		},
		{
			name:    "a table's comment on MySQL",
			dialect: platform.MySQL,
			nodes: []ast.Node{
				setComment("tasks", "", "t2"),
				alter("tasks", addColumn("due")),
			},
			want: "ALTER TABLE `tasks` COMMENT='t2';\n\n" +
				"ALTER TABLE `tasks` ADD COLUMN `due` int NULL;\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, test.nodes, commentedTasks())

			c.Assert(must.Must(builtin.RenderSQL(test.dialect, got...)), qt.Equals, test.want)
		})
	}
}
