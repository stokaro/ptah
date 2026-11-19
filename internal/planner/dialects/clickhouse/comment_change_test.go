package clickhouse_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/clickhouse"
)

// A comment change is a statement of its own: MODIFY COLUMN leaves the comment
// a column holds, so a change planned only as one never reaches the server.
// A column whose comment is its only change takes no MODIFY COLUMN, and a
// column with another change takes both.
func TestGenerateMigrationAST_PlansACommentChangeAsItsOwnStatement(t *testing.T) {
	c := qt.New(t)
	table := schemamodel.Table{StructName: "Note", Name: "notes"}
	diff := capturedColumnFixture(table)
	diff.CommentChange = &difftypes.CommentChange{Current: "old", Desired: "it's new"}
	diff.ColumnsModified = []difftypes.ColumnDiff{
		{
			ColumnName:    "a",
			Desired:       schemamodel.Field{StructName: "Note", Name: "a", Type: "String", Comment: `c\d`},
			Changes:       make(map[string]string),
			CommentChange: &difftypes.CommentChange{Current: "old", Desired: `c\d`},
		},
		{
			ColumnName:    "b",
			Desired:       schemamodel.Field{StructName: "Note", Name: "b", Type: "BIGINT"},
			Changes:       map[string]string{"type": "Int32"},
			CommentChange: &difftypes.CommentChange{Current: "gone"},
		},
	}
	schemaDiff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{diff}}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{table}}
	runtime := must.Must(builtin.New())

	nodes, err := clickhouse.New().GenerateMigrationAST(context.Background(), runtime, withDeclaredTables(schemaDiff, desired))
	c.Assert(err, qt.IsNil)
	got, err := builtin.RenderSQL("clickhouse", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "ALTER TABLE notes MODIFY COMMENT 'it''s new';\n"+
		"ALTER TABLE notes COMMENT COLUMN a 'c\\\\d';\n"+
		"ALTER TABLE notes COMMENT COLUMN b '';\n"+
		"ALTER TABLE notes MODIFY COLUMN b Int64;\n")
}
