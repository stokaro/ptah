package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// An index the comparison adds with a note, for a foreign key the declaration
// does not show, gets the note as a comment above the statement that adds it
// and drops the index it takes over from. An addition without a note gets no
// comment.
func TestPlanner_WritesTheNoteOfAnIndexAddition(t *testing.T) {
	const note = "Add index kids_parent_fk for foreign key kids_parent_fk, which an exclude selector keeps out of scope"
	tests := []struct {
		name, note string
		want       []ast.Node
	}{
		{name: "with a note", note: note, want: []ast.Node{ast.NewComment(note)}},
		{name: "without one", want: make([]ast.Node, 0)},
	}
	for _, planner := range mysqlFamilyPlannerCases() {
		for _, test := range tests {
			t.Run(planner.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				diff := &difftypes.SchemaDiff{
					IndexesAdded: difftypes.IndexChanges{{
						TableName: "kids",
						Index:     schemamodel.Index{Name: "kids_parent_fk", TableName: "kids", Fields: []string{"parent_id"}},
						Replaces:  []difftypes.IndexRef{{Name: "kids_parent_idx", TableName: "kids"}},
						Note:      test.note,
					}},
					IndexesRemoved: []difftypes.IndexRef{{Name: "kids_parent_idx", TableName: "kids"}},
				}

				nodes, err := planner.planner.GenerateMigrationAST(context.Background(), must.Must(builtin.New()),
					withDeclaredObjects(diff, &schemamodel.Database{}))

				c.Assert(err, qt.IsNil)
				c.Assert(nodes, qt.HasLen, len(test.want)+1)
				c.Assert(nodes[:len(test.want)], qt.DeepEquals, test.want)
				table, replacement := replacedIndex(c, nodes[len(test.want)])
				c.Assert(table, qt.Equals, "kids")
				c.Assert(replacement.Name, qt.Equals, "kids_parent_fk")
			})
		}
	}
}
