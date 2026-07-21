package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
)

func TestUpsertNodeBuilder(t *testing.T) {
	c := qt.New(t)

	node := ast.NewUpsert("dbo.users").
		AddInsertValue("id", "@p1").
		AddInsertValue("email", "@p2").
		SetMatchColumns("id").
		AddUpdateAssignment("email", "src.[email]").
		SetUpdatePredicate("target.[deleted_at] IS NULL").
		SetInsertPredicate("@allow_insert = 1").
		SetComment("upsert user")

	c.Assert(node.Table, qt.Equals, "dbo.users")
	c.Assert(node.InsertColumns, qt.DeepEquals, []string{"id", "email"})
	c.Assert(node.Values, qt.DeepEquals, []string{"@p1", "@p2"})
	c.Assert(node.MatchColumns, qt.DeepEquals, []string{"id"})
	c.Assert(node.UpdateAssignments, qt.DeepEquals, []ast.UpsertAssignment{
		{Column: "email", Expression: "src.[email]"},
	})
	c.Assert(node.UpdatePredicate, qt.Equals, "target.[deleted_at] IS NULL")
	c.Assert(node.InsertPredicate, qt.Equals, "@allow_insert = 1")
	c.Assert(node.Comment, qt.Equals, "upsert user")
}

func TestUpsertNodeBuilderCopiesInputSlices(t *testing.T) {
	c := qt.New(t)

	columns := []string{"id", "email"}
	values := []string{"@p1", "@p2"}
	matchColumns := []string{"id"}

	node := ast.NewUpsert("dbo.users").
		SetInsert(columns, values).
		SetMatchColumns(matchColumns...)

	columns[0] = "mutated_column"
	values[0] = "mutated_value"
	matchColumns[0] = "mutated_match"

	c.Assert(node.InsertColumns, qt.DeepEquals, []string{"id", "email"})
	c.Assert(node.Values, qt.DeepEquals, []string{"@p1", "@p2"})
	c.Assert(node.MatchColumns, qt.DeepEquals, []string{"id"})
}
