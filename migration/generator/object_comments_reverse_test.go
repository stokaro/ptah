package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback puts back the comment the forward change replaced, and removes
// one the forward change added (stokaro/ptah#3627). The reverse builder is a
// struct literal, and a literal is silent about a field it omits: a comment
// change left out of it rolls back to nothing at all.
func TestPlanBidirectionalSchemaDiff_ObjectCommentRollsBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{
		{Kind: difftypes.CommentedView, Name: "app.v", Current: "old", Desired: "new"},
		{Kind: difftypes.CommentedDomain, Name: "app.d", Current: "", Desired: "added"},
		{Kind: difftypes.CommentedTrigger, Name: "tg", Table: "app.t", Current: "old", Desired: "new"},
		{Kind: difftypes.CommentedFunction, Name: "app.f", Arguments: new("a integer"), Current: "old", Desired: "new"},
	}}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &schemamodel.Database{},
		CurrentSchema: &catalog.Database{},
		Dialect:       platform.Postgres,
		Capabilities:  capability.Postgres18(),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.ObjectCommentsChanged, qt.DeepEquals, []difftypes.ObjectCommentChange{
		{Kind: difftypes.CommentedView, Name: "app.v", Current: "new", Desired: "old"},
		{Kind: difftypes.CommentedDomain, Name: "app.d", Current: "added", Desired: ""},
		// A trigger keeps its table and a routine its arguments: without
		// them the rollback addresses no object.
		{Kind: difftypes.CommentedTrigger, Name: "tg", Table: "app.t", Current: "new", Desired: "old"},
		{Kind: difftypes.CommentedFunction, Name: "app.f", Arguments: new("a integer"), Current: "new", Desired: "old"},
	})
	c.Assert(diff.ObjectCommentsChanged[0].Desired, qt.Equals, "new",
		qt.Commentf("the reversal must not write through to the forward diff"))
}
