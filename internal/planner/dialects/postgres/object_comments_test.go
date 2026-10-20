package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
)

// A comment that is all that differs is set in place: one COMMENT ON the
// object, and neither a drop nor a create (stokaro/ptah#3627).
func TestPlanner_ObjectCommentChangeIsSetInPlace(t *testing.T) {
	tests := []struct {
		name    string
		change  difftypes.ObjectCommentChange
		want    string
		exclude string
	}{
		{
			name:    "a view",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedView, Name: "app.v", Current: "old", Desired: "new"},
			want:    `COMMENT ON VIEW "app"."v" IS 'new';`,
			exclude: "VIEW \"app\".\"v\" AS",
		},
		{
			name:    "a sequence",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedSequence, Name: "app.s", Current: "old", Desired: "new"},
			want:    `COMMENT ON SEQUENCE "app"."s" IS 'new';`,
			exclude: "SEQUENCE \"app\".\"s\";",
		},
		{
			name:    "an enum type",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedEnumType, Name: "app.e", Current: "old", Desired: "new"},
			want:    `COMMENT ON TYPE "app"."e" IS 'new';`,
			exclude: "ENUM",
		},
		{
			name: "a function, by the arguments the server recorded",
			change: difftypes.ObjectCommentChange{
				Kind: difftypes.CommentedFunction, Name: "app.f", Arguments: new("a integer"), Current: "old", Desired: "new",
			},
			want:    `COMMENT ON FUNCTION "app"."f"(a integer) IS 'new';`,
			exclude: "CREATE",
		},
		{
			name: "a procedure",
			change: difftypes.ObjectCommentChange{
				Kind: difftypes.CommentedProcedure, Name: "app.p", Arguments: new("IN a integer"), Current: "old", Desired: "new",
			},
			want:    `COMMENT ON PROCEDURE "app"."p"(IN a integer) IS 'new';`,
			exclude: "CREATE",
		},
		{
			name:    "a materialized view",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedMatView, Name: "app.m", Current: "old", Desired: "new"},
			want:    `COMMENT ON MATERIALIZED VIEW "app"."m" IS 'new';`,
			exclude: "CREATE",
		},
		{
			name: "a trigger",
			change: difftypes.ObjectCommentChange{
				Kind: difftypes.CommentedTrigger, Name: "tg", Table: "app.t", Current: "old", Desired: "new",
			},
			want:    `COMMENT ON TRIGGER "tg" ON "app"."t" IS 'new';`,
			exclude: "CREATE",
		},
		{
			name: "a policy",
			change: difftypes.ObjectCommentChange{
				Kind: difftypes.CommentedPolicy, Name: "pol", Table: "app.t", Current: "old", Desired: "new",
			},
			want:    `COMMENT ON POLICY "pol" ON "app"."t" IS 'new';`,
			exclude: "CREATE",
		},
		{
			name:    "a comment removed",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedView, Name: "app.v", Current: "old", Desired: ""},
			want:    `COMMENT ON VIEW "app"."v" IS NULL;`,
			exclude: "IS ''",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := renderedPlan(c,
				&difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{test.change}},
				&schemamodel.Database{},
			)

			c.Assert(sql, qt.Contains, test.want)
			c.Assert(strings.Count(sql, "COMMENT ON"), qt.Equals, 1, qt.Commentf("%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), test.exclude)
			c.Assert(sql, qt.Not(qt.Contains), "DROP")
		})
	}
}

// commentedView is the view the replace and recreate rows below declare.
func commentedView(body, comment string) schemamodel.View {
	return schemamodel.View{Name: "app.v", Body: body, Comment: comment}
}

// A view the plan replaces in place keeps the comment it had, so a changed
// comment is written once and a removed one is cleared. A view the plan drops
// and creates again starts with none, so a removed comment needs nothing and a
// kept one is written with the view.
func TestPlanner_ObjectCommentOnAReplacedOrRecreatedView(t *testing.T) {
	tests := []struct {
		name     string
		previous string
		body     string
		current  string
		desired  string
		want     []string
	}{
		{
			name:     "replaced, the comment changed",
			previous: "SELECT a FROM app.t", body: "SELECT a, b FROM app.t",
			current: "old", desired: "new",
			want: []string{`COMMENT ON VIEW "app"."v" IS 'new';`},
		},
		{
			name:     "replaced, the comment removed",
			previous: "SELECT a FROM app.t", body: "SELECT a, b FROM app.t",
			current: "old", desired: "",
			want: []string{`COMMENT ON VIEW "app"."v" IS NULL;`},
		},
		{
			name:     "replaced, the comment kept",
			previous: "SELECT a FROM app.t", body: "SELECT a, b FROM app.t",
			current: "same", desired: "same",
			want: []string{`COMMENT ON VIEW "app"."v" IS 'same';`},
		},
		{
			name:     "recreated, the comment changed",
			previous: "SELECT a, b FROM app.t", body: "SELECT b FROM app.t",
			current: "old", desired: "new",
			want: []string{`COMMENT ON VIEW "app"."v" IS 'new';`},
		},
		{
			name:     "recreated, the comment removed",
			previous: "SELECT a, b FROM app.t", body: "SELECT b FROM app.t",
			current: "old", desired: "",
			want: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			view := commentedView(test.body, test.desired)
			diff := &difftypes.SchemaDiff{
				ViewsModified: []difftypes.ViewDiff{{
					ViewName: "app.v", PreviousBody: test.previous, Desired: view,
					Changes: map[string]string{"body": test.previous + " -> " + test.body},
				}},
			}
			// The comparison records a transition only where the comments
			// differ.
			diff.ObjectCommentsChanged = objectCommentChanges(difftypes.CommentedView, "app.v", test.current, test.desired)

			sql := renderedPlan(c, diff, &schemamodel.Database{Views: []schemamodel.View{view}})

			c.Assert(commentStatements(sql), qt.DeepEquals, test.want, qt.Commentf("%s", sql))
		})
	}
}

// objectCommentChanges is what the comparison reports for one object: a
// transition where the comments differ, and nothing where they agree.
func objectCommentChanges(kind difftypes.CommentedObjectKind, name, current, desired string) []difftypes.ObjectCommentChange {
	if current == desired {
		return nil
	}
	return []difftypes.ObjectCommentChange{{Kind: kind, Name: name, Current: current, Desired: desired}}
}

// commentStatements lists every COMMENT ON line of a rendered plan.
func commentStatements(sql string) []string {
	var statements []string
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.HasPrefix(line, "COMMENT ON") {
			statements = append(statements, line)
		}
	}
	return statements
}
