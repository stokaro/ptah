package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
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
			name:    "a domain",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedDomain, Name: "app.d", Current: "old", Desired: "new"},
			want:    `COMMENT ON DOMAIN "app"."d" IS 'new';`,
			exclude: "DOMAIN \"app\".\"d\" AS",
		},
		{
			name:    "a composite type",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedCompositeType, Name: "app.c", Current: "old", Desired: "new"},
			want:    `COMMENT ON TYPE "app"."c" IS 'new';`,
			exclude: "TYPE \"app\".\"c\" AS",
		},
		{
			name:    "a range type",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedRangeType, Name: "app.r", Current: "old", Desired: "new"},
			want:    `COMMENT ON TYPE "app"."r" IS 'new';`,
			exclude: "TYPE \"app\".\"r\" AS",
		},
		{
			// An extension's name is database-wide and never split on a dot.
			name:    "an extension",
			change:  difftypes.ObjectCommentChange{Kind: difftypes.CommentedExtension, Name: "odd.name", Current: "old", Desired: "new"},
			want:    `COMMENT ON EXTENSION "odd.name" IS 'new';`,
			exclude: "CREATE EXTENSION",
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

// A domain whose base type changed is dropped and created again, which takes
// its comment with it. The recreation writes the declared comment, and the
// comment transition adds nothing beside it.
func TestPlanner_ObjectCommentOnARecreatedDomain(t *testing.T) {
	tests := []struct {
		name    string
		current string
		desired string
		want    []string
	}{
		{name: "the comment changed", current: "old", desired: "new", want: []string{`COMMENT ON DOMAIN "app"."d" IS 'new';`}},
		{name: "the comment kept", current: "same", desired: "same", want: []string{`COMMENT ON DOMAIN "app"."d" IS 'same';`}},
		{name: "the comment removed", current: "old", desired: "", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			domain := schemamodel.Domain{Name: "d", Schema: "app", BaseType: "bigint", Comment: test.desired}
			diff := &difftypes.SchemaDiff{
				DomainsModified: []difftypes.DomainDiff{{
					DomainName: "app.d", Changes: map[string]string{"type": "integer -> bigint"},
					CurrentBaseType: "integer", Desired: domain,
				}},
				ObjectCommentsChanged: objectCommentChanges(difftypes.CommentedDomain, "app.d", test.current, test.desired),
			}

			sql := renderedPlan(c, diff, &schemamodel.Database{Domains: []schemamodel.Domain{domain}})

			c.Assert(sql, qt.Contains, `CREATE DOMAIN "app"."d" AS bigint`)
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
