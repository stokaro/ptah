package schemalineage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemalineage"
)

// Column lineage answers "what breaks if I drop this column", which Ptah could
// only answer after the drop. The edges come from view bodies the schema model
// already holds (stokaro/ptah#1712).

// schemaWith builds a database with one users table and the given views.
func schemaWith(views ...goschema.View) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id"},
			{StructName: "User", Name: "email"},
			{StructName: "User", Name: "created_at"},
		},
		Views: views,
	}
}

func TestDeriveResolvesAPlainColumnList(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(goschema.View{Name: "active_users", Body: "SELECT id, email FROM users"})

	result := schemalineage.Derive(db)

	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Edges, qt.DeepEquals, []schemalineage.Edge{
		{FromTable: "users", FromColumn: "email", ToView: "active_users", ToColumn: "email"},
		{FromTable: "users", FromColumn: "id", ToView: "active_users", ToColumn: "id"},
	})
}

// TestDeriveFollowsAnAliasToItsSource is the point of column-level lineage: the
// view column and the base column have different names, so a table-level edge
// could not answer which base column feeds which output.
func TestDeriveFollowsAnAliasToItsSource(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(goschema.View{Name: "v", Body: "SELECT email AS contact FROM users"})

	result := schemalineage.Derive(db)

	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Edges, qt.DeepEquals, []schemalineage.Edge{
		{FromTable: "users", FromColumn: "email", ToView: "v", ToColumn: "contact"},
	})
}

// TestDeriveExpandsAStarProjection covers the shape that names no columns.
//
// It resolves because the table declares them, which is why the schema model
// is the right place for this analysis rather than the SQL text alone.
func TestDeriveExpandsAStarProjection(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(goschema.View{Name: "v", Body: "SELECT * FROM users"})

	result := schemalineage.Derive(db)

	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Edges, qt.HasLen, 3)
	c.Assert(result.Edges[0], qt.Equals, schemalineage.Edge{
		FromTable: "users", FromColumn: "created_at", ToView: "v", ToColumn: "created_at",
	})
}

// TestDeriveAttributesEveryColumnAnExpressionNames keeps a computed column's
// dependencies. Dropping either input breaks the view, so both are edges.
func TestDeriveAttributesEveryColumnAnExpressionNames(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(goschema.View{
		Name: "v",
		Body: "SELECT coalesce(email, id) AS who FROM users",
	})

	result := schemalineage.Derive(db)

	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Edges, qt.DeepEquals, []schemalineage.Edge{
		{FromTable: "users", FromColumn: "email", ToView: "v", ToColumn: "who"},
		{FromTable: "users", FromColumn: "id", ToView: "v", ToColumn: "who"},
	})
}

// TestDeriveResolvesAQualifiedColumnThroughAnAlias covers the table alias.
func TestDeriveResolvesAQualifiedColumnThroughAnAlias(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(goschema.View{Name: "v", Body: "SELECT u.email AS mail FROM users u"})

	result := schemalineage.Derive(db)

	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Edges, qt.DeepEquals, []schemalineage.Edge{
		{FromTable: "users", FromColumn: "email", ToView: "v", ToColumn: "mail"},
	})
}

// TestDeriveReportsAJoinAsUndecidedRatherThanGuessing is the reason Undecided
// exists.
//
// With two sources in scope an unqualified column cannot be attributed, and
// attributing it to the wrong table is worse than saying so: a caller asking
// "is it safe to drop this column" would be told yes about a column a view
// reads.
func TestDeriveReportsAJoinAsUndecidedRatherThanGuessing(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(goschema.View{
		Name: "v",
		Body: "SELECT id FROM users JOIN orders ON orders.user_id = users.id",
	})

	result := schemalineage.Derive(db)

	c.Assert(result.Edges, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].View, qt.Equals, "v")
	c.Assert(result.Undecided[0].Reason, qt.Contains, "more than one source")
}

// TestDeriveReportsASubquerySourceAsUndecided covers the other unresolvable
// source shape.
func TestDeriveReportsASubquerySourceAsUndecided(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(goschema.View{Name: "v", Body: "SELECT id FROM (SELECT id FROM users) s"})

	result := schemalineage.Derive(db)

	c.Assert(result.Edges, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "subquery")
}

// TestDeriveDoesNotMistakeASubqueryFromForTheSourcesOwn is why FROM detection
// tracks parenthesis depth. Without it the inner table would be read as the
// source and every edge would name the wrong table.
func TestDeriveDoesNotMistakeASubqueryFromForTheSourcesOwn(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(goschema.View{
		Name: "v",
		Body: "SELECT (SELECT max(id) FROM audit) AS latest, email FROM users",
	})

	result := schemalineage.Derive(db)

	c.Assert(result.Undecided, qt.HasLen, 0)
	// Every edge names users, never audit.
	for _, edge := range result.Edges {
		c.Assert(edge.FromTable, qt.Equals, "users")
	}
	c.Assert(result.Edges, qt.Contains, schemalineage.Edge{
		FromTable: "users", FromColumn: "email", ToView: "v", ToColumn: "email",
	})
}

// TestDeriveMarksMaterializedViews keeps the two kinds apart: refreshing a
// materialized view is a different operation from querying a view, so a
// consumer needs to know which it is looking at.
func TestDeriveMarksMaterializedViews(t *testing.T) {
	c := qt.New(t)
	db := schemaWith()
	db.MaterializedViews = []goschema.MaterializedView{
		{Name: "mv", Body: "SELECT id FROM users"},
	}

	result := schemalineage.Derive(db)

	c.Assert(result.Edges, qt.HasLen, 1)
	c.Assert(result.Edges[0].Materialized, qt.IsTrue)
}

// TestDeriveIsOrderedSoTwoRunsAgree keeps the output diffable. Go randomizes
// map iteration, and a lineage document that reorders itself between runs
// cannot be compared across schema versions.
func TestDeriveIsOrderedSoTwoRunsAgree(t *testing.T) {
	c := qt.New(t)
	db := schemaWith(
		goschema.View{Name: "zebra", Body: "SELECT id FROM users"},
		goschema.View{Name: "alpha", Body: "SELECT email, id FROM users"},
	)

	first := schemalineage.Derive(db)
	second := schemalineage.Derive(db)

	c.Assert(first, qt.DeepEquals, second)
	c.Assert(first.Edges[0].ToView, qt.Equals, "alpha")
	c.Assert(first.Edges[len(first.Edges)-1].ToView, qt.Equals, "zebra")
}

// TestDeriveOnAnEmptySchemaIsEmptyNotUndecided keeps "nothing to analyze" from
// reading as "could not analyze".
func TestDeriveOnAnEmptySchemaIsEmptyNotUndecided(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.Derive(&goschema.Database{})

	c.Assert(result.Edges, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}
