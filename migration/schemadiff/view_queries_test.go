package schemadiff_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// exactViewQueries records each request and calls two queries one query when
// their words match, case included, as on an engine whose names keep their
// case. With err it fails, and with short it answers one pair too few.
type exactViewQueries struct {
	requests []schemacomparison.ViewQueriesRequest
	err      error
	short    bool
}

func (r *exactViewQueries) EquivalentViewQueries(_ context.Context, request schemacomparison.ViewQueriesRequest) ([]bool, error) {
	r.requests = append(r.requests, request)
	if r.err != nil {
		return nil, r.err
	}
	answers := make([]bool, len(request.Pairs))
	for i, pair := range request.Pairs {
		answers[i] = strings.Join(strings.Fields(pair.Declared), " ") == strings.Join(strings.Fields(pair.Held), " ")
	}
	if r.short {
		answers = answers[1:]
	}
	return answers, nil
}

// viewQuerySchemas declare view cased, whose query differs from the held one
// in the case of a column name alone, view same, whose query the database
// holds as declared, view fresh, which the database does not hold, and
// materialized view totals, whose held query carries no data clause.
func viewQuerySchemas() (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "price", Type: "BIGINT", Nullable: true}},
		Views: []schemamodel.View{{Name: "cased", Body: "SELECT Price FROM items"}, {Name: "same", Body: "SELECT price FROM items"},
			{Name: "fresh", Body: "SELECT price FROM items"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "totals", Body: "SELECT count(*) FROM items WITH NO DATA"}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "items", Type: "TABLE", Columns: []catalog.Column{
			{Name: "price", DataType: "BIGINT", ColumnType: "BIGINT", IsNullable: "YES", OrdinalPosition: 1},
		}}},
		Views:    []catalog.View{{Name: "cased", Body: "SELECT price FROM items"}, {Name: "same", Body: "SELECT price FROM items"}},
		MatViews: []catalog.MaterializedView{{Name: "totals", Body: "SELECT count(*) FROM items"}},
	}
	return desired, current
}

// A target that decides view queries is asked, in one batch, about each
// declared view and materialized view the database holds under its name: the
// held query as read, and the declared query, a materialized view's without
// its data clause. A view only one side has is not asked about.
func TestCompareAsksTheTargetAboutViewQueries_HappyPath(t *testing.T) {
	c := qt.New(t)
	queries := &exactViewQueries{}
	desired, current := viewQuerySchemas()

	_, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "custom", rulesRuntime(schemacomparison.Rules{ViewQueries: queries}))

	c.Assert(err, qt.IsNil)
	c.Assert(queries.requests, qt.HasLen, 1)
	c.Assert(queries.requests[0].Target, qt.Equals, "custom")
	c.Assert(queries.requests[0].Pairs, qt.DeepEquals, []schemacomparison.ViewQueryPair{
		{Kind: objectidentity.KindMatView, Name: "totals", Held: "SELECT count(*) FROM items", Declared: "SELECT count(*) FROM items"},
		{Kind: objectidentity.KindView, Name: "cased", Held: "SELECT price FROM items", Declared: "SELECT Price FROM items"},
		{Kind: objectidentity.KindView, Name: "same", Held: "SELECT price FROM items", Declared: "SELECT price FROM items"},
	})
}

// The comparison reads the target's answers alone: a view whose query the
// target calls another query is changed, even where the shared reading, which
// folds case, calls the two one query. A target that registers no owner is
// read by the shared reading, which calls them one query.
func TestCompareReadsTheTargetsViewQueryAnswers(t *testing.T) {
	for _, test := range []struct {
		name            string
		rules           schemacomparison.Rules
		wantModified    []string
		wantMatModified int
	}{
		{name: "an owner that keeps case", rules: schemacomparison.Rules{ViewQueries: &exactViewQueries{}}, wantModified: []string{"cased"}},
		{name: "no owner", rules: schemacomparison.Rules{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := viewQuerySchemas()

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "custom", rulesRuntime(test.rules))

			c.Assert(err, qt.IsNil)
			var modified []string
			for _, view := range diff.ViewsModified {
				modified = append(modified, view.ViewName)
			}
			c.Assert(modified, qt.DeepEquals, test.wantModified)
			c.Assert(diff.MaterializedViewsModified, qt.HasLen, test.wantMatModified)
			c.Assert(diff.ViewsAdded, qt.HasLen, 1)
		})
	}
}

// A target that cannot answer, or answers fewer pairs than it was asked, fails
// the comparison, which decides no view by another rule.
func TestCompareAsksTheTargetAboutViewQueries_FailurePath(t *testing.T) {
	t.Run("the target refuses", func(t *testing.T) {
		c := qt.New(t)
		refused := errors.New("the target cannot read this query")
		desired, current := viewQuerySchemas()

		diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "custom",
			rulesRuntime(schemacomparison.Rules{ViewQueries: &exactViewQueries{err: refused}}))

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, `compare schemas: the target cannot read this query`)
		c.Assert(diff, qt.IsNil)
	})
	t.Run("the target answers too few pairs", func(t *testing.T) {
		c := qt.New(t)
		desired, current := viewQuerySchemas()

		diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "custom",
			rulesRuntime(schemacomparison.Rules{ViewQueries: &exactViewQueries{short: true}}))

		c.Assert(err, qt.ErrorMatches, `compare schemas: the target answered 2 of 3 view query comparisons`)
		c.Assert(diff, qt.IsNil)
	})
}
