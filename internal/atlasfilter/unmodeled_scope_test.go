package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasfilter"
)

// unmodeledScopeFixture is a MySQL database with two tables, one of them
// partitioned, an event, and a record of every event of a schema, which a read
// writes when the account cannot list them.
func unmodeledScopeFixture() *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "public"}, {Name: "archive"}},
		Tables: []catalog.Table{
			{Name: "sales", Columns: []catalog.Column{{Name: "id"}}},
			{Name: "other", Columns: []catalog.Column{{Name: "id"}}},
		},
		UnmodeledObjects: []catalog.UnmodeledObject{
			{Kind: "table partitioning", Name: "sales", Table: "sales", Reason: "not modeled"},
			{Kind: "event", Name: "tick", Reason: "not modeled"},
			{Kind: "event", Schema: "archive", Reason: "the server refused to list them"},
		},
	}
}

// TestScopeDatabase_ReportsOnlyTheUnmodeledObjectsTheRunSelects_HappyPath pins
// that a report does not name an object the run's selection left out: the
// partitioning of a table that left, an event a selector names, and anything
// an include selector does not reach.
func TestScopeDatabase_ReportsOnlyTheUnmodeledObjectsTheRunSelects_HappyPath(t *testing.T) {
	sales := catalog.UnmodeledObject{Kind: "table partitioning", Name: "sales", Table: "sales", Reason: "not modeled"}
	tick := catalog.UnmodeledObject{Kind: "event", Name: "tick", Reason: "not modeled"}
	archive := catalog.UnmodeledObject{Kind: "event", Schema: "archive", Reason: "the server refused to list them"}
	tests := []struct {
		name    string
		exclude []string
		include []string
		schemas []string
		want    []catalog.UnmodeledObject
	}{
		{name: "no selector keeps every record", exclude: []string{"nosuch"}, want: []catalog.UnmodeledObject{sales, tick, archive}},
		{name: "an excluded table takes its partitioning", exclude: []string{"sales"}, want: []catalog.UnmodeledObject{tick, archive}},
		{name: "a typed table selector takes its partitioning", exclude: []string{"sales[type=table]"}, want: []catalog.UnmodeledObject{tick, archive}},
		{name: "an excluded event leaves", exclude: []string{"tick"}, want: []catalog.UnmodeledObject{sales, archive}},
		{name: "a qualified typed event selector", exclude: []string{"*.tick[type=event]"}, want: []catalog.UnmodeledObject{sales, archive}},
		{name: "an excluded schema takes its record of every event", exclude: []string{"archive"}, want: []catalog.UnmodeledObject{sales, tick}},
		{name: "an include of another table leaves both objects", include: []string{"other"}, want: []catalog.UnmodeledObject{archive}},
		{name: "an include of the table keeps its partitioning", include: []string{"sales"}, want: []catalog.UnmodeledObject{sales, archive}},
		{name: "an include of the table and the event", include: []string{"other", "tick"}, want: []catalog.UnmodeledObject{tick, archive}},
		{name: "a schema universe without the archive", schemas: []string{"public"}, want: []catalog.UnmodeledObject{sales, tick}},
		{name: "an excluded default schema keeps an object in no schema", exclude: []string{"public"}, want: []catalog.UnmodeledObject{tick, archive}},
		{name: "a schema universe of the archive keeps an object in no schema", schemas: []string{"archive"}, want: []catalog.UnmodeledObject{tick, archive}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ScopeDatabase(unmodeledScopeFixture(), atlasfilter.Scope{
				Selections: ownerSelections(), Exclude: test.exclude, Include: test.include, Schemas: test.schemas,
				DefaultSchema: "public",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(got.UnmodeledObjects, qt.DeepEquals, test.want)
		})
	}
}

// TestScopeDatabase_ReportsOnlyTheUnmodeledObjectsTheRunSelects_FailurePath
// pins that keeping an object Ptah does not model is not a match: an include
// selector that names only an event has selected nothing to compare.
func TestScopeDatabase_ReportsOnlyTheUnmodeledObjectsTheRunSelects_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ScopeDatabase(unmodeledScopeFixture(), atlasfilter.Scope{
		Selections: ownerSelections(), Include: []string{"tick"}, DefaultSchema: "public",
	})

	var empty *atlasfilter.EmptySelectionError
	c.Assert(err, qt.ErrorAs, &empty)
	c.Assert(empty.Selectors, qt.DeepEquals, []string{"tick"})
	c.Assert(got.Tables, qt.HasLen, 0)
}
