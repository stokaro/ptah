package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasfilter"
)

// TestScopeDatabaseReport_KeepsWhereTheCatalogCameFrom filters a catalog with
// an exclusion and with an inclusion, and reads back whether the result is a
// prediction. A comparison fills in the kind of a generated column declared
// without one only on a predicted catalog, so a filter that dropped the flag
// would compare a filtered schema file with itself as a change, and one that
// set it would fill in a kind a server never reported.
func TestScopeDatabaseReport_KeepsWhereTheCatalogCameFrom(t *testing.T) {
	tests := []struct {
		name      string
		predicted bool
		scope     atlasfilter.Scope
	}{
		{
			name: "an excluded table of a predicted catalog", predicted: true,
			scope: atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{"gone"}, DefaultSchema: "public", Dialect: "postgres"},
		},
		{
			name: "an excluded table of a read catalog", predicted: false,
			scope: atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{"gone"}, DefaultSchema: "public", Dialect: "postgres"},
		},
		{
			name: "an included table of a predicted catalog", predicted: true,
			scope: atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"kept"}, DefaultSchema: "public", Dialect: "postgres"},
		},
		{
			name: "an included table of a read catalog", predicted: false,
			scope: atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"kept"}, DefaultSchema: "public", Dialect: "postgres"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{Predicted: test.predicted, Tables: []catalog.Table{
				{Name: "kept", Schema: "public"}, {Name: "gone", Schema: "public"},
			}}

			scoped, _, err := atlasfilter.ScopeDatabaseReport(database, test.scope)

			c.Assert(err, qt.IsNil)
			c.Assert(readTableNames(scoped), qt.DeepEquals, []string{"kept"})
			c.Assert(scoped.Predicted, qt.Equals, test.predicted)
		})
	}
}
