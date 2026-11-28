package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasfilter"
)

// keyIntoParents is a read of app holding parents and kids, whose key
// kids_parent_fk references parents and stands on the index the server built
// under its name.
func keyIntoParents() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Schema: "app", Name: "parents", Columns: []catalog.Column{{Name: "id", IsPrimaryKey: true}}},
			{Schema: "app", Name: "kids", Columns: []catalog.Column{{Name: "id", IsPrimaryKey: true}, {Name: "parent_id"}, {Name: "owner_id"}}},
		},
		Indexes: []catalog.Index{
			{Schema: "app", TableName: "kids", Name: "kids_parent_fk", Columns: []string{"parent_id"}},
			{Schema: "app", TableName: "kids", Name: "kids_owner_idx", Columns: []string{"owner_id"}},
		},
		Constraints: []catalog.Constraint{
			{
				Schema: "app", TableName: "kids", Name: "kids_parent_fk", Type: "FOREIGN KEY",
				ColumnNames: []string{"parent_id"}, ForeignTable: new("parents"), ForeignColumns: []string{"id"},
			},
		},
	}
}

// An exclusion that takes a table out names it on the read, and a key of a
// kept table into it leaves with the index the server built for it. In scope
// alone, that index reads as one the declaration leaves out, and every plan
// drops and rebuilds it under the key's name.
func TestExcludeDatabaseScopeReport_RecordsATableTakenOut(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{
		Selections: ownerSelections(), Exclude: []string{"parents"}, DefaultSchema: "app",
		Dialect: "mysql", ForeignKeyIndexes: namedAfterKey{},
	}

	current, report, err := atlasfilter.ExcludeDatabaseScopeReport(keyIntoParents(), scope)

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.IsNil)
	c.Assert(current.TablesOutOfScope, qt.DeepEquals, []string{"app.parents"})
	c.Assert(current.Constraints, qt.HasLen, 0)
	c.Assert(indexNames(current.Indexes), qt.DeepEquals, []string{"kids_owner_idx"})
	c.Assert(constraintNames(current.OutOfScope.Constraints), qt.DeepEquals, []string{"kids_parent_fk"})
	c.Assert(indexNames(current.OutOfScope.Indexes), qt.DeepEquals, []string{"kids_parent_fk"})
}

// A second filter keeps the tables the first took out, and a selector that
// names one again is matched: the database still holds the table, as the
// second pass of a positive selection finds it.
func TestExcludeDatabaseScopeReport_KeepsATableAnEarlierFilterTookOut(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{"parents"}, DefaultSchema: "app", Dialect: "mysql"}
	first, _, err := atlasfilter.ExcludeDatabaseScopeReport(keyIntoParents(), scope)
	c.Assert(err, qt.IsNil)

	second, report, err := atlasfilter.ExcludeDatabaseScopeReport(first, scope)

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.IsNil)
	c.Assert(second.TablesOutOfScope, qt.DeepEquals, []string{"app.parents"})
}
