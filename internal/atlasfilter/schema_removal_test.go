package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasfilter"
)

func TestScopeDatabase_RecordsTheExcludedParentInsteadOfItsChildren(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}},
		Tables: []catalog.Table{{Schema: "app", Name: "t", Columns: []catalog.Column{{Name: "id"}}},
			{Schema: "app", Name: "u"}},
	}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: ownerSelections(), Exclude: []string{"app.t"}, RealmRelativePatterns: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{
		"app": {Object: "table app.t", Cause: catalog.SelectionExcluded, Pattern: "app"},
	})
	// The excluded table left with its columns, so no kept table holds them.
	c.Assert(filtered.TableRemovalBlockers, qt.IsNil)
	c.Assert(current.SchemaRemovalBlockers, qt.IsNil)
}

func TestScopeDatabase_SchemaProtectionDoesNotMutateTheRead(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}, {Name: "other"}},
		SchemaRemovalBlockers: map[string]catalog.SelectionBlocker{
			"app": {Object: "table app.t", Cause: catalog.SelectionExcluded},
		},
		Tables: []catalog.Table{{Schema: "other", Name: "u"}},
	}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: ownerSelections(), Exclude: []string{"other.u"}, RealmRelativePatterns: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{
		"app":   {Object: "table app.t", Cause: catalog.SelectionExcluded},
		"other": {Object: "table other.u", Cause: catalog.SelectionExcluded, Pattern: "other"},
	})
	c.Assert(current.SchemaRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{
		"app": {Object: "table app.t", Cause: catalog.SelectionExcluded},
	})
}

func TestScopeDatabase_SchemaProtectionKeepsDottedNamesDistinct(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app.dot"}, {Name: "app"}},
		Tables:  []catalog.Table{{Schema: "app.dot", Name: "t"}, {Schema: "app", Name: "dot.t"}},
	}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: ownerSelections(), Include: []string{"t[type=table]"}, RealmRelativePatterns: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.SchemaRemovalBlockers["app"], qt.DeepEquals, catalog.SelectionBlocker{
		Object: `table app."dot.t"`, Cause: catalog.SelectionNotIncluded, Pattern: "app",
	})
	c.Assert(filtered.Tables, qt.HasLen, 1)
	c.Assert(filtered.Tables[0].Schema, qt.Equals, "app.dot")
}

// tableWithChildren is a read whose table app.u holds a column, an index and
// a trigger beside its key column, and whose table app.t holds nothing a
// selector names.
func tableWithChildren() *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}},
		Tables: []catalog.Table{
			{Schema: "app", Name: "t", Columns: []catalog.Column{{Name: "id"}}},
			{Schema: "app", Name: "u", Columns: []catalog.Column{{Name: "id"}, {Name: "c"}}},
		},
		Indexes:  []catalog.Index{{Schema: "app", TableName: "u", Name: "ix", Columns: []string{"c"}}},
		Triggers: []catalog.Trigger{{Schema: "app", Table: "u", Name: "trg"}},
	}
}

// A table that holds an excluded child is kept, and dropping it would remove
// the child, so the projection records the child for the table as it does
// for the database.
func TestScopeDatabase_RecordsAnExcludedChildForItsTable(t *testing.T) {
	for _, test := range []struct {
		exclude, object string
	}{
		{exclude: "app.u.c", object: "column app.u.c"},
		{exclude: "app.u.id", object: "column app.u.id"},
		{exclude: "app.u.ix", object: "index app.u.ix"},
		{exclude: "app.u.trg", object: "trigger app.u.trg"},
	} {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)
			filtered, err := atlasfilter.ScopeDatabase(tableWithChildren(), atlasfilter.Scope{
				Selections: ownerSelections(), Exclude: []string{test.exclude}, RealmRelativePatterns: true,
			})
			c.Assert(err, qt.IsNil)
			want := catalog.SelectionBlocker{Object: test.object, Cause: catalog.SelectionExcluded}
			c.Assert(filtered.TableRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{
				"app.u": {Object: want.Object, Cause: want.Cause, Pattern: "app.u"},
			})
			c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{
				"app": {Object: want.Object, Cause: want.Cause, Pattern: "app"},
			})
		})
	}
}

// A whole-kind exclusion of a table child can leave that kind unread, so an
// empty list cannot show that dropping any kept table is safe. One of a kind
// that is not a table child, such as a function, protects only the schema.
func TestScopeDatabase_RecordsAnUnreadKindForTheTablesThatCanHoldIt(t *testing.T) {
	for _, test := range []struct {
		name, exclude string
		tables        map[string]catalog.SelectionBlocker
		schema        catalog.SelectionBlocker
	}{
		{
			name: "triggers", exclude: "*[type=trigger]",
			tables: map[string]catalog.SelectionBlocker{
				"app.t": {Object: "triggers", Cause: catalog.SelectionKindExcluded, Selector: "*[type=trigger]", Pattern: "app.t"},
				"app.u": {Object: "trigger app.u.trg", Cause: catalog.SelectionExcluded, Pattern: "app.u"},
			},
			// An object the read found is named ahead of a kind it left out.
			schema: catalog.SelectionBlocker{Object: "trigger app.u.trg", Cause: catalog.SelectionExcluded, Pattern: "app"},
		},
		{
			name: "functions", exclude: "*[type=function]",
			schema: catalog.SelectionBlocker{Object: "routines", Cause: catalog.SelectionKindExcluded, Selector: "*[type=function]", Pattern: "app"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			filtered, err := atlasfilter.ScopeDatabase(tableWithChildren(), atlasfilter.Scope{
				Selections: ownerSelections(), Exclude: []string{test.exclude}, RealmRelativePatterns: true,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(filtered.TableRemovalBlockers, qt.DeepEquals, test.tables)
			c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{"app": test.schema})
		})
	}
}

// A child an --include selection leaves out of a table it keeps is recorded
// for that table with the cause that names --include.
func TestScopeDatabase_RecordsAChildTheIncludeLeavesOut(t *testing.T) {
	c := qt.New(t)
	filtered, err := atlasfilter.ScopeDatabase(tableWithChildren(), atlasfilter.Scope{
		Selections: ownerSelections(), Include: []string{"app.u[type=table]"}, RealmRelativePatterns: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{
		"app": {Object: "table app.t", Cause: catalog.SelectionNotIncluded, Pattern: "app"},
	})
}

// An --include selection whose selectors all name one type leaves the other
// kinds unread, so a schema it keeps is protected by a kind even when every
// object the read found is selected.
func TestScopeDatabase_RecordsAKindTheIncludeLeavesUnread(t *testing.T) {
	c := qt.New(t)
	filtered, err := atlasfilter.ScopeDatabase(tableWithChildren(), atlasfilter.Scope{
		Selections: ownerSelections(), Include: []string{"app.*[type=table]"}, RealmRelativePatterns: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{
		"app": {Object: "events", Cause: catalog.SelectionKindNotIncluded, Pattern: "app"},
	})
	c.Assert(filtered.TableRemovalBlockers, qt.IsNil)
}

// A refusal names an object the read found ahead of a kind the read left out,
// whatever their names: the object says what the drop would remove.
func TestScopeDatabase_NamesAnObjectAheadOfAnUnreadKind(t *testing.T) {
	c := qt.New(t)
	filtered, err := atlasfilter.ScopeDatabase(tableWithChildren(), atlasfilter.Scope{
		Selections: ownerSelections(), Exclude: []string{"*[type=function]", "app.t"}, RealmRelativePatterns: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]catalog.SelectionBlocker{
		"app": {Object: "table app.t", Cause: catalog.SelectionExcluded, Pattern: "app"},
	})
}
