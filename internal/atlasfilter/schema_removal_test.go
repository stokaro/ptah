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
	c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]string{"app": "table app.t"})
	c.Assert(current.SchemaRemovalBlockers, qt.IsNil)
}

func TestScopeDatabase_SchemaProtectionDoesNotMutateTheRead(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Schemas:               []catalog.Schema{{Name: "app"}, {Name: "other"}},
		SchemaRemovalBlockers: map[string]string{"app": "table app.t"},
		Tables:                []catalog.Table{{Schema: "other", Name: "u"}},
	}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: ownerSelections(), Exclude: []string{"other.u"}, RealmRelativePatterns: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.SchemaRemovalBlockers, qt.DeepEquals, map[string]string{"app": "table app.t", "other": "table other.u"})
	c.Assert(current.SchemaRemovalBlockers, qt.DeepEquals, map[string]string{"app": "table app.t"})
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
	c.Assert(filtered.SchemaRemovalBlockers["app"], qt.Equals, `table app."dot.t"`)
	c.Assert(filtered.Tables, qt.HasLen, 1)
	c.Assert(filtered.Tables[0].Schema, qt.Equals, "app.dot")
}
