package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// spreadExtensions is one extension in the connection's default schema, one
// in a schema a column names, and one in a schema nothing else uses.
var spreadExtensions = []installed{
	{name: "pgcrypto"},
	{schema: "extensions", name: "citext"},
	{schema: "other", name: "unrelated"},
}

// allSpreadExtensions is [spreadExtensions] as [readExtensions] lists it.
var allSpreadExtensions = []string{".pgcrypto:", "extensions.citext:", "other.unrelated:"}

func TestScopeGenerated_SchemaUniverseRetainsDatabaseWideExtensions(t *testing.T) {
	c := qt.New(t)
	database := withDeclaredExtensions(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables:  []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Fields:  []schemamodel.Field{{StructName: "User", Name: "email", Type: "extensions.citext"}},
	}, spreadExtensions...)

	got, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(declaredExtensions(got), qt.DeepEquals, allSpreadExtensions)
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(got.NotDescribed.IsZero(), qt.IsTrue)
}

func TestScopeDatabase_SchemaUniverseRetainsDatabaseWideExtensions(t *testing.T) {
	c := qt.New(t)
	database := withReadExtensions(&catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables: []catalog.Table{{
			Schema: "app",
			Name:   "users",
			Columns: []catalog.Column{{
				Name: "email", DataType: "USER-DEFINED", UDTName: "citext", FormattedType: "extensions.citext",
			}},
		}},
	}, spreadExtensions...)

	got, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(readExtensions(got), qt.DeepEquals, allSpreadExtensions)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
}

func TestScopeGenerated_NonExtensionIncludeCarriesDatabaseWideExtensions(t *testing.T) {
	c := qt.New(t)
	database := withDeclaredExtensions(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables:  []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Fields:  []schemamodel.Field{{StructName: "User", Name: "email", Type: "extensions.citext"}},
	}, spreadExtensions...)

	got, reports, err := atlasfilter.ScopeGeneratedSelectionReport(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"app.users", "other.unrelated"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(declaredExtensions(got), qt.DeepEquals, allSpreadExtensions)
	c.Assert(reports.Selection.NonExtensionMatched, qt.IsTrue)
	c.Assert(got.NotDescribed.IsZero(), qt.IsTrue)
}

func TestScopeDatabase_NonExtensionIncludeCarriesDatabaseWideExtensions(t *testing.T) {
	c := qt.New(t)
	database := withReadExtensions(&catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables: []catalog.Table{{
			Schema: "app",
			Name:   "users",
			Columns: []catalog.Column{{
				Name: "email", DataType: "USER-DEFINED", UDTName: "citext", FormattedType: "extensions.citext",
			}},
		}},
	}, spreadExtensions...)

	got, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"app.users", "other.unrelated"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(readExtensions(got), qt.DeepEquals, allSpreadExtensions)
}

func TestScopeGenerated_ExtensionOnlyIncludeSelectsExtensions(t *testing.T) {
	c := qt.New(t)
	database := withDeclaredExtensions(&schemamodel.Database{}, spreadExtensions...)

	qualified, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"extensions.citext"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredExtensions(qualified), qt.DeepEquals, []string{"extensions.citext:"})
	c.Assert(qualified.NotDescribed.IsZero(), qt.IsTrue)

	bare, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"citext"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredExtensions(bare), qt.DeepEquals, []string{"extensions.citext:"})

	missed, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"extensions.typo"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "extensions\.typo"`)
	c.Assert(missed.FeatureObjects.Len(), qt.Equals, 0)
}

func TestScopeDatabase_ExtensionOnlyIncludeSelectsExtensions(t *testing.T) {
	c := qt.New(t)
	database := withReadExtensions(&catalog.Database{}, spreadExtensions...)

	qualified, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"extensions.citext"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(readExtensions(qualified), qt.DeepEquals, []string{"extensions.citext:"})

	bare, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"citext"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(readExtensions(bare), qt.DeepEquals, []string{"extensions.citext:"})

	missed, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"extensions.typo"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "extensions\.typo"`)
	c.Assert(missed.FeatureObjects.Len(), qt.Equals, 0)
}
