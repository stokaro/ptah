package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

func TestScopeGenerated_SchemaUniverseRetainsDatabaseWideExtensions(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables:  []goschema.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Fields:  []goschema.Field{{StructName: "User", Name: "email", Type: "extensions.citext"}},
		Extensions: []goschema.Extension{
			{Name: "pgcrypto"},
			{Schema: "extensions", Name: "citext"},
			{Schema: "other", Name: "unrelated"},
		},
	}

	got, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedExtensionNames(got.Extensions), qt.DeepEquals, []string{
		"extensions.citext",
		"pgcrypto",
		"other.unrelated",
	})
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(got.NotDescribed.IsZero(), qt.IsTrue)
}

func TestScopeDatabase_SchemaUniverseRetainsDatabaseWideExtensions(t *testing.T) {
	c := qt.New(t)
	database := &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables: []dbschematypes.DBTable{{
			Schema: "app",
			Name:   "users",
			Columns: []dbschematypes.DBColumn{{
				Name: "email", DataType: "USER-DEFINED", UDTName: "citext", FormattedType: "extensions.citext",
			}},
		}},
		Extensions: []dbschematypes.DBExtension{
			{Name: "pgcrypto"},
			{Schema: "extensions", Name: "citext"},
			{Schema: "other", Name: "unrelated"},
		},
	}

	got, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Extensions, qt.DeepEquals, database.Extensions)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
}

func TestScopeGenerated_NonExtensionIncludeCarriesDatabaseWideExtensions(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables:  []goschema.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Fields:  []goschema.Field{{StructName: "User", Name: "email", Type: "extensions.citext"}},
		Extensions: []goschema.Extension{
			{Name: "pgcrypto"},
			{Schema: "extensions", Name: "citext"},
			{Schema: "other", Name: "unrelated"},
		},
	}

	got, reports, err := atlasfilter.ScopeGeneratedSelectionReport(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"app.users", "other.unrelated"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(generatedExtensionNames(got.Extensions), qt.DeepEquals, []string{
		"extensions.citext",
		"pgcrypto",
		"other.unrelated",
	})
	c.Assert(reports.Selection.NonExtensionMatched, qt.IsTrue)
	c.Assert(got.NotDescribed.IsZero(), qt.IsTrue)
}

func TestScopeDatabase_NonExtensionIncludeCarriesDatabaseWideExtensions(t *testing.T) {
	c := qt.New(t)
	database := &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables: []dbschematypes.DBTable{{
			Schema: "app",
			Name:   "users",
			Columns: []dbschematypes.DBColumn{{
				Name: "email", DataType: "USER-DEFINED", UDTName: "citext", FormattedType: "extensions.citext",
			}},
		}},
		Extensions: []dbschematypes.DBExtension{
			{Name: "pgcrypto"},
			{Schema: "extensions", Name: "citext"},
			{Schema: "other", Name: "unrelated"},
		},
	}

	got, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"app.users", "other.unrelated"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(got.Extensions, qt.DeepEquals, database.Extensions)
}

func TestScopeGenerated_ExtensionOnlyIncludeSelectsExtensions(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{Extensions: []goschema.Extension{
		{Name: "pgcrypto"},
		{Schema: "extensions", Name: "citext"},
		{Schema: "other", Name: "unrelated"},
	}}

	qualified, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"extensions.citext"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(generatedExtensionNames(qualified.Extensions), qt.DeepEquals, []string{"extensions.citext"})
	c.Assert(qualified.NotDescribed.IsZero(), qt.IsTrue)

	bare, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"citext"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(generatedExtensionNames(bare.Extensions), qt.DeepEquals, []string{"extensions.citext"})

	missed, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"extensions.typo"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "extensions\.typo"`)
	c.Assert(missed.Extensions, qt.HasLen, 0)
}

func TestScopeDatabase_ExtensionOnlyIncludeSelectsExtensions(t *testing.T) {
	c := qt.New(t)
	database := &dbschematypes.DBSchema{Extensions: []dbschematypes.DBExtension{
		{Name: "pgcrypto"},
		{Schema: "extensions", Name: "citext"},
		{Schema: "other", Name: "unrelated"},
	}}

	qualified, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"extensions.citext"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(qualified.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{
		Schema: "extensions", Name: "citext",
	}})

	bare, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"citext"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(bare.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{
		Schema: "extensions", Name: "citext",
	}})

	missed, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"extensions.typo"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "extensions\.typo"`)
	c.Assert(missed.Extensions, qt.HasLen, 0)
}
