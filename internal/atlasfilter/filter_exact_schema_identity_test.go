package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

func TestExcludeDatabaseWithDefaultSchema_QuotedSchemaTakesItsExactObjects(t *testing.T) {
	c := qt.New(t)
	database := &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: " app "}, {Name: "app"}},
		Tables: []dbschematypes.DBTable{
			{Schema: " app ", Name: "users"},
			{Schema: "app", Name: "users"},
		},
		Extensions: []dbschematypes.DBExtension{
			{Schema: " app ", Name: "pgcrypto"},
			{Schema: "app", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(database, []string{`" app "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []dbschematypes.DBSchemaInfo{{Name: "app"}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, "app")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{Schema: "app", Name: "citext"}})
}

func TestExcludeDatabaseWithDefaultSchema_UnquotedSchemaKeepsWhitespaceIdentity(t *testing.T) {
	c := qt.New(t)
	database := &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: " app "}, {Name: "app"}},
		Tables: []dbschematypes.DBTable{
			{Schema: " app ", Name: "users"},
			{Schema: "app", Name: "users"},
		},
		Extensions: []dbschematypes.DBExtension{
			{Schema: " app ", Name: "pgcrypto"},
			{Schema: "app", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(database, []string{"app"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []dbschematypes.DBSchemaInfo{{Name: " app "}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, " app ")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{Schema: " app ", Name: "pgcrypto"}})
}

func TestExcludeGeneratedWithDefaultSchema_QuotedSchemaTakesItsExactObjects(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []goschema.Table{
			{Schema: " app ", Name: "users", StructName: "AppUsers"},
			{Schema: "app", Name: "users", StructName: "PlainAppUsers"},
		},
		Extensions: []goschema.Extension{
			{Schema: " app ", Name: "pgcrypto"},
			{Schema: "app", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(database, []string{`" app "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "app"}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, "app")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Tables[0].StructName, qt.Equals, "PlainAppUsers")
	c.Assert(got.Extensions, qt.DeepEquals, []goschema.Extension{{Schema: "app", Name: "citext"}})
}

func TestExcludeGeneratedWithDefaultSchema_UnquotedSchemaKeepsWhitespaceIdentity(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []goschema.Table{
			{Schema: " app ", Name: "users", StructName: "AppUsers"},
			{Schema: "app", Name: "users", StructName: "PlainAppUsers"},
		},
		Extensions: []goschema.Extension{
			{Schema: " app ", Name: "pgcrypto"},
			{Schema: "app", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(database, []string{"app"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []goschema.Schema{{Name: " app "}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, " app ")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Tables[0].StructName, qt.Equals, "AppUsers")
	c.Assert(got.Extensions, qt.DeepEquals, []goschema.Extension{{Schema: " app ", Name: "pgcrypto"}})
}

func TestExcludeDatabaseWithDefaultSchema_WhitespaceOnlySchemaIsNotTheDefault(t *testing.T) {
	c := qt.New(t)
	database := &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: " "}, {Name: "public"}},
		Extensions: []dbschematypes.DBExtension{
			{Schema: " ", Name: "pgcrypto"},
			{Schema: "public", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(database, []string{`" "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []dbschematypes.DBSchemaInfo{{Name: "public"}})
	c.Assert(got.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{Schema: "public", Name: "citext"}})
}

func TestExcludeGeneratedWithDefaultSchema_WhitespaceOnlySchemaIsNotTheDefault(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{{Name: " "}, {Name: "public"}},
		Extensions: []goschema.Extension{
			{Schema: " ", Name: "pgcrypto"},
			{Schema: "public", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(database, []string{`" "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "public"}})
	c.Assert(got.Extensions, qt.DeepEquals, []goschema.Extension{{Schema: "public", Name: "citext"}})
}
