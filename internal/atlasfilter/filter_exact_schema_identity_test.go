package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

func TestExcludeDatabaseWithDefaultSchema_BareSchemaPreservesCatalogCase(t *testing.T) {
	tests := []string{"Sales", "Sales[type=schema]", "S*"}
	for _, pattern := range tests {
		t.Run(pattern, func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{
				Schemas: []catalog.Schema{{Name: "Sales"}, {Name: "sales"}},
				Tables: []catalog.Table{
					{Schema: "Sales", Name: "orders"},
					{Schema: "sales", Name: "orders"},
				},
			}

			got, report, err := atlasfilter.ExcludeDatabaseReport(database, []string{pattern}, "dbo")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.IsNil)
			c.Assert(got.Schemas, qt.DeepEquals, []catalog.Schema{{Name: "sales"}})
			c.Assert(got.Tables, qt.HasLen, 1)
			c.Assert(got.Tables[0].Schema, qt.Equals, "sales")
			c.Assert(got.Tables[0].Name, qt.Equals, "orders")
		})
	}
}

func TestExcludeGeneratedWithDefaultSchema_BareSchemaPreservesCatalogCase(t *testing.T) {
	tests := []string{"Sales", "Sales[type=schema]", "S*"}
	for _, pattern := range tests {
		t.Run(pattern, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "Sales"}, {Name: "sales"}},
				Tables: []schemamodel.Table{
					{Schema: "Sales", Name: "orders", StructName: "UpperOrders"},
					{Schema: "sales", Name: "orders", StructName: "LowerOrders"},
				},
			}

			got, report, err := atlasfilter.ExcludeGeneratedReport(database, []string{pattern}, "dbo")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.IsNil)
			c.Assert(got.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "sales"}})
			c.Assert(got.Tables, qt.HasLen, 1)
			c.Assert(got.Tables[0].Schema, qt.Equals, "sales")
			c.Assert(got.Tables[0].Name, qt.Equals, "orders")
			c.Assert(got.Tables[0].StructName, qt.Equals, "LowerOrders")
		})
	}
}

func TestExcludeDatabaseWithDefaultSchema_QuotedSchemaTakesItsExactObjects(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Schemas: []catalog.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []catalog.Table{
			{Schema: " app ", Name: "users"},
			{Schema: "app", Name: "users"},
		},
		Extensions: []catalog.Extension{
			{Schema: " app ", Name: "pgcrypto"},
			{Schema: "app", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(database, []string{`" app "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []catalog.Schema{{Name: "app"}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, "app")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Extensions, qt.DeepEquals, []catalog.Extension{{Schema: "app", Name: "citext"}})
}

func TestExcludeDatabaseWithDefaultSchema_UnquotedSchemaKeepsWhitespaceIdentity(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Schemas: []catalog.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []catalog.Table{
			{Schema: " app ", Name: "users"},
			{Schema: "app", Name: "users"},
		},
		Extensions: []catalog.Extension{
			{Schema: " app ", Name: "pgcrypto"},
			{Schema: "app", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(database, []string{"app"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []catalog.Schema{{Name: " app "}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, " app ")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Extensions, qt.DeepEquals, []catalog.Extension{{Schema: " app ", Name: "pgcrypto"}})
}

func TestExcludeGeneratedWithDefaultSchema_QuotedSchemaTakesItsExactObjects(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []schemamodel.Table{
			{Schema: " app ", Name: "users", StructName: "AppUsers"},
			{Schema: "app", Name: "users", StructName: "PlainAppUsers"},
		},
		Extensions: []schemamodel.Extension{
			{Schema: " app ", Name: "pgcrypto"},
			{Schema: "app", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(database, []string{`" app "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "app"}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, "app")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Tables[0].StructName, qt.Equals, "PlainAppUsers")
	c.Assert(got.Extensions, qt.DeepEquals, []schemamodel.Extension{{Schema: "app", Name: "citext"}})
}

func TestExcludeGeneratedWithDefaultSchema_UnquotedSchemaKeepsWhitespaceIdentity(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []schemamodel.Table{
			{Schema: " app ", Name: "users", StructName: "AppUsers"},
			{Schema: "app", Name: "users", StructName: "PlainAppUsers"},
		},
		Extensions: []schemamodel.Extension{
			{Schema: " app ", Name: "pgcrypto"},
			{Schema: "app", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(database, []string{"app"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: " app "}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, " app ")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Tables[0].StructName, qt.Equals, "AppUsers")
	c.Assert(got.Extensions, qt.DeepEquals, []schemamodel.Extension{{Schema: " app ", Name: "pgcrypto"}})
}

func TestExcludeDatabaseWithDefaultSchema_WhitespaceOnlySchemaIsNotTheDefault(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Schemas: []catalog.Schema{{Name: " "}, {Name: "public"}},
		Extensions: []catalog.Extension{
			{Schema: " ", Name: "pgcrypto"},
			{Schema: "public", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(database, []string{`" "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []catalog.Schema{{Name: "public"}})
	c.Assert(got.Extensions, qt.DeepEquals, []catalog.Extension{{Schema: "public", Name: "citext"}})
}

func TestExcludeGeneratedWithDefaultSchema_WhitespaceOnlySchemaIsNotTheDefault(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: " "}, {Name: "public"}},
		Extensions: []schemamodel.Extension{
			{Schema: " ", Name: "pgcrypto"},
			{Schema: "public", Name: "citext"},
		},
	}

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(database, []string{`" "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "public"}})
	c.Assert(got.Extensions, qt.DeepEquals, []schemamodel.Extension{{Schema: "public", Name: "citext"}})
}
