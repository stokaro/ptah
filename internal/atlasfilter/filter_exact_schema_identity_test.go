package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
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

			got, report, err := atlasfilter.ExcludeDatabaseReport(ownerSelections(), database, []string{pattern}, "dbo")

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

			got, report, err := atlasfilter.ExcludeGeneratedReport(ownerSelections(), database, []string{pattern}, "dbo")

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
	database := withReadExtensions(&catalog.Database{
		Schemas: []catalog.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []catalog.Table{
			{Schema: " app ", Name: "users"},
			{Schema: "app", Name: "users"},
		},
	}, installed{schema: " app ", name: "pgcrypto"}, installed{schema: "app", name: "citext"})

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownerSelections(), database, []string{`" app "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []catalog.Schema{{Name: "app"}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, "app")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(readExtensions(got), qt.DeepEquals, []string{"app.citext:"})
}

func TestExcludeDatabaseWithDefaultSchema_UnquotedSchemaKeepsWhitespaceIdentity(t *testing.T) {
	c := qt.New(t)
	database := withReadExtensions(&catalog.Database{
		Schemas: []catalog.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []catalog.Table{
			{Schema: " app ", Name: "users"},
			{Schema: "app", Name: "users"},
		},
	}, installed{schema: " app ", name: "pgcrypto"}, installed{schema: "app", name: "citext"})

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownerSelections(), database, []string{"app"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []catalog.Schema{{Name: " app "}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, " app ")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(readExtensions(got), qt.DeepEquals, []string{" app .pgcrypto:"})
}

func TestExcludeGeneratedWithDefaultSchema_QuotedSchemaTakesItsExactObjects(t *testing.T) {
	c := qt.New(t)
	database := withDeclaredExtensions(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []schemamodel.Table{
			{Schema: " app ", Name: "users", StructName: "AppUsers"},
			{Schema: "app", Name: "users", StructName: "PlainAppUsers"},
		},
	}, installed{schema: " app ", name: "pgcrypto"}, installed{schema: "app", name: "citext"})

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownerSelections(), database, []string{`" app "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "app"}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, "app")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Tables[0].StructName, qt.Equals, "PlainAppUsers")
	c.Assert(declaredExtensions(got), qt.DeepEquals, []string{"app.citext:"})
}

func TestExcludeGeneratedWithDefaultSchema_UnquotedSchemaKeepsWhitespaceIdentity(t *testing.T) {
	c := qt.New(t)
	database := withDeclaredExtensions(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: " app "}, {Name: "app"}},
		Tables: []schemamodel.Table{
			{Schema: " app ", Name: "users", StructName: "AppUsers"},
			{Schema: "app", Name: "users", StructName: "PlainAppUsers"},
		},
	}, installed{schema: " app ", name: "pgcrypto"}, installed{schema: "app", name: "citext"})

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownerSelections(), database, []string{"app"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: " app "}})
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Schema, qt.Equals, " app ")
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Tables[0].StructName, qt.Equals, "AppUsers")
	c.Assert(declaredExtensions(got), qt.DeepEquals, []string{" app .pgcrypto:"})
}

func TestExcludeDatabaseWithDefaultSchema_WhitespaceOnlySchemaIsNotTheDefault(t *testing.T) {
	c := qt.New(t)
	database := withReadExtensions(&catalog.Database{
		Schemas: []catalog.Schema{{Name: " "}, {Name: "public"}},
	}, installed{schema: " ", name: "pgcrypto"}, installed{schema: "public", name: "citext"})

	got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownerSelections(), database, []string{`" "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []catalog.Schema{{Name: "public"}})
	c.Assert(readExtensions(got), qt.DeepEquals, []string{"public.citext:"})
}

func TestExcludeGeneratedWithDefaultSchema_WhitespaceOnlySchemaIsNotTheDefault(t *testing.T) {
	c := qt.New(t)
	database := withDeclaredExtensions(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: " "}, {Name: "public"}},
	}, installed{schema: " ", name: "pgcrypto"}, installed{schema: "public", name: "citext"})

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownerSelections(), database, []string{`" "`}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "public"}})
	c.Assert(declaredExtensions(got), qt.DeepEquals, []string{"public.citext:"})
}
