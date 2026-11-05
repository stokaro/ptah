package schemalineage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/lexerext"
	"ptah.run/internal/providerscan"
	"ptah.run/internal/providerscan/scantest"
	"ptah.run/internal/schemalineage"
	"ptah.run/internal/sqlident"
)

// pathShop is a schema whose views name their table by one path, the way an
// engine with paths writes them: a table in the directory shop, and views
// whose bodies hold a double-quoted string and an @@ string.
func pathShop() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Schema: "shop"}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id"},
			{StructName: "Item", Name: "label"},
			{StructName: "Item", Name: "qty"},
		},
		Views: []schemamodel.View{
			{Name: "shop.active", Body: "SELECT id , label AS name FROM `shop/items` WHERE qty > 0"},
			{Name: "shop.active_names", Body: "SELECT name FROM `shop/active`"},
			{Name: "shop.everything", Body: "SELECT * FROM `shop/items`"},
			{Name: "shop.tagged", Body: `SELECT id , "fixed" AS tag FROM ` + "`shop/items`"},
			{Name: "shop.noted", Body: "SELECT id , @@it's@@ AS note , label FROM `shop/items`"},
		},
	}
}

// On an engine whose provider scans its text and declares paths, an edge's
// source is the table's Ptah name, its directory as the schema, so a view
// over a view links to the view it reads. A star resolves to the columns the
// table declares, a double-quoted text is a string, which feeds no column,
// and so is an @@ string, whose quote opens no other string.
func TestDeriveForDialect_AProvidersText(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlident.RegisterQuoting(descriptor.Quoting{Style: descriptor.QuoteBacktick, Bare: descriptor.BareNever, Paths: true}, "lineage-paths"), qt.IsNil)
	release, err := lexerext.Provide(providerscan.New(scantest.Scan), "lineage-paths")
	c.Assert(err, qt.IsNil)
	t.Cleanup(release)

	result := schemalineage.DeriveForDialect(pathShop(), "lineage-paths")

	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Edges, qt.DeepEquals, []schemalineage.Edge{
		{FromTable: "shop.items", FromColumn: "id", ToView: "shop.active", ToColumn: "id"},
		{FromTable: "shop.items", FromColumn: "label", ToView: "shop.active", ToColumn: "name"},
		{FromTable: "shop.active", FromColumn: "name", ToView: "shop.active_names", ToColumn: "name"},
		{FromTable: "shop.items", FromColumn: "id", ToView: "shop.everything", ToColumn: "id"},
		{FromTable: "shop.items", FromColumn: "label", ToView: "shop.everything", ToColumn: "label"},
		{FromTable: "shop.items", FromColumn: "qty", ToView: "shop.everything", ToColumn: "qty"},
		{FromTable: "shop.items", FromColumn: "id", ToView: "shop.noted", ToColumn: "id"},
		{FromTable: "shop.items", FromColumn: "label", ToView: "shop.noted", ToColumn: "label"},
		{FromTable: "shop.items", FromColumn: "id", ToView: "shop.tagged", ToColumn: "id"},
	})
}

// The same bodies read without the engine take the path for a table name no
// table has, so the star is not resolved, and read the quote inside the @@
// string as the start of another string that swallows the FROM. This is the
// control that shows the engine's reading is what resolves both.
func TestDerive_ReadsAPathAsAName(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.Derive(pathShop())

	c.Assert(result.Undecided, qt.DeepEquals, []schemalineage.Undecided{
		{
			View:   "shop.everything",
			Reason: `the select list is * and table "shop/items" declares no columns here, so its names are unknown`,
		},
		{View: "shop.noted", Reason: "the body has no top-level FROM"},
	})
}
