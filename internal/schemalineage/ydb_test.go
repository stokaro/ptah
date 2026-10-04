package schemalineage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemalineage"
)

// ydbShop is a YDB schema as a read describes it: a table in the directory
// shop, and views whose bodies are the text YDB stores, its tokens joined by
// single spaces. A YDB view names its table by the whole path, as one quoted
// identifier.
func ydbShop() *schemamodel.Database {
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

// On YDB an edge's source is the table's Ptah name, its directory as the
// schema, which is the name a view in that directory goes by too, so a view
// over a view links to the view it reads. A star resolves to the columns the
// table declares, and a double-quoted text is a YQL string, which feeds no
// column, as is a multi-line @@ string, whose quote does not open another.
func TestDeriveForDialect_YDB(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveForDialect(ydbShop(), "ydb")

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

// The same bodies read without the dialect take the path for a table name no
// table has, so the star is not resolved, and read a quote inside a YQL @@
// string as the start of another string that swallows the FROM. This is the
// control that shows the YDB reading is what resolves both.
func TestDerive_ReadsAYDBPathAsAName(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.Derive(ydbShop())

	c.Assert(result.Undecided, qt.DeepEquals, []schemalineage.Undecided{
		{
			View:   "shop.everything",
			Reason: `the select list is * and table "shop/items" declares no columns here, so its names are unknown`,
		},
		{View: "shop.noted", Reason: "the body has no top-level FROM"},
	})
}
