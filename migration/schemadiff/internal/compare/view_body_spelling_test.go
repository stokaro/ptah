package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// itemListDeclared and itemListStored are a view over a set-returning function,
// as declared and as PostgreSQL 18.6 printed it back: the server adds the
// function's whole result as a column alias list (stokaro/ptah#4057).
const (
	itemListDeclared = "SELECT id, title FROM public.all_items()"
	itemListStored   = " SELECT id,\n    title\n   FROM all_items() all_items(id, title, tags);"
)

// spelledBody is the server's answer for one declared body.
func spelledBody(declared, spelled string) map[string]config.ViewBody {
	return map[string]config.ViewBody{exprkey.ViewBody(declared): {Body: spelled, Resolved: true}}
}

// viewsWithSpelling compares one declared view with one stored view.
func viewsWithSpelling(declared, stored string, spelled map[string]config.ViewBody) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.ViewsWithSemantics(
		&schemamodel.Database{Views: []schemamodel.View{{Name: "item_list", Body: declared}}},
		&catalog.Database{Views: []catalog.View{{Name: "item_list", Schema: "public", Body: stored}}},
		diff, platform.Postgres, identifier.ForDialect(platform.Postgres), spelled,
	)
	return diff
}

// matViewsWithSpelling is viewsWithSpelling for a materialized view.
func matViewsWithSpelling(declared, stored string, spelled map[string]config.ViewBody) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.MaterializedViewsWithSemantics(
		&schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "item_list", Body: declared}}},
		&catalog.Database{MatViews: []catalog.MaterializedView{{Name: "item_list", Schema: "public", Body: stored}}},
		diff, platform.Postgres, identifier.ForDialect(platform.Postgres), spelled,
	)
	return diff
}

// Where a server spelled the declared body, a view the text comparison finds
// different matches when the spelling equals the catalog's. Each row is a
// declaration and what PostgreSQL 18.6 printed for it.
func TestViewsWithSemantics_ComparesTheServersSpellingOfTheBody(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
	}{
		{name: "a qualified function in FROM", declared: itemListDeclared, stored: itemListStored},
		{
			name:     "an aliased function in FROM",
			declared: "SELECT i.id FROM all_items() i",
			stored:   " SELECT id\n   FROM all_items() i(id, title, tags);",
		},
		{
			name:     "a scalar set-returning function",
			declared: "SELECT g FROM generate_series(1, 3) g",
			stored:   " SELECT g\n   FROM generate_series(1, 3) g(g);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := viewsWithSpelling(test.declared, test.stored, spelledBody(test.declared, test.stored))

			c.Assert(diff.ViewsModified, qt.HasLen, 0)
			c.Assert(diff.ViewsAdded, qt.HasLen, 0)
			c.Assert(diff.ViewsRemoved, qt.HasLen, 0)
		})
	}
}

// A materialized view's body goes through the same spelling.
func TestMaterializedViewsWithSemantics_ComparesTheServersSpellingOfTheBody(t *testing.T) {
	c := qt.New(t)

	diff := matViewsWithSpelling(itemListDeclared, itemListStored, spelledBody(itemListDeclared, itemListStored))

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
}

// The controls. A spelling that differs from the catalog's is a change, and the
// plan writes the declaration as written. Without an answer, or with one given
// for another body, the text comparison decides, and it cannot produce the
// alias list.
func TestViewsWithSemantics_TheServersSpellingStillSeesAChange(t *testing.T) {
	tests := []struct {
		name    string
		stored  string
		spelled map[string]config.ViewBody
	}{
		{
			name:    "a resolved spelling of a different select list",
			stored:  " SELECT id\n   FROM all_items() all_items(id, title, tags);",
			spelled: spelledBody(itemListDeclared, itemListStored),
		},
		{
			name:    "an unresolved spelling",
			stored:  itemListStored,
			spelled: map[string]config.ViewBody{exprkey.ViewBody(itemListDeclared): {}},
		},
		{
			name:    "a spelling of another body",
			stored:  itemListStored,
			spelled: spelledBody("SELECT id, title FROM all_items()", itemListStored),
		},
		{
			name:   "no server",
			stored: itemListStored,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := viewsWithSpelling(itemListDeclared, test.stored, test.spelled)

			c.Assert(diff.ViewsModified, qt.HasLen, 1)
			c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
			c.Assert(diff.ViewsModified[0].Desired.Body, qt.Equals, itemListDeclared)
		})
	}
}

// The server's spelling only ever matches. A body the text comparison finds
// equal stays equal even when the spelling differs, so a server answer cannot
// plan a view that was not planned without it.
func TestViewsWithSemantics_TheServersSpellingNeverAddsAChange(t *testing.T) {
	c := qt.New(t)
	const plain = "SELECT id, title FROM items WHERE id > 10"

	diff := viewsWithSpelling(plain, " SELECT id,\n    title\n   FROM items\n  WHERE id > 10;",
		spelledBody(plain, " SELECT something_else\n   FROM elsewhere;"))

	c.Assert(diff.ViewsModified, qt.HasLen, 0)
}
