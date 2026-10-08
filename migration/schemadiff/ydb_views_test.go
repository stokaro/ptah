package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// ydbViewDeclaration declares a view in the directory shop with the query
// written as an author writes it: comments, line breaks, odd spacing and a
// semicolon.
func ydbViewDeclaration(query string) *schemamodel.Database {
	return &schemamodel.Database{Views: []schemamodel.View{{Name: "shop.active", Body: query}}}
}

// ydbViewCatalog is the view as the YDB reader reports it: the text YDB
// stores, its tokens joined by single spaces.
func ydbViewCatalog(stored string) *catalog.Database {
	return &catalog.Database{Views: []catalog.View{{Schema: "shop", Name: "active", Body: stored}}}
}

// authoredQuery and storedQuery are one view's query as written and as YDB
// 25.1.4.7 and 26.2.1.14 store it.
const (
	authoredQuery = "select   id,\n  `label` -- the name shown\nFROM `shop/items`\nWHERE qty>0;"
	storedQuery   = "select id , `label` FROM `shop/items` WHERE qty > 0"
)

// TestCompare_YDBViewMatchesTheQueryTheServerStores reads a declared query
// and the server's text of it into one form, so a view applied once plans
// nothing, against the database and against the same document on the other
// side of a file-to-file comparison.
func TestCompare_YDBViewMatchesTheQueryTheServerStores(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "against the database",
			diff: must.Must(schemadiff.CompareWithDialect(t.Context(), ydbViewDeclaration(authoredQuery), ydbViewCatalog(storedQuery), platform.YDB, must.Must(builtin.New())))},
		{name: "against the same document",
			diff: must.Must(schemadiff.CompareSchemas(t.Context(), ydbViewDeclaration(authoredQuery), ydbViewDeclaration(authoredQuery), platform.YDB, must.Must(builtin.New())))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.diff.ViewsAdded, qt.HasLen, 0)
			c.Assert(test.diff.ViewsRemoved, qt.HasLen, 0)
			c.Assert(test.diff.ViewsModified, qt.HasLen, 0)
		})
	}
}

// A query that differs from the stored one only where YDB tells two queries
// apart is a change: a name's case, since YDB names are case-sensitive, an
// operator, and a pragma the view was created under, which YDB stores with the
// query because it decides what the query's names mean.
func TestCompare_YDBViewQueryChanges(t *testing.T) {
	tests := []struct {
		name   string
		stored string
	}{
		{name: "a column's case", stored: "select ID , `label` FROM `shop/items` WHERE qty > 0"},
		{name: "an operator", stored: "select id , `label` FROM `shop/items` WHERE qty >= 0"},
		{name: "a pragma", stored: "PRAGMA TablePathPrefix('/local/shop');\n" + storedQuery},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbViewDeclaration(authoredQuery), ydbViewCatalog(test.stored), platform.YDB, must.Must(builtin.New())))

			c.Assert(diff.ViewsModified, qt.HasLen, 1)
			c.Assert(diff.ViewsModified[0].ViewName, qt.Equals, "shop.active")
			c.Assert(diff.ViewsModified[0].PreviousBody, qt.Equals, test.stored)
			c.Assert(diff.ViewsAdded, qt.HasLen, 0)
			c.Assert(diff.ViewsRemoved, qt.HasLen, 0)
		})
	}
}
