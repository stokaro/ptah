package lineage_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/lineage"
)

// lineageRow is one view body and everything the derivation should say about
// it: the dependencies it establishes, and the columns it cannot.
//
// wantEdges are rendered as "table.column -> view.column" and wantUnresolved as
// "view.column: reason", so a row reads as the report a person would.
type lineageRow struct {
	name           string
	body           string
	wantEdges      []string
	wantUnresolved []string
}

// twoTableSchema declares the relations every row below reads: authors(id,
// name) and books(id, title). Both carry an "id" so a bare reference to it is
// genuinely ambiguous rather than ambiguous by construction.
func twoTableSchema(viewName, body string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Author", Name: "authors"},
			{StructName: "Book", Name: "books"},
		},
		Fields: []goschema.Field{
			{StructName: "Author", Name: "id", Type: "BIGINT"},
			{StructName: "Author", Name: "name", Type: "TEXT"},
			{StructName: "Book", Name: "id", Type: "BIGINT"},
			{StructName: "Book", Name: "title", Type: "TEXT"},
		},
		Views: []goschema.View{{StructName: "V", Name: viewName, Body: body}},
	}
}

func renderEdges(result lineage.Result) []string {
	var rendered []string
	for _, edge := range result.Edges {
		rendered = append(rendered, fmt.Sprintf("%s.%s -> %s.%s",
			edge.FromTable, edge.FromColumn, edge.ToView, edge.ToColumn))
	}
	return rendered
}

func renderUnresolved(result lineage.Result) []string {
	var rendered []string
	for _, unresolved := range result.Unresolved {
		rendered = append(rendered, fmt.Sprintf("%s.%s: %s",
			unresolved.View, unresolved.Column, unresolved.Reason))
	}
	return rendered
}

// TestDerive_ResolvesWhatItCanAndNamesTheRest is the contract: every view
// column appears in exactly one of the two halves.
//
// A column that is neither an edge nor an unresolved entry would have been
// dropped, and a reader cannot tell a dropped column from one that depends on
// nothing (stokaro/ptah#1712).
func TestDerive_ResolvesWhatItCanAndNamesTheRest(t *testing.T) {
	rows := []lineageRow{{
		name:      "a plain select resolves every column",
		body:      "SELECT id, name FROM authors",
		wantEdges: []string{"authors.id -> v.id", "authors.name -> v.name"},
	}, {
		// The qualification is what makes this resolvable where the bare
		// spelling below is not.
		name:      "a qualified reference resolves against its own relation",
		body:      "SELECT authors.id FROM authors",
		wantEdges: []string{"authors.id -> v.id"},
	}, {
		name:      "a join attributes each column to the relation that declares it",
		body:      "SELECT authors.name, books.title FROM authors JOIN books ON books.id = authors.id",
		wantEdges: []string{"authors.name -> v.name", "books.title -> v.title"},
	}, {
		name:           "a computed column says it is computed rather than claiming a source",
		body:           "SELECT upper(name) AS shout FROM authors",
		wantUnresolved: []string{"v.shout: " + lineage.ReasonComputed},
	}, {
		// Both relations declare id, and the server resolves this from
		// information the derivation does not have.
		name:           "a bare reference two relations could satisfy is ambiguous",
		body:           "SELECT id FROM authors, books",
		wantUnresolved: []string{"v.id: " + lineage.ReasonAmbiguous},
	}, {
		name:           "a reference no relation declares is named, not dropped",
		body:           "SELECT nope FROM authors",
		wantUnresolved: []string{"v.nope: " + lineage.ReasonNoSuchColumn},
	}, {
		// The parser models a top-level select list. A body that opens with a
		// common table expression is not one, and no column of it is reached.
		name:           "a body the parser does not model is reported whole",
		body:           "WITH x AS (SELECT id FROM authors) SELECT id FROM x",
		wantUnresolved: []string{"v.: " + lineage.ReasonBodyNotModeled},
	}, {
		// A star has no column list to resolve, so the same answer is the
		// honest one -- and it is the answer that keeps a schema using SELECT *
		// from reading as a schema with no dependencies.
		name:           "a star projection is reported whole too",
		body:           "SELECT * FROM authors",
		wantUnresolved: []string{"v.: " + lineage.ReasonBodyNotModeled},
	}}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			result := lineage.Derive(twoTableSchema("v", row.body))
			c.Assert(renderEdges(result), qt.DeepEquals, row.wantEdges)
			c.Assert(renderUnresolved(result), qt.DeepEquals, row.wantUnresolved)
		})
	}
}

// TestDerive_ReadsMaterializedViewsToo pins that the second view kind is not
// silently skipped: it carries a body for the same reason and depends on base
// columns the same way.
func TestDerive_ReadsMaterializedViewsToo(t *testing.T) {
	c := qt.New(t)
	db := twoTableSchema("unused", "SELECT id FROM authors")
	db.Views = nil
	db.MaterializedViews = []goschema.MaterializedView{{StructName: "M", Name: "mv", Body: "SELECT title FROM books"}}

	result := lineage.Derive(db)

	c.Assert(renderEdges(result), qt.DeepEquals, []string{"books.title -> mv.title"})
	c.Assert(renderUnresolved(result), qt.HasLen, 0)
}

// TestDerive_IsOrdered pins that two runs over the same schema report the same
// thing in the same order, so a diff of two reports is about the schema.
func TestDerive_IsOrdered(t *testing.T) {
	c := qt.New(t)
	db := twoTableSchema("v", "SELECT name, id FROM authors")
	db.Views = append(db.Views, goschema.View{StructName: "W", Name: "a_view", Body: "SELECT title FROM books"})

	first := renderEdges(lineage.Derive(db))
	second := renderEdges(lineage.Derive(db))

	c.Assert(first, qt.DeepEquals, second)
	c.Assert(first, qt.DeepEquals, []string{
		"books.title -> a_view.title",
		"authors.id -> v.id",
		"authors.name -> v.name",
	})
}
