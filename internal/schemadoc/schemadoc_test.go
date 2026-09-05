package schemadoc_test

import (
	"maps"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemadoc"
)

// bookshop is a schema with a dependency chain, a foreign key, an index and an
// enum, so one fixture exercises every section the document renders.
func bookshop() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Author", Name: "authors", Comment: "People who write things"},
			{StructName: "Book", Name: "books"},
			{StructName: "Order", Name: "orders"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Author", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Author", Name: "email", Type: "TEXT", Unique: true, Nullable: true},
			{StructName: "Book", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Book", Name: "author_id", Type: "BIGINT", Foreign: "authors(id)"},
			{StructName: "Order", Name: "book_id", Type: "BIGINT", Foreign: "books(id)"},
		},
		Indexes: []schemamodel.Index{{StructName: "Book", Name: "idx_books_author", Fields: []string{"author_id"}}},
		Enums:   []schemamodel.Enum{{Name: "order_status", Values: []string{"pending", "paid"}}},
	}
}

func render(c *qt.C, db *schemamodel.Database, opts schemadoc.Options) string {
	c.Helper()
	result, err := schemadoc.Render(db, opts)
	c.Assert(err, qt.IsNil)
	return string(result.Data)
}

// TestRender_FetchesNothing is the property the whole design follows from.
//
// Any absolute URL in the output is a request the reader's browser makes when
// they open the file, which is what this document exists not to do: it would
// mean the page does not work offline, and that looking at a schema can be
// observed.
func TestRender_FetchesNothing(t *testing.T) {
	c := qt.New(t)

	page := render(c, bookshop(), schemadoc.Options{})

	external := regexp.MustCompile(`(?i)(https?:)?//[a-z0-9.-]+\.[a-z]{2,}`).FindAllString(page, -1)
	c.Assert(external, qt.HasLen, 0, qt.Commentf("the document must reference nothing outside itself"))
	for _, element := range []string{"<script", "<link", "<img", "@import"} {
		c.Assert(page, qt.Not(qt.Contains), element,
			qt.Commentf("%s is how a page reaches for something it does not carry", element))
	}
	// url(#a) points at the arrowhead defined a few bytes earlier, so the test
	// is about where a url() points rather than about the function appearing.
	// Forbidding it outright would forbid the diagram's own marker.
	for _, reference := range regexp.MustCompile(`url\(([^)]*)\)`).FindAllStringSubmatch(page, -1) {
		c.Assert(strings.HasPrefix(reference[1], "#"), qt.IsTrue,
			qt.Commentf("url(%s) leaves the document", reference[1]))
	}
}

// docRow is one thing the document must say, and where.
type docRow struct {
	name string
	want string
}

// TestRender_SaysEverythingTheSchemaDeclares pins that no declared object is
// silently absent. Documentation that hid a column would describe a schema the
// reader does not have.
func TestRender_SaysEverythingTheSchemaDeclares(t *testing.T) {
	rows := []docRow{
		{name: "a table name", want: "authors"},
		{name: "a table comment", want: "People who write things"},
		{name: "a column name", want: "book_id"},
		{name: "a column type", want: "BIGINT"},
		{name: "a primary key", want: `class="tag key">primary`},
		{name: "a unique column", want: `class="tag">unique`},
		{name: "a nullable column", want: `class="tag null">null`},
		{name: "a foreign key, linked to its target", want: `href="#authors"`},
		{name: "a foreign key, rendered as a reference", want: `class="ref" href="#authors">→ authors(id)`},
		{name: "a NOT NULL column, left untagged", want: `class="none">—`},
		{name: "what the document is and is not", want: `class="lede">Declared schema · not a live database`},
		{name: "how to read the diagram", want: "Left to right by dependency"},
		{name: "the binary that wrote the file", want: `class="footer-mark"`},
		{name: "an index", want: "idx_books_author"},
		{name: "an enum value", want: "pending"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, bookshop(), schemadoc.Options{}), qt.Contains, row.want)
		})
	}
}

// TestRender_EscapesAuthoredText pins that schema text reaches the page as text.
//
// A column comment is authored, and a document that pasted it into markup would
// let a comment rewrite the page it appears in.
func TestRender_EscapesAuthoredText(t *testing.T) {
	c := qt.New(t)
	db := bookshop()
	db.Tables[0].Comment = `<script>alert("x")</script>`

	page := render(c, db, schemadoc.Options{})

	c.Assert(page, qt.Not(qt.Contains), `<script>`)
	c.Assert(page, qt.Contains, `&lt;script&gt;`)
}

// TestRender_LaysTheDiagramOutByDependency pins the one thing the diagram
// promises: reading it left to right reads the order the tables can be created
// in.
//
// The x coordinates are the assertion because they are the layout. Three tables
// in a chain must occupy three distinct columns, and the table nothing points
// at must be leftmost.
func TestRender_LaysTheDiagramOutByDependency(t *testing.T) {
	c := qt.New(t)

	page := render(c, bookshop(), schemadoc.Options{})

	columns := regexp.MustCompile(`class="node" x="([0-9.]+)"`).FindAllStringSubmatch(page, -1)
	c.Assert(columns, qt.HasLen, 3)
	distinct := make(map[string]bool)
	for _, match := range columns {
		distinct[match[1]] = true
	}
	c.Assert(distinct, qt.HasLen, 3, qt.Commentf("a three-table chain occupies three layers"))
	c.Assert(strings.Index(page, `>authors<`) > 0, qt.IsTrue)
}

// TestRender_SaysWhenTheSelectionMatchedNothing pins that an empty document
// explains itself. A reader cannot otherwise tell "no tables" from "the filter
// matched none".
func TestRender_SaysWhenTheSelectionMatchedNothing(t *testing.T) {
	c := qt.New(t)

	result, err := schemadoc.Render(bookshop(), schemadoc.Options{IncludeTables: []string{"nothing_matches_this"}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.DeepEquals, []string{"no tables matched the selection"})
	c.Assert(string(result.Data), qt.Contains, "No tables are selected.")
}

// TestRender_DefinesEveryColorInEveryTheme pins that no token is introduced by
// a theme block alone.
//
// A color defined only under prefers-color-scheme is missing for a reader who
// chose light explicitly, and the element it paints falls back to whatever the
// browser decides -- which is how a page ends up with black text on a dark card.
func TestRender_DefinesEveryColorInEveryTheme(t *testing.T) {
	c := qt.New(t)
	page := render(c, bookshop(), schemadoc.Options{})

	blocks := regexp.MustCompile(`(?s)\{([^{}]*--[a-z-]+:[^{}]*)\}`).FindAllStringSubmatch(page, -1)
	c.Assert(len(blocks) >= 3, qt.IsTrue, qt.Commentf("expected a base block and two theme blocks"))

	base := tokensIn(blocks[0][1])
	for _, block := range blocks[1:] {
		for token := range tokensIn(block[1]) {
			c.Assert(base[token], qt.IsTrue,
				qt.Commentf("%s is defined in a theme block but not in the base one", token))
		}
	}
}

func tokensIn(block string) map[string]bool {
	found := make(map[string]bool)
	for _, match := range regexp.MustCompile(`(--[a-z-]+):`).FindAllStringSubmatch(block, -1) {
		found[match[1]] = true
	}
	return found
}

// TestRender_NamesItsSourceWithoutItsPath keeps a shared document from carrying
// the exporting machine's filesystem layout.
//
// The caller supplies the name, so this pins the rendering rather than the
// basename; cmd/schema is where the path is reduced to a name.
func TestRender_NamesItsSourceWithoutItsPath(t *testing.T) {
	c := qt.New(t)

	page := render(c, bookshop(), schemadoc.Options{Source: "schema.yaml"})

	c.Assert(page, qt.Contains, `class="lede">Declared schema · schema.yaml · not a live database`)
}

// TestRender_ResolvesEveryCustomPropertyItUses is the assertion the appearance
// cannot make about itself by looking correct.
//
// A var() naming a token nothing declares does not fail: the browser discards
// the declaration it appears in and reports nothing. The element keeps whatever
// it inherited, so a retired token leaves a page that renders, renders wrongly,
// and passes every other test here.
func TestRender_ResolvesEveryCustomPropertyItUses(t *testing.T) {
	c := qt.New(t)
	page := render(c, bookshop(), schemadoc.Options{})

	declared := declaredTokens(page)
	used := usedTokens(page)
	c.Assert(len(used) > 0, qt.IsTrue, qt.Commentf("the stylesheet uses no tokens at all"))
	for _, token := range slices.Sorted(maps.Keys(used)) {
		c.Assert(declared[token], qt.IsTrue,
			qt.Commentf("var(%s) resolves to nothing: no block declares it", token))
	}
}

// TestPage_LeavesTheDocumentToTheDocument pins what the live dashboard gets.
//
// Page returns the schema's own parts for a caller that supplies the page
// around them. Three things must not come with them: the rail, because the
// caller writes its own and nesting one inside it draws two; the provenance
// line, because a dashboard reads a live database and "not a live database"
// would be false there; and the footer, because the caller's page ends its own
// way.
func TestPage_LeavesTheDocumentToTheDocument(t *testing.T) {
	c := qt.New(t)

	sidebar, content, err := schemadoc.Page(bookshop(), schemadoc.Options{})

	c.Assert(err, qt.IsNil)
	page := sidebar + content
	c.Assert(sidebar, qt.Contains, `<nav class="nav">`)
	c.Assert(page, qt.Not(qt.Contains), "<aside")
	c.Assert(page, qt.Not(qt.Contains), "not a live database")
	c.Assert(page, qt.Not(qt.Contains), `class="footer`)
}

// declaredTokens is every custom property the page's stylesheet defines.
func declaredTokens(page string) map[string]bool {
	found := make(map[string]bool)
	for _, match := range regexp.MustCompile(`(--[a-z0-9-]+)\s*:`).FindAllStringSubmatch(page, -1) {
		found[match[1]] = true
	}
	return found
}

// usedTokens is every custom property the page reads back.
func usedTokens(page string) map[string]bool {
	found := make(map[string]bool)
	for _, match := range regexp.MustCompile(`var\((--[a-z0-9-]+)`).FindAllStringSubmatch(page, -1) {
		found[match[1]] = true
	}
	return found
}

// TestRender_RefusesANilSchema pins that the error is an error rather than an
// empty page, which would read as a schema with nothing in it.
func TestRender_RefusesANilSchema(t *testing.T) {
	c := qt.New(t)

	_, err := schemadoc.Render(nil, schemadoc.Options{})

	c.Assert(err, qt.IsNotNil)
}
