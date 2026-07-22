package graphqlrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/graphqlrender"
)

func fixture() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Author", Name: "authors"},
			{StructName: "Book", Name: "books"},
		},
		Fields: []goschema.Field{
			{StructName: "Author", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Author", Name: "name", Type: "VARCHAR(255)"},
			{StructName: "Author", Name: "status", Type: "enum_author_status", Nullable: true, Enum: []string{"active", "retired"}},
			{StructName: "Book", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Book", Name: "title", Type: "TEXT"},
			{StructName: "Book", Name: "price", Type: "DECIMAL(10,2)"},
			{StructName: "Book", Name: "published_at", Type: "TIMESTAMP", Nullable: true},
			{StructName: "Book", Name: "in_print", Type: "BOOLEAN"},
			{StructName: "Book", Name: "author_id", Type: "INTEGER", Foreign: "authors(id)"},
			{StructName: "Book", Name: "metadata", Type: "JSONB", Nullable: true},
		},
		Enums: []goschema.Enum{{Name: "enum_author_status", Values: []string{"active", "retired"}}},
	}
}

func render(c *qt.C, opts graphqlrender.Options) string {
	res, err := graphqlrender.Render(fixture(), opts)
	c.Assert(err, qt.IsNil)
	return string(res.Data)
}

func TestRenderObjectTypesAndScalars(t *testing.T) {
	c := qt.New(t)
	sdl := render(c, graphqlrender.Options{})

	c.Assert(sdl, qt.Contains, "scalar DateTime")
	c.Assert(sdl, qt.Contains, "scalar JSON")

	c.Assert(sdl, qt.Contains, "enum AuthorStatus {\n  active\n  retired\n}")

	// Primary keys map to ID and non-null.
	c.Assert(sdl, qt.Contains, "type Author {")
	c.Assert(sdl, qt.Contains, "  id: ID!")
	c.Assert(sdl, qt.Contains, "  name: String!")
	c.Assert(sdl, qt.Contains, "  status: AuthorStatus")

	c.Assert(sdl, qt.Contains, "  price: Float!")
	c.Assert(sdl, qt.Contains, "  published_at: DateTime")
	c.Assert(sdl, qt.Contains, "  in_print: Boolean!")
	c.Assert(sdl, qt.Contains, "  metadata: JSON")
}

func TestRenderForeignKeyRelation(t *testing.T) {
	c := qt.New(t)
	sdl := render(c, graphqlrender.Options{})
	// The scalar id column is kept and a relation object is added alongside it.
	c.Assert(sdl, qt.Contains, "  author_id: Int!")
	c.Assert(sdl, qt.Contains, "  author: Author!")
}

func TestRenderInputExcludesServerGenerated(t *testing.T) {
	c := qt.New(t)
	sdl := render(c, graphqlrender.Options{})

	input := section(sdl, "input BookInput {")
	c.Assert(input, qt.Not(qt.Contains), "  id: ") // the serial primary key is server-generated
	c.Assert(input, qt.Contains, "title: String!")
	c.Assert(input, qt.Contains, "author_id: Int!")
}

func TestRenderConnectionsAndQuery(t *testing.T) {
	c := qt.New(t)
	sdl := render(c, graphqlrender.Options{})

	c.Assert(sdl, qt.Contains, "type PageInfo {")
	c.Assert(sdl, qt.Contains, "type AuthorEdge {\n  node: Author!\n  cursor: String!\n}")
	c.Assert(sdl, qt.Contains, "type AuthorConnection {\n  edges: [AuthorEdge!]!\n  pageInfo: PageInfo!\n}")

	query := section(sdl, "type Query {")
	c.Assert(query, qt.Contains, "authors(first: Int, after: String): AuthorConnection")
	c.Assert(query, qt.Contains, "author(id: ID!): Author")
	c.Assert(query, qt.Contains, "books(first: Int, after: String): BookConnection")
	c.Assert(query, qt.Contains, "book(id: ID!): Book")
}

func TestRenderRelationToExcludedTableIsOmitted(t *testing.T) {
	c := qt.New(t)
	res, err := graphqlrender.Render(fixture(), graphqlrender.Options{IncludeTables: []string{"books"}})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)

	c.Assert(sdl, qt.Not(qt.Contains), "author: Author")
	c.Assert(sdl, qt.Contains, "  author_id: Int!") // scalar column stays
	var found bool
	for _, d := range res.Diagnostics {
		if strings.Contains(d.Message, "not exported") {
			found = true
		}
	}
	c.Assert(found, qt.IsTrue)
}

func TestRenderInvalidEnumFallsBackToString(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "T", Name: "phase", Type: "enum_phase", Enum: []string{"in-progress", "done"}},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)
	c.Assert(sdl, qt.Not(qt.Contains), "enum ")    // no enum type emitted
	c.Assert(sdl, qt.Contains, "  phase: String!") // fell back to scalar
	c.Assert(len(res.Diagnostics) >= 1, qt.IsTrue)
}

func TestRenderSanitizesInvalidNames(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "2fa_tokens"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "T", Name: "2fa_enabled", Type: "BOOLEAN"},
			{StructName: "T", Name: "user-agent", Type: "TEXT", Nullable: true},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)
	// The digit-leading type name and invalid column names are made legal.
	c.Assert(sdl, qt.Contains, "type _2faToken {")
	c.Assert(sdl, qt.Contains, "  _2fa_enabled: Boolean!")
	c.Assert(sdl, qt.Contains, "  user_agent: String")
	c.Assert(len(res.Diagnostics) >= 2, qt.IsTrue)
}

func TestRenderArrayColumn(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "posts"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "T", Name: "tags", Type: "TEXT[]", Nullable: true},
			{StructName: "T", Name: "scores", Type: "INTEGER[]"},
		},
	}
	sdl := string(mustRender(c, db))
	c.Assert(sdl, qt.Contains, "  tags: [String]")
	c.Assert(sdl, qt.Contains, "  scores: [Int]!")
}

func TestRenderEmptySelectionEmitsPlaceholderQuery(t *testing.T) {
	c := qt.New(t)
	res, err := graphqlrender.Render(fixture(), graphqlrender.Options{IncludeTables: []string{"does_not_exist"}})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)
	// A schema must have a Query root, even when nothing is selected.
	c.Assert(sdl, qt.Contains, "type Query {\n  _empty: Boolean\n}")
	c.Assert(sdl, qt.Not(qt.Contains), "type Author")
}

func mustRender(c *qt.C, db *goschema.Database) []byte {
	res, err := graphqlrender.Render(db, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)
	return res.Data
}

func TestRenderDeterministic(t *testing.T) {
	c := qt.New(t)
	c.Assert(render(c, graphqlrender.Options{}), qt.Equals, render(c, graphqlrender.Options{}))
}

// section returns the block starting at header up to the next blank line, so a
// substring assertion is scoped to one definition.
func section(sdl, header string) string {
	idx := strings.Index(sdl, header)
	if idx < 0 {
		return ""
	}
	rest := sdl[idx:]
	if before, _, ok := strings.Cut(rest, "\n}\n"); ok {
		return before
	}
	return rest
}
