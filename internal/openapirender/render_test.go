package openapirender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	yaml "go.yaml.in/yaml/v3"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/openapirender"
)

func fixture() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Author", Name: "authors", Comment: "An author of books"},
			{StructName: "Book", Name: "books"},
		},
		Fields: []goschema.Field{
			{StructName: "Author", Name: "id", Type: "SERIAL", Primary: true, Nullable: true},
			{StructName: "Author", Name: "name", Type: "VARCHAR(255)"},
			{StructName: "Author", Name: "status", Type: "enum_author_status", Nullable: true, Enum: []string{"active", "retired"}},
			{StructName: "Book", Name: "id", Type: "BIGSERIAL", Primary: true, Nullable: true},
			{StructName: "Book", Name: "title", Type: "TEXT"},
			{StructName: "Book", Name: "price", Type: "DECIMAL(10,2)"},
			{StructName: "Book", Name: "published_at", Type: "TIMESTAMP", Nullable: true},
			{StructName: "Book", Name: "in_print", Type: "BOOLEAN"},
			{StructName: "Book", Name: "author_id", Type: "INTEGER", Foreign: "authors(id)"},
			{StructName: "Book", Name: "quirk", Type: "some_unknown_type", Nullable: true},
		},
		Enums: []goschema.Enum{{Name: "enum_author_status", Values: []string{"active", "retired"}}},
	}
}

func renderMap(c *qt.C, opts openapirender.Options) map[string]any {
	res, err := openapirender.Render(fixture(), opts)
	c.Assert(err, qt.IsNil)
	var doc map[string]any
	c.Assert(yaml.Unmarshal(res.Data, &doc), qt.IsNil)
	return doc
}

func TestRenderEnvelope(t *testing.T) {
	c := qt.New(t)
	doc := renderMap(c, openapirender.Options{Title: "My API", Version: "2.0.0"})

	c.Assert(doc["openapi"], qt.Equals, "3.0.3")
	info := doc["info"].(map[string]any)
	c.Assert(info["title"], qt.Equals, "My API")
	c.Assert(info["version"], qt.Equals, "2.0.0")
	// A non-empty servers block is required for a clean lint.
	c.Assert(doc["servers"].([]any), qt.HasLen, 1)
	c.Assert(doc["paths"], qt.HasLen, 0)
}

func TestRenderColumnMappings(t *testing.T) {
	c := qt.New(t)
	doc := renderMap(c, openapirender.Options{})
	schemas := doc["components"].(map[string]any)["schemas"].(map[string]any)

	authors := schemas["authors"].(map[string]any)
	c.Assert(authors["type"], qt.Equals, "object")
	c.Assert(authors["description"], qt.Equals, "An author of books")
	authorProps := authors["properties"].(map[string]any)

	// A primary key is NOT NULL and required even when declared nullable.
	id := authorProps["id"].(map[string]any)
	c.Assert(id["type"], qt.Equals, "integer")
	c.Assert(id["format"], qt.Equals, "int32")
	_, hasNullable := id["nullable"]
	c.Assert(hasNullable, qt.IsFalse)
	c.Assert(contains(authors["required"], "id"), qt.IsTrue)

	name := authorProps["name"].(map[string]any)
	c.Assert(name["type"], qt.Equals, "string")
	c.Assert(name["maxLength"], qt.Equals, 255)

	status := authorProps["status"].(map[string]any)
	c.Assert(status["type"], qt.Equals, "string")
	// A nullable enum must list null as an allowed value under OpenAPI 3.0.
	c.Assert(status["enum"], qt.DeepEquals, []any{"active", "retired", nil})
	c.Assert(status["nullable"], qt.Equals, true)
	c.Assert(contains(authors["required"], "status"), qt.IsFalse)

	books := schemas["books"].(map[string]any)
	bookProps := books["properties"].(map[string]any)
	c.Assert(bookProps["id"].(map[string]any)["format"], qt.Equals, "int64")
	c.Assert(bookProps["price"].(map[string]any)["type"], qt.Equals, "number")
	c.Assert(bookProps["published_at"].(map[string]any)["format"], qt.Equals, "date-time")
	c.Assert(bookProps["published_at"].(map[string]any)["nullable"], qt.Equals, true)
	c.Assert(bookProps["in_print"].(map[string]any)["type"], qt.Equals, "boolean")
}

func TestRenderUnknownTypeDiagnostic(t *testing.T) {
	c := qt.New(t)
	res, err := openapirender.Render(fixture(), openapirender.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(res.Diagnostics, qt.HasLen, 1)
	c.Assert(res.Diagnostics[0].Path, qt.Contains, "books.properties.quirk")
	// The unknown type still renders, as a string.
	doc := renderMap(c, openapirender.Options{})
	quirk := doc["components"].(map[string]any)["schemas"].(map[string]any)["books"].(map[string]any)["properties"].(map[string]any)["quirk"].(map[string]any)
	c.Assert(quirk["type"], qt.Equals, "string")
}

func TestRenderArrayAndUnsigned(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "T", Name: "tags", Type: "TEXT[]", Nullable: true},
			{StructName: "T", Name: "big", Type: "BIGINT UNSIGNED"},
		},
	}
	res, err := openapirender.Render(db, openapirender.Options{})
	c.Assert(err, qt.IsNil)
	var doc map[string]any
	c.Assert(yaml.Unmarshal(res.Data, &doc), qt.IsNil)
	props := doc["components"].(map[string]any)["schemas"].(map[string]any)["t"].(map[string]any)["properties"].(map[string]any)

	tags := props["tags"].(map[string]any)
	c.Assert(tags["type"], qt.Equals, "array")
	c.Assert(tags["items"].(map[string]any)["type"], qt.Equals, "string")

	big := props["big"].(map[string]any)
	c.Assert(big["type"], qt.Equals, "integer")
	c.Assert(big["minimum"], qt.Equals, 0)
}

func TestRenderTableFilter(t *testing.T) {
	c := qt.New(t)
	doc := renderMap(c, openapirender.Options{IncludeTables: []string{"authors"}})
	schemas := doc["components"].(map[string]any)["schemas"].(map[string]any)
	c.Assert(schemas, qt.HasLen, 1)
	_, ok := schemas["authors"]
	c.Assert(ok, qt.IsTrue)
}

func TestRenderDeterministic(t *testing.T) {
	c := qt.New(t)
	first, err := openapirender.Render(fixture(), openapirender.Options{})
	c.Assert(err, qt.IsNil)
	second, err := openapirender.Render(fixture(), openapirender.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(string(first.Data), qt.Equals, string(second.Data))
}

func contains(list any, want string) bool {
	items, ok := list.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
