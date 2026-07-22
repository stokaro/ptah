package schemaexport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/schemaexport"
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
			{StructName: "Book", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Book", Name: "author_id", Type: "INTEGER", Foreign: "authors(id)"},
			{StructName: "Book", Name: "status", Type: "enum_book_status"},
		},
		Enums: []goschema.Enum{{Name: "enum_book_status", Values: []string{"draft", "published"}}},
	}
}

func TestSelectTables(t *testing.T) {
	c := qt.New(t)
	db := fixture()

	all := schemaexport.SelectTables(db, schemaexport.Options{})
	c.Assert(names(all), qt.DeepEquals, []string{"authors", "books"})

	only := schemaexport.SelectTables(db, schemaexport.Options{IncludeTables: []string{"books"}})
	c.Assert(names(only), qt.DeepEquals, []string{"books"})

	without := schemaexport.SelectTables(db, schemaexport.Options{ExcludeTables: []string{"authors"}})
	c.Assert(names(without), qt.DeepEquals, []string{"books"})

	// Exclude wins over include.
	both := schemaexport.SelectTables(db, schemaexport.Options{
		IncludeTables: []string{"authors", "books"},
		ExcludeTables: []string{"authors"},
	})
	c.Assert(names(both), qt.DeepEquals, []string{"books"})
}

func TestFieldsForAndPrimaryKey(t *testing.T) {
	c := qt.New(t)
	db := fixture()
	books := db.Tables[1]

	fields := schemaexport.FieldsFor(db, books)
	c.Assert(len(fields), qt.Equals, 3)
	c.Assert(schemaexport.EffectivePrimaryKey(books, fields), qt.DeepEquals, []string{"id"})

	// Composite primary key comes from the table.
	composite := goschema.Table{StructName: "M", Name: "m", PrimaryKey: []string{"a", "b"}}
	c.Assert(schemaexport.EffectivePrimaryKey(composite, nil), qt.DeepEquals, []string{"a", "b"})
}

func TestResolveEnumValues(t *testing.T) {
	c := qt.New(t)
	enums := schemaexport.EnumIndex(fixture())

	inline := goschema.Field{Type: "whatever", Enum: []string{"a", "b"}}
	values, ok := schemaexport.ResolveEnumValues(inline, enums)
	c.Assert(ok, qt.IsTrue)
	c.Assert(values, qt.DeepEquals, []string{"a", "b"})

	named := goschema.Field{Type: "enum_book_status"}
	values, ok = schemaexport.ResolveEnumValues(named, enums)
	c.Assert(ok, qt.IsTrue)
	c.Assert(values, qt.DeepEquals, []string{"draft", "published"})

	_, ok = schemaexport.ResolveEnumValues(goschema.Field{Type: "VARCHAR(255)"}, enums)
	c.Assert(ok, qt.IsFalse)
}

func TestParseForeignRef(t *testing.T) {
	c := qt.New(t)

	ref, ok := schemaexport.ParseForeignRef("users(id)")
	c.Assert(ok, qt.IsTrue)
	c.Assert(ref, qt.Equals, schemaexport.ForeignRef{Table: "users", Column: "id"})

	ref, ok = schemaexport.ParseForeignRef("public.orders ( order_id , line )")
	c.Assert(ok, qt.IsTrue)
	c.Assert(ref.Table, qt.Equals, "public.orders")
	c.Assert(ref.Column, qt.Equals, "order_id")

	_, ok = schemaexport.ParseForeignRef("")
	c.Assert(ok, qt.IsFalse)
	_, ok = schemaexport.ParseForeignRef("no_parens")
	c.Assert(ok, qt.IsFalse)
}

func TestNameHelpers(t *testing.T) {
	c := qt.New(t)

	c.Assert(schemaexport.PascalCase("simplified_users"), qt.Equals, "SimplifiedUsers")
	c.Assert(schemaexport.PascalCase("author-id"), qt.Equals, "AuthorId")

	c.Assert(schemaexport.Singularize("users"), qt.Equals, "user")
	c.Assert(schemaexport.Singularize("categories"), qt.Equals, "category")
	c.Assert(schemaexport.Singularize("boxes"), qt.Equals, "box")
	c.Assert(schemaexport.Singularize("address"), qt.Equals, "address")

	c.Assert(schemaexport.TypeName("simplified_users"), qt.Equals, "SimplifiedUser")
	c.Assert(schemaexport.TypeName("categories"), qt.Equals, "Category")

	c.Assert(schemaexport.IsValidGraphQLName("active"), qt.IsTrue)
	c.Assert(schemaexport.IsValidGraphQLName("out_of_stock"), qt.IsTrue)
	c.Assert(schemaexport.IsValidGraphQLName("in-progress"), qt.IsFalse)
	c.Assert(schemaexport.IsValidGraphQLName("2fa"), qt.IsFalse)
	c.Assert(schemaexport.IsValidGraphQLName(""), qt.IsFalse)

	rel, ok := schemaexport.RelationFieldName("author_id")
	c.Assert(ok, qt.IsTrue)
	c.Assert(rel, qt.Equals, "author")
	_, ok = schemaexport.RelationFieldName("title")
	c.Assert(ok, qt.IsFalse)
}

func TestSanitizeGraphQLName(t *testing.T) {
	c := qt.New(t)
	c.Assert(schemaexport.SanitizeGraphQLName("author_id"), qt.Equals, "author_id")
	c.Assert(schemaexport.SanitizeGraphQLName("2fa_enabled"), qt.Equals, "_2fa_enabled")
	c.Assert(schemaexport.SanitizeGraphQLName("user-agent"), qt.Equals, "user_agent")
	c.Assert(schemaexport.SanitizeGraphQLName("naïve"), qt.Equals, "na_ve")
	c.Assert(schemaexport.SanitizeGraphQLName(""), qt.Equals, "_")
	// Every result is a legal GraphQL name.
	for _, in := range []string{"2fa", "a b c", "@x", "9", "___"} {
		c.Assert(schemaexport.IsValidGraphQLName(schemaexport.SanitizeGraphQLName(in)), qt.IsTrue)
	}
}

func TestElementType(t *testing.T) {
	c := qt.New(t)
	el, ok := schemaexport.ElementType("TEXT[]")
	c.Assert(ok, qt.IsTrue)
	c.Assert(el, qt.Equals, "TEXT")

	el, ok = schemaexport.ElementType("VARCHAR(255)")
	c.Assert(ok, qt.IsFalse)
	c.Assert(el, qt.Equals, "VARCHAR(255)")
}

func TestNormalizeType(t *testing.T) {
	c := qt.New(t)

	base, args := schemaexport.NormalizeType("VARCHAR(255)")
	c.Assert(base, qt.Equals, "VARCHAR")
	c.Assert(args, qt.DeepEquals, []string{"255"})

	base, _ = schemaexport.NormalizeType("int unsigned")
	c.Assert(base, qt.Equals, "INT")

	base, _ = schemaexport.NormalizeType("INT AUTO_INCREMENT")
	c.Assert(base, qt.Equals, "INT")

	base, args = schemaexport.NormalizeType("decimal(10, 2)")
	c.Assert(base, qt.Equals, "DECIMAL")
	c.Assert(args, qt.DeepEquals, []string{"10", "2"})
}

func names(tables []goschema.Table) []string {
	out := make([]string, len(tables))
	for i, table := range tables {
		out[i] = table.Name
	}
	return out
}
