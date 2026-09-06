package dbmlrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbmlrender"
)

// TestRender_WritesTheSchemaItWasGiven pins the document a schema produces.
//
// A renderer is compared against its whole output rather than against a list of
// substrings, because the parts a substring check does not name are exactly the
// parts that rot: a stray blank line, a setting that moved, a table that
// appeared twice. The schema here carries one of each thing the format can say.
func TestRender_WritesTheSchemaItWasGiven(t *testing.T) {
	c := qt.New(t)

	result, err := dbmlrender.Render(bookshop(), dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.DBML, qt.Equals, `Enum "public"."post_status" {
  "draft"
  "published"
}

Table "public"."posts" {
  "id" BIGINT [pk, increment, not null]
  "author_id" BIGINT [not null]
  "status" "post_status" [not null, default: 'draft', note: 'Where the post is in review']
  "published_at" TIMESTAMPTZ [default: `+"`now()`"+`]

  Indexes {
    ("author_id", "status") [name: "posts_author_status_idx"]
  }

  Note: 'Everything anybody wrote'
}

Table "public"."users" {
  "id" BIGINT [pk, increment, not null]
  "email" TEXT [unique, not null]
}

Ref "posts_author_fk": "public"."posts"."author_id" > "public"."users"."id" [delete: cascade]
`)
}

// TestRender_IsByteDeterministic is the canonical-output contract.
//
// Two renders of one schema have to be the same bytes, and the schema is fed in
// an order a map iteration could not preserve.
func TestRender_IsByteDeterministic(t *testing.T) {
	c := qt.New(t)

	first, err := dbmlrender.Render(bookshop(), dbmlrender.Options{})
	c.Assert(err, qt.IsNil)
	for range 8 {
		again, againErr := dbmlrender.Render(bookshop(), dbmlrender.Options{})
		c.Assert(againErr, qt.IsNil)
		c.Assert(again.DBML, qt.Equals, first.DBML)
	}
}

// TestRender_EndsWithExactlyOneNewlineAndUsesLF pins the two line-ending rules
// separately from the document, so a change to the fixture cannot quietly take
// one of them with it.
func TestRender_EndsWithExactlyOneNewlineAndUsesLF(t *testing.T) {
	c := qt.New(t)

	result, err := dbmlrender.Render(bookshop(), dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasSuffix(result.DBML, "\n"), qt.IsTrue)
	c.Assert(strings.HasSuffix(result.DBML, "\n\n"), qt.IsFalse)
	c.Assert(result.DBML, qt.Not(qt.Contains), "\r")
}

// TestRender_NamesWhatTheFormatCannotCarry pins the loss report.
//
// DBML describes tables, columns, enums, indexes and references. A schema
// holding a view and a trigger renders without them, and a caller that was told
// nothing would hand somebody a file that looks like the whole database.
func TestRender_NamesWhatTheFormatCannotCarry(t *testing.T) {
	c := qt.New(t)
	db := bookshop()
	db.Views = []schemamodel.View{{Name: "recent_posts"}, {Name: "active_users"}}
	db.Triggers = []schemamodel.Trigger{{Name: "posts_audit"}}

	result, err := dbmlrender.Render(db, dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Omitted, qt.DeepEquals, []string{"triggers (1)", "views (2)"})
	c.Assert(result.DBML, qt.Not(qt.Contains), "recent_posts")
}

// TestRender_AnEmptySchemaRendersNothing pins the one case with no trailing
// newline: a document with no blocks is empty rather than a lone newline.
func TestRender_AnEmptySchemaRendersNothing(t *testing.T) {
	c := qt.New(t)

	result, err := dbmlrender.Render(&schemamodel.Database{}, dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.DBML, qt.Equals, "")
	c.Assert(result.Omitted, qt.HasLen, 0)
}

// TestRender_RefusesANilSchema keeps the entry point from rendering a document
// for nothing.
func TestRender_RefusesANilSchema(t *testing.T) {
	c := qt.New(t)

	_, err := dbmlrender.Render(nil, dbmlrender.Options{})

	c.Assert(err, qt.IsNotNil)
}

// TestRender_RefusesExportMetadataBeforeRendering records DBML's deliberate
// expressiveness boundary. A warning beside a usable document is insufficient:
// the document would look canonical while losing the API contract carried by
// the source schema.
func TestRender_RefusesExportMetadataBeforeRendering(t *testing.T) {
	c := qt.New(t)
	db := bookshop()
	db.Tables[1].APIName = "Account"
	db.Fields[5].APINames.GraphQL = "emailAddress"

	result, err := dbmlrender.Render(db, dbmlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`.*DBML cannot represent API export metadata without loss:.*api_name="Account".*graphql_name="emailAddress".*`)
	c.Assert(result.DBML, qt.Equals, "")
	c.Assert(result.Omitted, qt.HasLen, 0)
}

func TestRender_IgnoresMetadataOnAnExcludedTable(t *testing.T) {
	c := qt.New(t)
	db := bookshop()
	db.Tables[0].APIName = "articles"

	result, err := dbmlrender.Render(db, dbmlrender.Options{ExcludeTables: []string{"posts"}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.DBML, qt.Not(qt.Contains), `Table "public"."posts"`)
	c.Assert(result.DBML, qt.Contains, `Table "public"."users"`)
}

// bookshop is one schema carrying one of each thing DBML can say, declared in
// an order the renderer has to sort rather than echo.
func bookshop() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Post", Name: "posts", Schema: "public", Comment: "Everything anybody wrote"},
			{StructName: "User", Name: "users", Schema: "public"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Post", Name: "id", Type: "BIGINT", Primary: true, AutoInc: true},
			{
				StructName: "Post", Name: "author_id", Type: "BIGINT",
				Foreign: "public.users(id)", ForeignKeyName: "posts_author_fk", OnDelete: "CASCADE",
			},
			{
				StructName: "Post", Name: "status", Type: "post_status",
				Enum: []string{"draft", "published"}, Default: "draft", DefaultSet: true,
				Comment: "Where the post is in review",
			},
			{StructName: "Post", Name: "published_at", Type: "TIMESTAMPTZ", Nullable: true, DefaultExpr: "now()"},
			{StructName: "User", Name: "id", Type: "BIGINT", Primary: true, AutoInc: true},
			{StructName: "User", Name: "email", Type: "TEXT", Unique: true},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Post", Name: "posts_author_status_idx", Fields: []string{"author_id", "status"}},
		},
		Enums: []schemamodel.Enum{
			{Name: "post_status", Schema: "public", Values: []string{"draft", "published"}},
		},
	}
}
