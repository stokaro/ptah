package dbmlparse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/dbmlparse"
)

// bookshop is one document carrying each construct this parser reads.
const bookshop = `// A shop that sells books.
Enum "public"."post_status" {
  draft
  published [note: 'visible to everybody']
}

Table public.users {
  id bigint [pk, increment]
  email varchar(255) [not null, unique, note: 'How we reach them']
}

Table public.posts as P {
  id bigint [pk, increment]
  author_id bigint [not null]
  status post_status [not null, default: 'draft']
  published_at timestamptz [default: ` + "`now()`" + `]

  Indexes {
    (author_id, status) [name: 'posts_author_status_idx']
    email [unique]
  }

  Note: 'Everything anybody wrote'
}

Ref posts_author_fk: public.posts.author_id > public.users.id [delete: cascade, update: no action]
`

// TestParse_ReadsEveryConstructItSupports pins the model one document produces.
func TestParse_ReadsEveryConstructItSupports(t *testing.T) {
	c := qt.New(t)

	db, err := dbmlparse.Parse(bookshop, dbmlparse.Options{File: "schema.dbml"})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Enums, qt.HasLen, 1)
	c.Assert(db.Enums[0].Name, qt.Equals, "post_status")
	c.Assert(db.Enums[0].Schema, qt.Equals, "public")
	c.Assert(db.Enums[0].Values, qt.DeepEquals, []string{"draft", "published"})

	c.Assert(db.Tables, qt.HasLen, 2)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Tables[0].Schema, qt.Equals, "public")
	c.Assert(db.Tables[1].Comment, qt.Equals, "Everything anybody wrote")

	fields := fieldsByName(db, "public.posts")
	c.Assert(fields["id"].Primary, qt.IsTrue)
	c.Assert(fields["id"].AutoInc, qt.IsTrue)
	// `pk` says the column is the key and nothing about NULL, so nullability
	// stays what the document stated -- which for a column that does not say
	// `not null` is nullable. Inferring otherwise made DBML and SQL describe
	// different schemas for one intent.
	c.Assert(fields["id"].Nullable, qt.IsTrue)
	c.Assert(fields["author_id"].Nullable, qt.IsFalse)
	c.Assert(fields["author_id"].Foreign, qt.Equals, "public.users(id)")
	c.Assert(fields["author_id"].ForeignKeyName, qt.Equals, "posts_author_fk")
	c.Assert(fields["author_id"].OnDelete, qt.Equals, "CASCADE")
	c.Assert(fields["author_id"].OnUpdate, qt.Equals, "NO ACTION")
	c.Assert(fields["published_at"].Nullable, qt.IsTrue)

	c.Assert(db.Indexes, qt.HasLen, 2)
}

// TestParse_ALiteralDefaultAndAnExpressionDefaultStayApart is the one that
// decides what a column does.
//
// 'now()' is a six-character string and `now()` is a call. A parser that read
// one as the other would give the column a different value on every insert, and
// the document would still look right.
func TestParse_ALiteralDefaultAndAnExpressionDefaultStayApart(t *testing.T) {
	c := qt.New(t)

	db, err := dbmlparse.Parse(bookshop, dbmlparse.Options{})

	c.Assert(err, qt.IsNil)
	fields := fieldsByName(db, "public.posts")
	c.Assert(fields["status"].Default, qt.Equals, "draft")
	c.Assert(fields["status"].DefaultSet, qt.IsTrue)
	c.Assert(fields["status"].DefaultExpr, qt.Equals, "")
	c.Assert(fields["published_at"].DefaultExpr, qt.Equals, "now()")
	c.Assert(fields["published_at"].DefaultSet, qt.IsFalse)
}

// TestParse_AColumnIsNullableUnlessTheDocumentSaysOtherwise pins the default
// DBML states, which is the opposite of Ptah's zero value.
func TestParse_AColumnIsNullableUnlessTheDocumentSaysOtherwise(t *testing.T) {
	c := qt.New(t)

	db, err := dbmlparse.Parse("Table t {\n  a int\n  b int [not null]\n}\n", dbmlparse.Options{})

	c.Assert(err, qt.IsNil)
	fields := fieldsByName(db, "t")
	c.Assert(fields["a"].Nullable, qt.IsTrue)
	c.Assert(fields["b"].Nullable, qt.IsFalse)
}

// TestParse_RefusesWhatItCannotRepresent covers the constructs that must fail
// rather than be dropped, each with the position a reader needs.
func TestParse_RefusesWhatItCannotRepresent(t *testing.T) {
	rows := []struct {
		name     string
		document string
		message  string
	}{
		{
			name:     "a many-to-many relationship",
			document: "Table a {\n  id int\n}\nTable b {\n  id int\n}\nRef: a.id <> b.id\n",
			message:  "join table",
		},
		{
			name:     "an unsupported column setting",
			document: "Table t {\n  a int [unsupported]\n}\n",
			message:  `unsupported column setting "unsupported"`,
		},
		{
			name:     "API export metadata on a table",
			document: "Table t [api_name: 'Account'] {\n  a int\n}\n",
			message:  `DBML cannot represent export metadata "api_name" on a table`,
		},
		{
			name:     "API export metadata on a column",
			document: "Table t {\n  a int [graphql_name: 'accountId']\n}\n",
			message:  `DBML cannot represent export metadata "graphql_name" on a column`,
		},
		{
			name:     "API export metadata on an enum value",
			document: "Enum status {\n  active [api_name: 'public_active']\n}\n",
			message:  `DBML cannot represent export metadata "api_name" on an enum value`,
		},
		{
			name:     "an unsupported index setting",
			document: "Table t {\n  a int\n\n  Indexes {\n    a [nope]\n  }\n}\n",
			message:  `unsupported index setting "nope"`,
		},
		{
			name:     "a primary key declared in Indexes",
			document: "Table t {\n  a int\n\n  Indexes {\n    a [pk]\n  }\n}\n",
			message:  "declare a primary key on its columns",
		},
		{
			name:     "a reference to a column nobody declared",
			document: "Table a {\n  id int\n}\nRef: a.missing > a.id\n",
			message:  "which no table declares",
		},
		{
			name:     "an unknown declaration",
			document: "Cluster c {\n}\n",
			message:  `unknown declaration "Cluster"`,
		},
		{
			name:     "an unterminated string",
			document: "Table t {\n  a int [note: 'oops]\n}\n",
			message:  "unterminated string",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbmlparse.Parse(row.document, dbmlparse.Options{File: "schema.dbml"})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, row.message)
			c.Assert(err.Error(), qt.Contains, "schema.dbml:")
		})
	}
}

// TestParse_SkipsWhatDescribesTheDiagram pins that presentation-only blocks do
// not fail the document.
//
// A Project block and a TableGroup are legitimate DBML that say nothing about
// schema state. Refusing them would make Ptah reject documents dbdiagram itself
// writes.
func TestParse_SkipsWhatDescribesTheDiagram(t *testing.T) {
	c := qt.New(t)
	document := "Project shop {\n  database_type: 'PostgreSQL'\n}\n\nTable t {\n  a int\n}\n\nTableGroup g {\n  t\n}\n"

	db, err := dbmlparse.Parse(document, dbmlparse.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "t")
}

// fieldsByName indexes one table's columns by column name.
func fieldsByName(db *schemamodel.Database, structName string) map[string]schemamodel.Field {
	fields := make(map[string]schemamodel.Field, len(db.Fields))
	for _, field := range db.Fields {
		if field.StructName != structName {
			continue
		}
		fields[field.Name] = field
	}
	return fields
}
