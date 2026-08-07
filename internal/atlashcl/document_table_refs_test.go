package atlashcl_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// foreignKeyDocument writes a document whose `posts` table holds one
// single-column foreign key spelled exactly as given.
//
// Everything but the reference and the extra table blocks is fixed, so a
// difference in the parsed Foreign is attributable to the spelling alone.
func foreignKeyDocument(reference, extraTables string) []byte {
	return fmt.Appendf(nil, `
schema "public" {
}

schema "other" {
}

table "users" {
  schema = schema.other
  column "id" {
    type = bigint
  }
}
%s
table "posts" {
  schema = schema.public
  column "author_id" {
    type = bigint
  }
  foreign_key "posts_author_fk" {
    columns     = [column.author_id]
    ref_columns = [%s]
  }
}
`, extraTables, reference)
}

const publicUsersTable = `
table "users" {
  schema = schema.public
  column "id" {
    type = bigint
  }
}
`

// TestParseReadsForeignKeySchemaOffTheReferencedBlock covers the reference
// spellings a single-column foreign key can arrive in.
//
// The first row is the one that made this code exist: it is the pinned Atlas
// community binary v1.3.0's OWN inspect output for a cross-schema foreign key,
// and Ptah used to read it as `users(id)` -- the schema silently gone. The
// multi-column form of the same key never lost it, because goschema.Finalize
// resolves a Constraint's ForeignTable and nothing resolved a Field's Foreign.
func TestParseReadsForeignKeySchemaOffTheReferencedBlock(t *testing.T) {
	tests := []struct {
		name        string
		reference   string
		extraTables string
		want        string
		// why records what the row is holding in place.
		why string
	}{
		{
			name:      "cross-schema short form gains the schema it names",
			reference: "table.users.column.id",
			want:      "other.users(id)",
			why: "the only `users` block is in `other`, so that is the table the reference names;" +
				" this is the spelling the pinned binary writes and the one Ptah now writes",
		},
		{
			name:        "same-schema short form keeps the short name",
			reference:   "table.users.column.id",
			extraTables: publicUsersTable,
			want:        "users(id)",
			why: "`public.users` is in the declaring table's own schema, which every reader of this IR" +
				" already has in hand; writing it out renders `REFERENCES \"main\".\"users\"` on SQLite," +
				" which that engine refuses with `near \".\": syntax error`",
		},
		{
			name:      "author-written schema survives",
			reference: "table.other.users.column.id",
			want:      "other.users(id)",
			why:       "this restores what a spelling drops; it never rewrites what the author wrote",
		},
		{
			name:        "author-written schema survives even when it is the declaring table's own",
			reference:   "table.public.users.column.id",
			extraTables: publicUsersTable,
			want:        "public.users(id)",
			why: "the row above cannot tell the difference -- resolving that reference reaches the same" +
				" qualified name either way. This one can: shortening rather than reading is the only" +
				" way to lose the `public.` the author typed",
		},
		{
			name:      "absent target keeps what was written",
			reference: "table.ghost.column.id",
			want:      "ghost(id)",
			why:       "no block declares `ghost`, so there is no schema to read off anything",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(foreignKeyDocument(test.reference, test.extraTables), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(fieldByName(db.Fields, "posts", "author_id").Foreign, qt.Equals, test.want,
				qt.Commentf("%s", test.why))
		})
	}
}

// TestParseLeavesAmbiguousForeignKeyReferenceAsWritten pins the direction a
// wrong answer must not go.
//
// Two tables of one name in different schemas are legal, and the pinned binary
// refuses a document with an unqualified reference to them -- `specutil: failed
// converting to *schema.Realm: multiple reference tables found for "users"` --
// rather than picking one. Ptah keeps the name as written for the same reason:
// a reference that resolves to the wrong table is worse than one that resolves
// to nothing, because nothing about the result says it happened.
func TestParseLeavesAmbiguousForeignKeyReferenceAsWritten(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(foreignKeyDocument("table.users.column.id", `
table "users" {
  schema = schema.billing
  column "id" {
    type = bigint
  }
}

schema "billing" {
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(fieldByName(db.Fields, "posts", "author_id").Foreign, qt.Equals, "users(id)")
}

// TestParseReadsManagedDataSchemaOffTheReferencedBlock covers the other
// position goschema.Finalize does not reach.
//
// A `data` block has no owning table, so nothing else in it carries a schema:
// either the referenced block supplies one or the block points at whichever
// `users` the reader assumes.
func TestParseReadsManagedDataSchemaOffTheReferencedBlock(t *testing.T) {
	tests := []struct {
		name        string
		extraTables string
		wantSchema  string
		why         string
	}{
		{
			name:       "short form gains the schema of the block it names",
			wantSchema: "other",
			why:        "the only `users` block is in `other`",
		},
		{
			name:        "ambiguous name resolves to nothing",
			extraTables: publicUsersTable,
			wantSchema:  "",
			why:         "two `users` blocks make the short form mean neither; picking one would be silent and wrong",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := fmt.Appendf(nil, `
schema "public" {
}

schema "other" {
}

table "users" {
  schema = schema.other
  column "id" {
    type = bigint
  }
}
%s
data {
  table = table.users
  keys  = ["id"]
  file  = "seed.csv"
}
`, test.extraTables)

			db, err := atlashcl.Parse(source, "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.ManagedData, qt.HasLen, 1)
			c.Assert(db.ManagedData[0].Table, qt.Equals, "users")
			c.Assert(db.ManagedData[0].Schema, qt.Equals, test.wantSchema, qt.Commentf("%s", test.why))
		})
	}
}
