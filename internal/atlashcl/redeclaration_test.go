package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// Fixture sources shared by more than one test, so a kind's document is written
// once and every test that has an opinion about it reads the same bytes.
const (
	viewTwiceSource = `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
view "active_users" {
  schema = schema.public
  as     = "SELECT id FROM users"
}
view "active_users" {
  schema = schema.public
  as     = "SELECT id FROM users"
}
`

	materializedTwiceSource = `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
materialized "user_stats" {
  schema = schema.public
  as     = "SELECT count(*) AS n FROM users"
}
materialized "user_stats" {
  schema = schema.public
  as     = "SELECT count(*) AS n FROM users"
}
`

	roleTwiceSource = `schema "public" {}
role "app_user" {
  login = true
}
role "app_user" {
  login = true
}
`

	uniqueTwiceSource = `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  column "email" { type = text }
  unique "users_email_key" {
    columns = [column.email]
  }
  unique "users_email_key" {
    columns = [column.email]
  }
}
`

	enumInTwoSchemasSource = `schema "public" {}
schema "other" {}
enum "mood" {
  schema = schema.public
  values = ["happy", "sad"]
}
enum "mood" {
  schema = schema.other
  values = ["happy", "sad"]
}
`

	tableTwiceSource = `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
`

	twoUnlabeledChecksSource = `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  check {
    expr = "id > 0"
  }
  check {
    expr = "id < 100"
  }
}
`
)

// documentCounts is how many objects of each kind a document reads as. It is
// the accepted rows' `want`, and it is a whole vector rather than the one number
// each row first asked about: a parser that stopped reading views could
// otherwise pass every row that counts tables.
type documentCounts struct {
	Schemas           int
	Tables            int
	Fields            int
	Constraints       int
	Enums             int
	Views             int
	MaterializedViews int
	Roles             int
	Functions         int
}

// countDocument projects a parsed document onto the kinds these fixtures
// declare. It asserts nothing; it is how a row states its verdict as data.
func countDocument(db *goschema.Database) documentCounts {
	return documentCounts{
		Schemas:           len(db.Schemas),
		Tables:            len(db.Tables),
		Fields:            len(db.Fields),
		Constraints:       len(db.Constraints),
		Enums:             len(db.Enums),
		Views:             len(db.Views),
		MaterializedViews: len(db.MaterializedViews),
		Roles:             len(db.Roles),
		Functions:         len(db.Functions),
	}
}

// TestParse_RefusesARedeclaredObject pins the DEFAULT refusal for every object
// kind an HCL document can declare twice.
//
// One rule decides which kinds refuse, and it is measured rather than chosen: a
// repeat is refused by default exactly where the pinned Atlas community binary
// v1.3.0 refuses the same document. Measured on PostgreSQL 17.10 with a
// throwaway dev database dropped and recreated between runs,
// `schema inspect -u file://<fixture>.hcl --dev-url postgres://…`, exit codes
// read from unpiped invocations, that binary exits 1 on the repeated `table`,
// `enum`, `index`, `column`, named `check` and `foreign_key` fixtures while Ptah
// exited 0 and rendered one of each.
//
// The documents that must NOT refuse are the other half of the same rule, and
// they live in TestParse_ReadsADocumentTheBinaryReadsAtExitZero. Without them
// this test is passed by a parser that refuses everything.
func TestParse_RefusesARedeclaredObject(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantErr string
	}{
		{
			name:    "a table declared twice",
			source:  tableTwiceSource,
			wantErr: `parse HCL schema schema.hcl: table "public.users" is declared more than once;.*`,
		},
		{
			name: "an enum declared twice",
			source: `schema "public" {}
enum "mood" {
  schema = schema.public
  values = ["happy", "sad"]
}
enum "mood" {
  schema = schema.public
  values = ["happy", "sad"]
}
`,
			wantErr: `parse HCL schema schema.hcl: enum "mood" is declared more than once;.*`,
		},
		{
			// One enum name in two schemas is one enum by DEFAULT, because that is
			// what the pinned binary refuses: `duplicate enum "mood"`, exit 1,
			// measured, for this document and for its two-label spelling alike.
			// Ptah models both types and reads them under
			// SchemaScopedEnumsEnvVar; the default is the drop-in floor.
			//
			// The message must name the variable that reads the two apart, not
			// the one that merges them: merging deletes a type, so advising it
			// here would answer a lossless question with a lossy answer.
			name:   "an enum name declared in two schemas",
			source: enumInTwoSchemasSource,
			wantErr: `parse HCL schema schema.hcl: enum "mood" is declared more than once; ` +
				`"public.mood" and "other.mood" are two objects Ptah models and one object ` +
				`on the Atlas-compatible surface, which keys this kind by its bare name ` +
				`\(set PTAH_HCL_SCHEMA_SCOPED_ENUMS=1 to read them as two\)`,
		},
		{
			name: "a column declared twice in one table",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  column "id" { type = integer }
}
`,
			wantErr: `parse HCL schema schema.hcl: column "users.id" is declared more than once;.*`,
		},
		{
			name: "an index declared twice in one table",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  index "idx_users_id" {
    columns = [column.id]
  }
  index "idx_users_id" {
    columns = [column.id]
  }
}
`,
			wantErr: `parse HCL schema schema.hcl: index "public.users.idx_users_id" is declared more than once;.*`,
		},
		{
			name: "a named check constraint declared twice in one table",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  check "id_positive" {
    expr = "id > 0"
  }
  check "id_positive" {
    expr = "id > 0"
  }
}
`,
			wantErr: `parse HCL schema schema.hcl: constraint "public.users.id_positive" is declared more than once;.*`,
		},
		{
			// The member the class fix missed. A SINGLE-column foreign key is not
			// a goschema.Constraint at all -- it is written onto the referencing
			// field -- so a ledger built from db.Constraints could never see it,
			// and the document was exit 0 with one key rendered. Measured on the
			// pinned binary: `create "posts" table: pq: constraint
			// "posts_author_fk" for relation "posts" already exists (42710)`,
			// exit 1.
			name: "a single-column foreign key declared twice in one table",
			source: `schema "public" {}
table "authors" {
  schema = schema.public
  column "id" { type = integer }
}
table "posts" {
  schema = schema.public
  column "id" { type = integer }
  column "author_id" { type = integer }
  foreign_key "posts_author_fk" {
    columns     = [column.author_id]
    ref_columns = [table.authors.column.id]
  }
  foreign_key "posts_author_fk" {
    columns     = [column.author_id]
    ref_columns = [table.authors.column.id]
  }
}
`,
			wantErr: `parse HCL schema schema.hcl: foreign key "public.posts.posts_author_fk" is declared more than once;.*`,
		},
		{
			// The other shape of the same block. A multi-column key DOES become a
			// constraint, so this row and the one above must reach the same
			// verdict through two different code paths; a fix that covers only
			// one of them leaves half the block kind open.
			name: "a multi-column foreign key declared twice in one table",
			source: `schema "public" {}
table "authors" {
  schema = schema.public
  column "id" { type = integer }
  column "tenant" { type = integer }
}
table "posts" {
  schema = schema.public
  column "author_id" { type = integer }
  column "tenant" { type = integer }
  foreign_key "posts_author_fk" {
    columns     = [column.author_id, column.tenant]
    ref_columns = [table.authors.column.id, table.authors.column.tenant]
  }
  foreign_key "posts_author_fk" {
    columns     = [column.author_id, column.tenant]
    ref_columns = [table.authors.column.id, table.authors.column.tenant]
  }
}
`,
			wantErr: `parse HCL schema schema.hcl: foreign key "public.posts.posts_author_fk" is declared more than once;.*`,
		},
		{
			name: "a named constraint block declared twice in one table",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  constraint "users_id_excl" {
    type     = "EXCLUDE"
    using    = "gist"
    elements = "id WITH ="
  }
  constraint "users_id_excl" {
    type     = "EXCLUDE"
    using    = "gist"
    elements = "id WITH ="
  }
}
`,
			wantErr: `parse HCL schema schema.hcl: constraint "public.users.users_id_excl" is declared more than once;.*`,
		},
		{
			name: "a sequence declared twice",
			source: `schema "public" {}
sequence "order_seq" {
  schema = schema.public
  start  = 1000
}
sequence "order_seq" {
  schema = schema.public
  start  = 1000
}
`,
			wantErr: `parse HCL schema schema.hcl: sequence "public.order_seq" is declared more than once;.*`,
		},
		{
			name: "a domain declared twice",
			source: `schema "public" {}
domain "email" {
  schema = schema.public
  type   = text
}
domain "email" {
  schema = schema.public
  type   = text
}
`,
			wantErr: `parse HCL schema schema.hcl: domain "public.email" is declared more than once;.*`,
		},
		{
			name: "a composite type declared twice",
			source: `schema "public" {}
composite "address" {
  schema = schema.public
  field "street" {
    type = text
  }
}
composite "address" {
  schema = schema.public
  field "street" {
    type = text
  }
}
`,
			wantErr: `parse HCL schema schema.hcl: composite type "public.address" is declared more than once;.*`,
		},
		{
			name: "a range declared twice",
			source: `schema "public" {}
range "floatrange" {
  schema  = schema.public
  subtype = float8
}
range "floatrange" {
  schema  = schema.public
  subtype = float8
}
`,
			wantErr: `parse HCL schema schema.hcl: range "public.floatrange" is declared more than once;.*`,
		},
		{
			name: "an extension declared twice",
			source: `schema "public" {}
extension "pg_trgm" {
  version = "1.6"
}
extension "pg_trgm" {
  version = "1.6"
}
`,
			wantErr: `parse HCL schema schema.hcl: extension "pg_trgm" is declared more than once;.*`,
		},
		{
			name: "a trigger declared twice",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
trigger "users_touch" {
  on = table.users
  before {
    update = true
  }
  for = ROW
  as  = "RETURN NEW;"
}
trigger "users_touch" {
  on = table.users
  before {
    update = true
  }
  for = ROW
  as  = "RETURN NEW;"
}
`,
			wantErr: `parse HCL schema schema.hcl: trigger "users.users_touch" is declared more than once;.*`,
		},
		{
			name: "a row-level security policy declared twice",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
policy "users_tenant" {
  on    = table.users
  for   = SELECT
  to    = [PUBLIC]
  using = "true"
}
policy "users_tenant" {
  on    = table.users
  for   = SELECT
  to    = [PUBLIC]
  using = "true"
}
`,
			wantErr: `parse HCL schema schema.hcl: policy "users.users_tenant" is declared more than once;.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.Parse([]byte(test.source), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestParse_ReadsADocumentTheBinaryReadsAtExitZero is what keeps the refusal in
// TestParse_RefusesARedeclaredObject from being a cheaper, wronger rule. Each
// row is a document with two declarations that are NOT the same object, or one
// whose repeat the pinned Atlas community binary v1.3.0 reads at exit 0:
// schemas repeat legitimately, `users` and `Users` reach DDL quoted and are two
// relations, one name in two schemas is two tables, a column, index or
// constraint name belongs to its table, and `view`, `materialized`, `role`,
// `unique`, `primary_key`, `row_security` and `variable` are all exit 0 on that
// binary.
//
// Every row states the whole count vector rather than the single number it was
// first written with, so a row cannot go on passing while the parser stops
// reading a kind it does not name.
func TestParse_ReadsADocumentTheBinaryReadsAtExitZero(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   documentCounts
	}{
		{
			// Measured on the pinned binary: exit 0, the repeated block dropped
			// unread, its inspect output holding neither the object nor a
			// diagnostic. Refusing it is above the drop-in floor, so it moved
			// behind StrictRedeclarationsEnvVar.
			name:   "a view declared twice is read at exit 0 like the binary reads it",
			source: viewTwiceSource,
			want:   documentCounts{Schemas: 1, Tables: 1, Fields: 1, Views: 1},
		},
		{
			name:   "a materialized view declared twice is read at exit 0 like the binary reads it",
			source: materializedTwiceSource,
			want:   documentCounts{Schemas: 1, Tables: 1, Fields: 1, MaterializedViews: 1},
		},
		{
			name:   "a role declared twice is read at exit 0 like the binary reads it",
			source: roleTwiceSource,
			want:   documentCounts{Schemas: 1, Roles: 1},
		},
		{
			// Measured on the pinned binary: exit 0, the two blocks merged into
			// one `unique "users_email_key"` in its inspect output. A named
			// `check` in the same slice reaches the engine twice and IS refused,
			// so the two constraint kinds cannot share one verdict.
			name:   "a unique constraint declared twice is read at exit 0 like the binary reads it",
			source: uniqueTwiceSource,
			want:   documentCounts{Schemas: 1, Tables: 1, Fields: 2, Constraints: 1},
		},
		{
			// Measured on the pinned binary: exit 0, one primary_key in its
			// inspect output. A primary key carries no name, so there is no
			// identity for this ledger to collide on either.
			name: "a primary key declared twice is read at exit 0 like the binary reads it",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  primary_key { columns = [column.id] }
  primary_key { columns = [column.id] }
}
`,
			want: documentCounts{Schemas: 1, Tables: 1, Fields: 1},
		},
		{
			// Measured on the pinned binary: exit 0, the block dropped unread.
			// Two of them enable the same thing twice, which PostgreSQL accepts.
			name: "row security enabled twice is read at exit 0 like the binary reads it",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  row_security { enabled = true }
  row_security { enabled = true }
}
`,
			want: documentCounts{Schemas: 1, Tables: 1, Fields: 1},
		},
		{
			// Measured on the pinned binary: exit 0, the default substituted into
			// the column. A `variable` block declares no database object.
			name: "a variable declared twice is read at exit 0 like the binary reads it",
			source: `variable "tenant" {
  type    = string
  default = "public"
}
variable "tenant" {
  type    = string
  default = "public"
}
schema "public" {}
table "users" {
  schema = schema.public
  column "id" {
    type = integer
  }
}
`,
			want: documentCounts{Schemas: 1, Tables: 1, Fields: 1},
		},
		{
			// The pinned binary reads this document at exit 0, measured, and it is
			// also the shape of an HCL schema DIRECTORY, whose files each open
			// with the same schema block. How many schemas a document may declare
			// is decided elsewhere, against the run's URL scope (stokaro/ptah#1231).
			name: "a schema declared twice is not a redeclaration",
			source: `schema "public" {}
schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
`,
			want: documentCounts{Schemas: 1, Tables: 1, Fields: 1},
		},
		{
			// Measured: the pinned binary reads this document at exit 0 and its
			// inspect output holds two tables. An HCL label reaches DDL quoted, so
			// these are two relations rather than two spellings of one.
			name: "two table names differing only in case are two tables",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
table "Users" {
  schema = schema.public
  column "id" { type = integer }
}
`,
			want: documentCounts{Schemas: 1, Tables: 2, Fields: 2},
		},
		{
			name: "one table name in two schemas is two tables",
			source: `schema "public" {}
schema "other" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
table "users" {
  schema = schema.other
  column "id" { type = integer }
}
`,
			want: documentCounts{Schemas: 2, Tables: 2, Fields: 2},
		},
		{
			name: "one column name in two tables is two columns",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
table "orders" {
  schema = schema.public
  column "id" { type = integer }
}
`,
			want: documentCounts{Schemas: 1, Tables: 2, Fields: 2},
		},
		{
			// A constraint name is scoped to its table on every engine Ptah
			// targets, so one foreign key name used once in each of two tables is
			// two keys. Without this row the foreign-key identity could drop the
			// table and still pass every refusing row.
			name: "one foreign key name in two tables is two foreign keys",
			source: `schema "public" {}
table "authors" {
  schema = schema.public
  column "id" { type = integer }
}
table "posts" {
  schema = schema.public
  column "author_id" { type = integer }
  foreign_key "author_fk" {
    columns     = [column.author_id]
    ref_columns = [table.authors.column.id]
  }
}
table "comments" {
  schema = schema.public
  column "author_id" { type = integer }
  foreign_key "author_fk" {
    columns     = [column.author_id]
    ref_columns = [table.authors.column.id]
  }
}
`,
			want: documentCounts{Schemas: 1, Tables: 3, Fields: 3},
		},
		{
			// An unlabeled check block is named after its table and its ordinal, so
			// two of them are two constraints rather than one declared twice. This
			// row is what stops the refusal from resting on a name the parser
			// synthesizes identically for every unlabeled block; the names
			// themselves are pinned by
			// TestParse_UnlabeledCheckConstraintsAreNamedApart.
			name:   "two unlabeled check constraints in one table",
			source: twoUnlabeledChecksSource,
			want:   documentCounts{Schemas: 1, Tables: 1, Fields: 1, Constraints: 2},
		},
		{
			// A PostgreSQL function's identity includes its argument types, so two
			// blocks sharing a name can be two legal overloads. Refusing them by
			// name would refuse a document PostgreSQL accepts.
			//
			// Functions is 1, and that is the measured value rather than the
			// intended one: the document is READ rather than refused, which is
			// what this row is here for, but the overload is not modeled
			// separately. Stating the count is how that stops being invisible --
			// the row asserted only "no error" before, so nothing said which of
			// the two blocks survived.
			name: "two function blocks sharing a name are not a redeclaration",
			source: `schema "public" {}
function "get_tenant" {
  schema = schema.public
  lang   = SQL
  return = text
  as     = "SELECT 'x'"
}
function "get_tenant" {
  schema = schema.public
  lang   = SQL
  return = text
  params = "id integer"
  as     = "SELECT 'y'"
}
`,
			want: documentCounts{Schemas: 1, Functions: 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.source), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(countDocument(db), qt.DeepEquals, test.want)
		})
	}
}

// TestParse_UnlabeledCheckConstraintsAreNamedApart is the half of the unlabeled
// check row that a count cannot state: the two constraints must carry DIFFERENT
// synthesized names. Two constraints named identically would collide in the
// redeclaration ledger the moment either gained a name of its own.
func TestParse_UnlabeledCheckConstraintsAreNamedApart(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(twoUnlabeledChecksSource), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Constraints, qt.HasLen, 2)
	c.Assert(db.Constraints[0].Name, qt.Not(qt.Equals), db.Constraints[1].Name)
}

// TestParse_StrictRedeclarationsEnvVarRefusesWhatTheBinaryReads covers the four
// kinds whose repeat the pinned Atlas community binary v1.3.0 reads at exit 0.
//
// The default is parity with that binary, which is the compatibility floor. The
// variable is the half above the floor: Ptah MODELS views, materialized views,
// roles and unique constraints, and the directory loader already refuses a
// repeat of each across files, so an operator can ask for the same answer within
// one file.
//
// The document that must NOT refuse under the variable is the point of the
// setting as much as these are, and it is
// TestParse_StrictRedeclarationsEnvVarStillReadsARepeatedSchemaBlock. A variable
// that also reached `schema` would be a second name for "refuse everything".
func TestParse_StrictRedeclarationsEnvVarRefusesWhatTheBinaryReads(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantErr string
	}{
		{
			name:    "a view declared twice",
			source:  viewTwiceSource,
			wantErr: `parse HCL schema schema.hcl: view "public.active_users" is declared more than once;.*`,
		},
		{
			name:    "a materialized view declared twice",
			source:  materializedTwiceSource,
			wantErr: `parse HCL schema schema.hcl: materialized view "public.user_stats" is declared more than once;.*`,
		},
		{
			name:    "a role declared twice",
			source:  roleTwiceSource,
			wantErr: `parse HCL schema schema.hcl: role "app_user" is declared more than once;.*`,
		},
		{
			name:    "a unique constraint declared twice",
			source:  uniqueTwiceSource,
			wantErr: `parse HCL schema schema.hcl: unique constraint "public.users.users_email_key" is declared more than once;.*`,
		},
		{
			// Above the floor, not instead of it: the default rule keeps working
			// with the variable set.
			name:    "a table declared twice still refuses",
			source:  tableTwiceSource,
			wantErr: `parse HCL schema schema.hcl: table "public.users" is declared more than once;.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlashcl.StrictRedeclarationsEnvVar, "1")

			_, err := atlashcl.Parse([]byte(test.source), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestParse_StrictRedeclarationsEnvVarStillReadsARepeatedSchemaBlock pins the
// exemption that holds under every setting. A repeated `schema` block is the
// layout of an HCL schema directory, and stokaro/ptah#1231 decides how many
// schemas a run may reach.
func TestParse_StrictRedeclarationsEnvVarStillReadsARepeatedSchemaBlock(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlashcl.StrictRedeclarationsEnvVar, "1")

	db, err := atlashcl.Parse([]byte(`schema "public" {}
schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Schemas, qt.HasLen, 1)
}

// TestParse_SchemaScopedEnumsEnvVarReadsTwoSchemasAsTwoEnums covers the enum
// identity, which is the one identity in this ledger that moves.
//
// By default it is the BARE name, because the pinned Atlas community binary
// v1.3.0 keys enums that way and refuses `enum "mood"` in two schemas with
// `duplicate enum "mood"` at exit 1 -- measured on PostgreSQL 17.10, for the
// one-label spelling and for the two-label spelling alike. Exiting 0 there is
// the direction the drop-in rule forbids.
//
// With the variable set the identity is the qualified name, which is what the
// two objects are. That is the setting under which Ptah's own inspect output for
// a realm holding public.mood and other.mood is readable again -- the round trip
// the pinned binary does not have, because it refuses the document its own
// inspect writes.
func TestParse_SchemaScopedEnumsEnvVarReadsTwoSchemasAsTwoEnums(t *testing.T) {
	tests := []struct {
		name   string
		source string
	}{
		{
			name:   "one label per schema is two enums",
			source: enumInTwoSchemasSource,
		},
		{
			name: "the two-label spelling is two enums",
			source: `schema "public" {}
schema "other" {}
enum "public" "mood" {
  values = ["happy", "sad"]
}
enum "other" "mood" {
  values = ["happy", "sad"]
}
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlashcl.SchemaScopedEnumsEnvVar, "1")

			db, err := atlashcl.Parse([]byte(test.source), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Enums, qt.HasLen, 2)
		})
	}
}

// TestParse_SchemaScopedEnumsEnvVarStillRefusesARedeclaredEnum is what keeps the
// variable from being read as "stop checking enums". The same schema twice is
// one object declared twice whatever the identity rule is, and the setting's
// off spelling has to leave the bare-name identity the pinned binary uses.
func TestParse_SchemaScopedEnumsEnvVarStillRefusesARedeclaredEnum(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		source  string
		wantErr string
	}{
		{
			name:  "the same schema twice still refuses",
			value: "1",
			source: `schema "public" {}
enum "mood" {
  schema = schema.public
  values = ["happy", "sad"]
}
enum "mood" {
  schema = schema.public
  values = ["happy", "sad"]
}
`,
			wantErr: `.*enum "public.mood" is declared more than once;.*`,
		},
		{
			name:    "0 keeps the bare-name identity the binary uses",
			value:   "0",
			source:  enumInTwoSchemasSource,
			wantErr: `.*enum "mood" is declared more than once;.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlashcl.SchemaScopedEnumsEnvVar, test.value)

			_, err := atlashcl.Parse([]byte(test.source), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestParse_SchemaScopedEnumsEnvVarRefusesAnUnparsableValue is the strict-grammar
// rule every boolean PTAH_* variable follows (stokaro/ptah#1334): an unreadable
// value is a configuration error naming the variable, not a silent false, so a
// typo in a CI environment file cannot look like it worked.
func TestParse_SchemaScopedEnumsEnvVarRefusesAnUnparsableValue(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlashcl.SchemaScopedEnumsEnvVar, "yes-please")

	_, err := atlashcl.Parse([]byte(enumInTwoSchemasSource), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "yes-please" for PTAH_HCL_SCHEMA_SCOPED_ENUMS`)
}

// TestParse_AcceptsATwoLabelEnumBlock pins the spelling the pinned Atlas
// community binary v1.3.0 writes for an ambiguous enum name.
//
// Measured on PostgreSQL 17.10, its `schema inspect` of a database holding
// public.mood and other.mood emits `enum "other" "mood"` and
// `enum "public" "mood"`, and it reads a document holding one such block at
// exit 0. Ptah refused that document with "enum block requires exactly one
// label", so a file that binary wrote could not be read here at all.
//
// Each row states the enum's whole identity -- name and schema -- rather than
// the half it was first written to check.
func TestParse_AcceptsATwoLabelEnumBlock(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		wantName   string
		wantSchema string
	}{
		{
			name: "the schema label names the enum's schema",
			source: `schema "public" {}
enum "public" "mood" {
  values = ["happy", "sad"]
}
`,
			wantName:   "mood",
			wantSchema: "public",
		},
		{
			name: "one label still means the schema attribute",
			source: `schema "other" {}
enum "mood" {
  schema = schema.other
  values = ["happy", "sad"]
}
`,
			wantName:   "mood",
			wantSchema: "other",
		},
		{
			// Agreeing with the attribute is what the binary's own output does,
			// so it must not be an error.
			name: "a schema label agreeing with the attribute is accepted",
			source: `schema "other" {}
enum "other" "mood" {
  schema = schema.other
  values = ["happy", "sad"]
}
`,
			wantName:   "mood",
			wantSchema: "other",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.source), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Enums, qt.HasLen, 1)
			c.Assert(db.Enums[0].Name, qt.Equals, test.wantName)
			c.Assert(db.Enums[0].Schema, qt.Equals, test.wantSchema)
		})
	}
}

// TestParse_RefusesAnEnumBlockWhoseLabelsDoNotNameOneEnum is the boundary of the
// two-label spelling: labels that contradict the schema attribute name two
// different enums, and three labels name none.
func TestParse_RefusesAnEnumBlockWhoseLabelsDoNotNameOneEnum(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantErr string
	}{
		{
			name: "a schema label contradicting the attribute is refused",
			source: `schema "public" {}
schema "other" {}
enum "other" "mood" {
  schema = schema.public
  values = ["happy", "sad"]
}
`,
			wantErr: `.*enum "mood" schema label conflicts with schema attribute "public".*`,
		},
		{
			name: "three labels are refused",
			source: `schema "public" {}
enum "a" "b" "c" {
  values = ["happy"]
}
`,
			wantErr: `.*enum block requires one or two labels.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.Parse([]byte(test.source), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestParse_MergeRedeclarationsEnvVarRestoresTheMerge covers the half of the
// compatibility rule that is not about exit codes: a capability is never
// removed, only defaulted away from. The merge is how one entity seen twice
// becomes one object, and setting the variable brings it back on this same
// surface.
func TestParse_MergeRedeclarationsEnvVarRestoresTheMerge(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "1 merges the redeclaration", value: "1"},
		{name: "true merges the redeclaration", value: "true"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlashcl.MergeRedeclarationsEnvVar, test.value)

			db, err := atlashcl.Parse([]byte(tableTwiceSource), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
		})
	}
}

// TestParse_MergeRedeclarationsEnvVarLeavesTheDocumentRefused is the other half
// of the same variable.
//
// The false row exists because the variable's whole job is to be an opt-in: a
// false spelling must leave the refusal in place. The unparsable and empty rows
// are the strict-grammar rule every boolean PTAH_* variable now follows
// (stokaro/ptah#1334): they are configuration errors naming the variable, not
// silently-false values, so a typo in a CI environment file cannot look like it
// worked.
func TestParse_MergeRedeclarationsEnvVarLeavesTheDocumentRefused(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{
			name:    "0 keeps the refusal",
			value:   "0",
			wantErr: `.*table "public.users" is declared more than once;.*`,
		},
		{
			name:    "an unparsable value is a configuration error",
			value:   "yes-please",
			wantErr: `invalid boolean value "yes-please" for PTAH_HCL_MERGE_REDECLARATIONS`,
		},
		{
			name:    "an empty value is a configuration error",
			value:   "",
			wantErr: `invalid boolean value "" for PTAH_HCL_MERGE_REDECLARATIONS`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlashcl.MergeRedeclarationsEnvVar, test.value)

			_, err := atlashcl.Parse([]byte(tableTwiceSource), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
