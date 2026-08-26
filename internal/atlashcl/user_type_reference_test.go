package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// A reference to a type the document declares resolves to that type's name in
// every position that names a type -- issue #2150.
//
// Only a column's `enum.x` was resolved. `domain.x`, `composite.x` and
// `range.x` were written through verbatim, and PostgreSQL read the block
// keyword as a schema qualifier:
//
//	"d" domain.pos NOT NULL  ->  ERROR: schema "domain" does not exist (SQLSTATE 3F000)
//
// which names nothing the operator wrote. The objects themselves were created
// correctly in the same plan, so the blocks were understood; only the reference
// to them was not.
//
// The three positions the issue did not name -- a domain's base type, a
// composite field, a range's subtype, and a function's argument and return --
// carried the same defect and are pinned here with the column.
const userTypeDeclarations = `
schema "public" {
}
enum "mood" {
  schema = schema.public
  values = ["ok", "bad"]
}
domain "pos" {
  schema = schema.public
  type   = integer
}
composite "pair" {
  schema = schema.public
  field "a" {
    type = integer
  }
}
range "span" {
  schema  = schema.public
  subtype = integer
}
`

func TestUserTypeReferenceResolvesInEveryPosition(t *testing.T) {
	tests := []struct {
		name string
		hcl  string
		read func(*schemamodel.Database) string
		want string
	}{
		{
			name: "column enum",
			hcl: `
table "t" {
  schema = schema.public
  column "c" {
    type = enum.mood
  }
}
`,
			read: firstFieldType,
			want: "mood",
		},
		{
			name: "column domain",
			hcl: `
table "t" {
  schema = schema.public
  column "c" {
    type = domain.pos
  }
}
`,
			read: firstFieldType,
			want: "pos",
		},
		{
			name: "column composite",
			hcl: `
table "t" {
  schema = schema.public
  column "c" {
    type = composite.pair
  }
}
`,
			read: firstFieldType,
			want: "pair",
		},
		{
			name: "column range",
			hcl: `
table "t" {
  schema = schema.public
  column "c" {
    type = range.span
  }
}
`,
			read: firstFieldType,
			want: "span",
		},
		{
			// A bare type keyword is not a reference and must survive
			// untouched: the resolver keys on the block prefixes only.
			name: "column bare keyword is untouched",
			hcl: `
table "t" {
  schema = schema.public
  column "c" {
    type = integer
  }
}
`,
			read: firstFieldType,
			want: "integer",
		},
		{
			// A schema named "domain" is a legal PostgreSQL schema, and a
			// qualified type from it is spelled with sql() rather than as a
			// traversal, so the hatch must not be re-read as a reference.
			name: "sql hatch naming a schema called domain is untouched",
			hcl: `
table "t" {
  schema = schema.public
  column "c" {
    type = sql("domain.pos")
  }
}
`,
			read: firstFieldType,
			want: "domain.pos",
		},
		{
			name: "domain base type",
			hcl: `
domain "wrapped" {
  schema = schema.public
  type   = domain.pos
}
`,
			read: lastDomainBaseType,
			want: "pos",
		},
		{
			name: "composite field type",
			hcl: `
composite "holder" {
  schema = schema.public
  field "f" {
    type = domain.pos
  }
}
`,
			read: lastCompositeFieldType,
			want: "pos",
		},
		{
			name: "range subtype",
			hcl: `
range "wrapped" {
  schema  = schema.public
  subtype = domain.pos
}
`,
			read: lastRangeSubtype,
			want: "pos",
		},
		{
			name: "function argument type",
			hcl: `
function "f" {
  schema = schema.public
  arg "a" {
    type = domain.pos
  }
  return = integer
  as     = "SELECT 1"
}
`,
			read: firstFunctionParameters,
			want: "a pos",
		},
		{
			name: "function return type",
			hcl: `
function "f" {
  schema = schema.public
  return = domain.pos
  as     = "SELECT 1"
}
`,
			read: firstFunctionReturns,
			want: "pos",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(userTypeDeclarations+tt.hcl), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(tt.read(db), qt.Equals, tt.want)
		})
	}
}

func firstFieldType(db *schemamodel.Database) string {
	return db.Fields[0].Type
}

func lastDomainBaseType(db *schemamodel.Database) string {
	return db.Domains[len(db.Domains)-1].BaseType
}

func lastCompositeFieldType(db *schemamodel.Database) string {
	return db.CompositeTypes[len(db.CompositeTypes)-1].Fields[0].Type
}

func lastRangeSubtype(db *schemamodel.Database) string {
	return db.Ranges[len(db.Ranges)-1].Subtype
}

func firstFunctionParameters(db *schemamodel.Database) string {
	return db.Functions[0].Parameters
}

func firstFunctionReturns(db *schemamodel.Database) string {
	return db.Functions[0].Returns
}
