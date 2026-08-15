package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// A column type written with Atlas's sql() escape hatch reduces to its SQL text
// -- issue #1106 -- but the reduction must not erase the fact that the hatch was
// used, because writing the type back bare is not a spelling Atlas accepts.
//
// Measured on the pinned Atlas community binary v1.3.0:
//
//	type = sql("USER_DEFINED")  -> plans CREATE TABLE `t` (`c` USER_DEFINED NULL), exit 0
//	type = USER_DEFINED         -> Error: Unknown column.type; There is no type
//	                               named "USER_DEFINED", exit 1
//
// So Type carries the SQL and TypeRawSQL carries the spelling obligation.
//
// With TypeRawSQL removed from the parser, the first row below fails on
// TypeRawSQL being false while Type stays correct -- the exact split that made
// issue #1138 look like a rendering bug rather than a parsing one.
func TestParseMarksSQLRawExpressionColumnTypes(t *testing.T) {
	tests := []struct {
		name       string
		hcl        string
		wantType   string
		wantRawSQL bool
	}{
		{
			name: "sql call around a type the dialect does not model",
			hcl: `
table "t" {
  column "c" { type = sql("USER_DEFINED") }
}
`,
			wantType:   "USER_DEFINED",
			wantRawSQL: true,
		},
		{
			// The argument's case is carried through untouched. The community
			// binary preserves it too: sql("user_defined") plans
			// `c user_defined` and inspects back as sql("user_defined").
			name: "sql call keeps the argument's case",
			hcl: `
table "t" {
  column "c" { type = sql("user_defined") }
}
`,
			wantType:   "user_defined",
			wantRawSQL: true,
		},
		{
			name: "sql call around a type the dialect does model",
			hcl: `
table "t" {
  column "c" { type = sql("varchar(10)") }
}
`,
			wantType:   "varchar(10)",
			wantRawSQL: true,
		},
		{
			// Negative control: the ordinary spelling must not acquire the
			// marker, or every rendered type becomes a sql() call.
			name: "bare type keyword",
			hcl: `
table "t" {
  column "c" { type = text }
}
`,
			wantType:   "text",
			wantRawSQL: false,
		},
		{
			// Negative control: a parameterized type is a call expression to
			// HCL, so a marker keyed on "is a function call" rather than on the
			// function's NAME would fire here.
			name: "parameterized type keyword",
			hcl: `
table "t" {
  column "c" { type = varchar(10) }
}
`,
			wantType:   "varchar(10)",
			wantRawSQL: false,
		},
		{
			// Negative control: a quoted type is already a string, so nothing
			// is reduced and nothing is marked.
			name: "quoted type",
			hcl: `
table "t" {
  column "c" { type = "USER_DEFINED" }
}
`,
			wantType:   "USER_DEFINED",
			wantRawSQL: false,
		},
		{
			// Negative control: an enum reference is resolved to the enum name,
			// which is not the sql() argument and must not be written back
			// through the hatch.
			name: "enum reference",
			hcl: `
enum "status" {
  values = ["a"]
}
table "t" {
  column "c" { type = enum.status }
}
`,
			wantType:   "status",
			wantRawSQL: false,
		},
		{
			// `unsigned = true` edits the type after the call is reduced, so
			// the sql() argument no longer spells what the column holds and
			// writing it back would drop the "unsigned".
			name: "unsigned rewrites the type and drops the marker",
			hcl: `
table "t" {
  column "c" {
    type     = sql("bigint")
    unsigned = true
  }
}
`,
			wantType:   "bigint unsigned",
			wantRawSQL: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Type, qt.Equals, test.wantType)
			c.Assert(db.Fields[0].TypeRawSQL, qt.Equals, test.wantRawSQL)
		})
	}
}
