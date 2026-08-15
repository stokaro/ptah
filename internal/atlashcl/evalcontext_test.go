package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// A schema file's `variable` and `locals` blocks bind values that var. and
// local. references resolve to, instead of the reference's own source text
// being copied into the IR -- issue #926.
//
// Every row puts the reference in a COLUMN DEFAULT, and that placement is the
// whole point. A fixture that puts it in a table comment discriminates nothing:
// Ptah drops table comments on SQLite whether or not the reference resolved, so
// the control and the variant agree and the test passes over a broken parser.
//
// Reverted, every row prints the reference's own source text where it asserts
// the value: `var.status` for the first, `prefix_${var.n}` (quotes included)
// for the interpolation, `local.d` for the local, `upper("abc")` for the
// function call. The literal control row passes either way, which is what makes
// the others discriminate.
func TestParseResolvesSchemaVariablesAndLocals(t *testing.T) {
	tests := []struct {
		name string
		hcl  string
		want string
	}{
		{
			name: "literal default control",
			hcl: `
table "t" {
  column "state" {
    type    = text
    default = "active"
  }
}
`,
			want: "active",
		},
		{
			name: "variable default",
			hcl: `
variable "status" {
  type    = string
  default = "active"
}
table "t" {
  column "state" {
    type    = text
    default = var.status
  }
}
`,
			want: "active",
		},
		{
			name: "interpolation in an attribute value",
			hcl: `
variable "n" {
  type    = string
  default = "users"
}
table "t" {
  column "state" {
    type    = text
    default = "prefix_${var.n}"
  }
}
`,
			want: "prefix_users",
		},
		{
			name: "locals block",
			hcl: `
locals {
  d = "abc"
}
table "t" {
  column "state" {
    type    = text
    default = local.d
  }
}
`,
			want: "abc",
		},
		{
			name: "local built from a variable",
			hcl: `
variable "env" {
  type    = string
  default = "prod"
}
locals {
  d = "${var.env}-default"
}
table "t" {
  column "state" {
    type    = text
    default = local.d
  }
}
`,
			want: "prod-default",
		},
		{
			name: "function call",
			hcl: `
table "t" {
  column "state" {
    type    = text
    default = upper("abc")
  }
}
`,
			want: "ABC",
		},
		{
			name: "numeric variable formats as its value",
			hcl: `
variable "n" {
  type    = int
  default = 7
}
table "t" {
  column "state" {
    type    = int
    default = var.n
  }
}
`,
			want: "7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse([]byte(tt.hcl), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Default, qt.Equals, tt.want)
		})
	}
}

// --var supplies a value for a variable that declares no default, and overrides
// one that does.
//
// Reverted, the first row parses at exit 0 with the DEFAULT `var.status` and
// this test fails on the value; the second row keeps the block's own default
// and fails the same way.
func TestParseAppliesVarOverrides(t *testing.T) {
	tests := []struct {
		name string
		hcl  string
		vars []string
		want string
	}{
		{
			name: "supplies a required variable",
			hcl: `
variable "status" {
  type = string
}
table "t" {
  column "state" {
    type    = text
    default = var.status
  }
}
`,
			vars: []string{"status=live"},
			want: "live",
		},
		{
			name: "overrides a declared default",
			hcl: `
variable "status" {
  type    = string
  default = "active"
}
table "t" {
  column "state" {
    type    = text
    default = var.status
  }
}
`,
			vars: []string{"status=live"},
			want: "live",
		},
		{
			name: "one occurrence carries comma-separated assignments",
			hcl: `
variable "a" {
  type = string
}
variable "b" {
  type = string
}
table "t" {
  column "state" {
    type    = text
    default = "${var.a}-${var.b}"
  }
}
`,
			vars: []string{"a=x,b=y"},
			want: "x-y",
		},
		{
			name: "an override for an undeclared name is ignored",
			hcl: `
variable "status" {
  type    = string
  default = "active"
}
table "t" {
  column "state" {
    type    = text
    default = var.status
  }
}
`,
			vars: []string{"nosuch=1"},
			want: "active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.ParseWithOptions(
				[]byte(tt.hcl), "schema.hcl", atlashcl.Options{Vars: tt.vars},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Default, qt.Equals, tt.want)
		})
	}
}

// Files the pinned Atlas community binary v1.3.0 exits 1 on now exit non-zero
// here too, instead of exiting 0 with the unresolved source text in the DDL.
//
// Each row was run through that binary with `schema diff --dev-url
// "sqlite://file?mode=memory"` and the diagnostic it printed is quoted on the
// row. Reverted, every row parses at exit 0 and this test fails on the
// non-nil-error assertion.
func TestParseRefusesUnresolvableExpressions(t *testing.T) {
	tests := []struct {
		name    string
		hcl     string
		wantErr string
	}{
		{
			// Community binary: `missing value for required variable "status"`.
			name: "typed variable with neither a default nor an override",
			hcl: `
variable "status" {
  type = string
}
table "t" {
  column "state" {
    type    = text
    default = var.status
  }
}
`,
			wantErr: `missing value for required variable "status"`,
		},
		{
			// Community binary: `The argument "type" is required, but no
			// definition was found.`
			name: "variable with no type",
			hcl: `
variable "status" {
  default = "active"
}
table "t" {
  column "state" {
    type    = text
    default = var.status
  }
}
`,
			wantErr: `.*variable "status" requires a type`,
		},
		{
			// Community binary: `Unsupported attribute; This object does not
			// have an attribute named "missing".`
			name: "reference to an undeclared variable",
			hcl: `
table "t" {
  column "state" {
    type    = text
    default = var.missing
  }
}
`,
			wantErr: `.*unknown variable "var"`,
		},
		{
			// Community binary: `Call to unknown function; There is no function
			// named "now".`
			name: "call to a function that is not in the set",
			hcl: `
table "t" {
  column "state" {
    type    = text
    default = now()
  }
}
`,
			wantErr: `.*call to unknown function: There is no function named "now".*`,
		},
		{
			// Community binary: `set field "type": unexpected type string`.
			// Resolving it instead would exit 0 where that binary exits 1.
			name: "variable reference in a column type",
			hcl: `
variable "coltype" {
  type    = string
  default = "varchar(255)"
}
table "t" {
  column "state" {
    type = var.coltype
  }
}
`,
			wantErr: `.*a variable reference is not supported in a type`,
		},
		{
			// Community binary: `variable "n": a number is required`.
			name: "default that does not match the declared type",
			hcl: `
variable "n" {
  type    = int
  default = "abc"
}
table "t" {
  column "state" {
    type    = int
    default = var.n
  }
}
`,
			wantErr: `.*variable "n": .*`,
		},
		{
			// Community binary: `An argument named "sensitive" is not expected
			// here` -- sensitive is an atlas.hcl PROJECT variable attribute, not
			// a schema-file one.
			name: "sensitive is not a schema-file variable attribute",
			hcl: `
variable "status" {
  type      = string
  default   = "active"
  sensitive = true
}
table "t" {
  column "state" {
    type    = text
    default = var.status
  }
}
`,
			wantErr: `.*unsupported variable attribute "sensitive"`,
		},
		{
			// Community binary: `Unknown variable; There is no variable named
			// "float"`. The keyword set is bool, int, number, string and
			// list/map/set of those, measured one keyword at a time.
			name: "variable type keyword outside the measured set",
			hcl: `
variable "n" {
  type    = float
  default = 1
}
table "t" {
  column "state" {
    type    = text
    default = var.n
  }
}
`,
			wantErr: `.*variable "n" type is not supported.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.Parse([]byte(tt.hcl), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}
}

// The reference forms a schema file is full of are NOT values, and adding the
// evaluation context must not start evaluating them.
//
// Every row here would fail to evaluate against any context -- `text` names no
// variable, `varchar(255)` calls no function, `[column.n]` reaches a root that
// does not exist -- and every one of them has to keep resolving from source
// text. Reverted to evaluating them, each row fails with an "unknown variable"
// or "call to unknown function" error that the assertion prints.
func TestParseKeepsReferenceFormsUnevaluated(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		hcl    string
		assert func(c *qt.C, db *goschema.Database)
	}{
		{
			name: "bare type keyword",
			hcl: `
table "t" {
  column "n" { type = text }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields[0].Type, qt.Equals, "text")
			},
		},
		{
			name: "parameterized type spelled as a call",
			hcl: `
table "t" {
  column "n" { type = varchar(255) }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields[0].Type, qt.Equals, "varchar(255)")
			},
		},
		{
			name: "schema reference",
			hcl: `
schema "app" {
}
table "t" {
  schema = schema.app
  column "n" { type = int }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Tables[0].Schema, qt.Equals, "app")
			},
		},
		{
			name: "column reference inside an index list",
			hcl: `
table "t" {
  column "n" { type = int }
  index "i" {
    columns = [column.n]
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"n"})
			},
		},
		{
			// function.return and range.subtype hold a type the same way
			// column.type does. Reverted to checking only `type`, this row
			// fails with `call to unknown function: There is no function named
			// "varchar"`.
			name: "parameterized type in a function return",
			hcl: `
function "f" {
  lang   = SQL
  return = varchar(255)
  as     = "SELECT 'x'"
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Functions, qt.HasLen, 1)
				c.Assert(db.Functions[0].Returns, qt.Equals, "varchar(255)")
			},
		},
		{
			name: "parameterized subtype in a range",
			hcl: `
schema "main" {
}
range "r" {
  schema  = schema.main
  subtype = numeric(10, 2)
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Ranges, qt.HasLen, 1)
				c.Assert(db.Ranges[0].Subtype, qt.Equals, "numeric(10, 2)")
			},
		},
		{
			name: "enum reference as a column type",
			hcl: `
schema "main" {
}
enum "status" {
  schema = schema.main
  values = ["a", "b"]
}
table "t" {
  schema = schema.main
  column "n" { type = enum.status }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields[0].Type, qt.Equals, "status")
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			db, err := atlashcl.Parse([]byte(tt.hcl), "schema.hcl")
			c.Assert(err, qt.IsNil)
			tt.assert(c, db)
		})
	}
}
