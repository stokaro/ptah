package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// TestParse_AProcedureBlockCarriesTheKind pins that the kind arrives.
//
// Until stokaro/ptah#2209 a routine read out of HCL was always a function,
// whatever it had been in the database it was described from, and the
// comparator keys routines by kind: the described procedure and the real one
// never met, so applying a database's own description dropped it.
func TestParse_AProcedureBlockCarriesTheKind(t *testing.T) {
	tests := []struct {
		name string
		hcl  string
		kind string
	}{
		{
			name: "a procedure block",
			hcl: `schema "main" {
}
procedure "p" {
  schema = schema.main
  as     = "BEGIN END"
}
`,
			kind: schemamodel.FunctionKindProcedure,
		},
		{
			// The control. Empty is what every declaration written before
			// procedures existed meant, so a parser that stamped the kind
			// unconditionally would pass the row above and break every
			// function in the same file.
			name: "a function block",
			hcl: `schema "main" {
}
function "f" {
  schema = schema.main
  return = bigint
  as     = "SELECT 1"
}
`,
			kind: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Functions, qt.HasLen, 1)
			c.Assert(db.Functions[0].Kind, qt.Equals, test.kind)
		})
	}
}

// TestParse_AProcedureHasNoReturnType refuses the one attribute the two blocks
// do not share.
//
// A procedure returns nothing -- that is the whole difference between the
// blocks -- so a document declaring otherwise describes an object no engine
// will create, and saying so beats silently keeping a return type that then
// reaches a CREATE PROCEDURE which has no place to put it.
//
// The control for the first row lives in
// TestParse_AProcedureBlockCarriesTheKind, whose function row declares the same
// `return = bigint` and parses: without it these rows would also pass a parser
// that had stopped reading `return` anywhere.
func TestParse_AProcedureHasNoReturnType(t *testing.T) {
	tests := []struct {
		name    string
		hcl     string
		wantErr string
	}{
		{
			name: "a return type on a procedure is refused",
			hcl: `schema "main" {
}
procedure "p" {
  schema = schema.main
  return = bigint
  as     = "BEGIN END"
}
`,
			wantErr: `.*unsupported procedure attribute "return".*`,
		},
		{
			name: "an argument still needs its type",
			hcl: `schema "main" {
}
procedure "p" {
  schema = schema.main
  arg "a" {
  }
  as = "BEGIN END"
}
`,
			wantErr: `.*procedure arg requires type.*`,
		},
		{
			name: "a body is still required",
			hcl: `schema "main" {
}
procedure "p" {
  schema = schema.main
}
`,
			wantErr: `.*procedure "p" requires as.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
