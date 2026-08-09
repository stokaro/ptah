package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// TestSchemaReferenceInAFunctionCallNamesTheRealCause is the regression test for
// a diagnostic that named the wrong cause.
//
// A function call spelled as a whole attribute value has to evaluate -- every
// value helper in this package falls back to an attribute's SOURCE TEXT when an
// expression will not, so a call let through would reach the database as the
// literal `jsonencode(schema.main)` -- and the evaluation context binds only
// `var.` and `local.`. The refusal is therefore correct. The MESSAGE was not:
// it said `unknown variable: There is no variable named "schema"` for a root
// that IS bound, both by the file's own `schema "main"` block two lines up and
// by the scope a dropped body is evaluated in, where `schema = schema.main`
// resolves. A reader sent looking for a missing block finds one already there.
//
// The tolerant row and the strict row both run because the tolerance policy is
// irrelevant here: this check runs before the body walk decides which names to
// drop, so both command trees saw the same wrong sentence.
//
// The undeclared row is the control that keeps the fix from becoming a blanket
// rewrite: with no `schema` block in the file, nothing binds the root and
// `unknown variable` is the accurate thing to say. So is `nonsense`, which no
// document ever declares.
func TestSchemaReferenceInAFunctionCallNamesTheRealCause(t *testing.T) {
	const declared = `schema "main" {}
procedure "p" {
  names = jsonencode(schema.main)
}
`
	const undeclared = `table "users" {
  column "id" {
    type = int
  }
}
procedure "p" {
  names = jsonencode(schema.main)
}
`
	const unknownRoot = `schema "main" {}
procedure "p" {
  names = jsonencode(nonsense.thing)
}
`

	tests := []struct {
		name      string
		source    string
		tolerant  bool
		errorLike string
	}{
		{
			name:      "a declared schema reference names itself",
			source:    declared,
			tolerant:  true,
			errorLike: `parse HCL schema at schema\.hcl:3,22-28: schema reference "schema" has no value to pass to a function call: only var\. and local\. references are evaluated here`,
		},
		{
			name:      "the strict command tree reports the same cause",
			source:    declared,
			tolerant:  false,
			errorLike: `parse HCL schema at schema\.hcl:3,22-28: schema reference "schema" has no value to pass to a function call: .*`,
		},
		{
			name:      "an undeclared schema root is still an unknown variable",
			source:    undeclared,
			tolerant:  true,
			errorLike: `parse HCL schema at schema\.hcl:7,22-28: unknown variable: There is no variable named "schema"\.`,
		},
		{
			name:      "a root no document declares is still an unknown variable",
			source:    unknownRoot,
			tolerant:  true,
			errorLike: `parse HCL schema at schema\.hcl:3,22-30: unknown variable: There is no variable named "nonsense"\.`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.ParseWithOptions([]byte(test.source), "schema.hcl", atlashcl.Options{
				IgnoreUnknownNames: test.tolerant,
			})

			c.Assert(err, qt.ErrorMatches, test.errorLike)
		})
	}
}

// TestSchemaReferenceStillResolvesWhereItIsRead is the non-interference control
// for the diagnostic above: the message changed and nothing else did.
//
// `schema = schema.main` on a modeled block is read from source text and never
// reaches the evaluation check, and a dropped body naming a declared schema is
// exit 0 on the pinned Atlas community binary v1.3.0 and here (stokaro/ptah#927).
// A fix that reported the new message for every `schema.` traversal would take
// both of these down.
func TestSchemaReferenceStillResolvesWhereItIsRead(t *testing.T) {
	const source = `schema "main" {}
procedure "p" {
  schema = schema.main
}
table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`
	c := qt.New(t)

	db, err := atlashcl.ParseWithOptions([]byte(source), "schema.hcl", atlashcl.Options{IgnoreUnknownNames: true})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Schema, qt.Equals, "main")
}
