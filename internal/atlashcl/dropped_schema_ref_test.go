package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// TestParseIgnoreUnknownNamesResolvesDeclaredSchemaRefs pins that a dropped body
// may name a schema the file declares.
//
// `procedure "p" { schema = schema.main }` is the ordinary spelling of every
// Atlas object Ptah does not model, and until stokaro/ptah#927 item 5 the whole
// file was refused over it: the dropped body was evaluated in a scope with no
// `schema` root at all, so the reference failed with `unknown variable
// "schema"` and the load exited 1. The pinned Atlas community binary v1.3.0
// loads each file below at exit 0 and reports `Schemas are synced, no changes to
// be made.`
//
// Each row asserts the strong form: the file WITH the dropped construct produces
// exactly the IR of the same file WITHOUT it, so the construct contributed
// nothing beyond being accepted. Each row also asserts that the strict parser --
// the one the native `ptah schema` commands use -- still refuses, because the
// tolerance is opt-in and widening it here would weaken Ptah's own surface.
func TestParseIgnoreUnknownNamesResolvesDeclaredSchemaRefs(t *testing.T) {

	tests := []struct {
		name       string
		hcl        string
		equivalent string
		strictErr  string
	}{
		{
			name: "procedure below the schema it names",
			hcl: `schema "main" {
}
procedure "p" {
  schema = schema.main
  as     = "BEGIN END"
}
`,
			equivalent: `schema "main" {
}
`,
			strictErr: `.*unsupported top-level block "procedure".*`,
		},
		{
			// Declaration order must not decide the verdict: the community
			// binary evaluates the whole file before it decides what to
			// decode, so a dropped body above the schema block resolves there.
			name: "procedure above the schema it names",
			hcl: `procedure "p" {
  schema = schema.main
  as     = "BEGIN END"
}
schema "main" {
}
`,
			equivalent: `schema "main" {
}
`,
			strictErr: `.*unsupported top-level block "procedure".*`,
		},
		{
			// The root carries every declared label, not just the first.
			name: "second of two declared schemas",
			hcl: `schema "first" {
}
schema "second" {
}
procedure "p" {
  schema = schema.second
  as     = "BEGIN END"
}
`,
			equivalent: `schema "first" {
}
schema "second" {
}
`,
			strictErr: `.*unsupported top-level block "procedure".*`,
		},
		{
			// The same reference in an unknown ATTRIBUTE rather than an
			// unknown block: both go through the dropped-body scope.
			name: "unknown table attribute naming a schema",
			hcl: `schema "main" {
}
table "t" {
  schema            = schema.main
  zzz_nonsense_attr = schema.main
  column "id" {
    type = int
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			strictErr: `.*unsupported table attribute "zzz_nonsense_attr".*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, strictErr := atlashcl.Parse([]byte(tt.hcl), "schema.hcl")
			c.Assert(strictErr, qt.ErrorMatches, tt.strictErr)

			tolerant, err := atlashcl.ParseWithOptions(
				[]byte(tt.hcl),
				"schema.hcl",
				atlashcl.Options{IgnoreUnknownNames: true},
			)
			c.Assert(err, qt.IsNil)

			without, err := atlashcl.Parse([]byte(tt.equivalent), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(tolerant, qt.DeepEquals, without)
		})
	}
}

// TestParseIgnoreUnknownNamesRefusesUndeclaredSchemaRefs is the inverse control
// for the row above: it pins the three refusals the `schema` root must keep.
//
// Without them the fix would be one line -- bind the root to a value that
// answers to anything -- and that line accepts files the pinned community
// binary v1.3.0 refuses. Measured there, same command, same dev database:
//
//	schema "main" {} + schema = schema.nope   -> exit 1, "This object does not
//	                                             have an attribute named
//	                                             \"nope\""
//	schema "main" {} + schema.main.nope       -> exit 1, same summary
//	no schema block  + schema = schema.other  -> exit 1, "There is no variable
//	                                             named \"schema\""
//
// A root bound to [cty.DynamicVal], or one that carries a wildcard name, exits
// 0 on all three and reddens every row here.
func TestParseIgnoreUnknownNamesRefusesUndeclaredSchemaRefs(t *testing.T) {

	tests := []struct {
		name    string
		hcl     string
		wantErr string
	}{
		{
			name: "schema name no block declares",
			hcl: `schema "main" {
}
procedure "p" {
  schema = schema.nope
  as     = "BEGIN END"
}
`,
			wantErr: `.*does not have an attribute named "nope".*`,
		},
		{
			name: "member access on a declared schema",
			hcl: `schema "main" {
}
procedure "p" {
  schema = schema.main.nope
  as     = "BEGIN END"
}
`,
			wantErr: `.*This value does not have any attributes.*`,
		},
		{
			name: "file declares no schema at all",
			hcl: `table "t" {
  column "id" {
    type = int
  }
}
procedure "p" {
  schema = schema.other
  as     = "BEGIN END"
}
`,
			wantErr: `.*unknown variable "schema".*`,
		},
		{
			// A malformed schema block contributes no name, so accepting the
			// dropped reference cannot become the reason a file the walk would
			// have refused now loads: the walk still reports the block itself.
			name: "schema block with two labels is still refused",
			hcl: `schema "main" "extra" {
}
procedure "p" {
  schema = schema.main
  as     = "BEGIN END"
}
`,
			wantErr: `.*schema block requires exactly one label.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.ParseWithOptions(
				[]byte(tt.hcl),
				"schema.hcl",
				atlashcl.Options{IgnoreUnknownNames: true},
			)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}
}
