package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// TestParseIgnoreUnknownNamesDropsTheName pins that a name the parser does not
// model is dropped rather than refused, and that dropping it leaves exactly the
// schema the same file would produce with the construct deleted.
//
// The equivalent-file comparison is the assertion that matters: it says the
// construct contributed nothing, which is stronger than "no error" and is the
// same shape as the DDL comparison the conformance run makes -- the schema WITH
// the construct against the schema WITHOUT it, on one implementation, rather
// than a byte comparison across two implementations that already render the
// same baseline differently.
//
// Every position below was measured on the pinned community binary with
// `schema inspect -u file://...`: exit 0, and DDL byte-identical to the same
// schema with the construct deleted.
func TestParseIgnoreUnknownNamesDropsTheName(t *testing.T) {

	tests := []struct {
		name       string
		hcl        string
		equivalent string
		strictErr  string
	}{
		{
			name: "top-level block",
			hcl: `
annotation "gql" {
  attr "name" {
    type = string
  }
}
schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			equivalent: `
schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			strictErr: `.*unsupported top-level block "annotation".*`,
		},
		{
			name: "top-level block with a nonsense name",
			hcl: `
frobnicate_nonsense "zz" {
  anything = "here"
}
table "t" {
  column "id" {
    type = int
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
}
`,
			strictErr: `.*unsupported top-level block "frobnicate_nonsense".*`,
		},
		{
			name: "table block",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  annotation {
    gql = "Thing"
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
}
`,
			strictErr: `.*unsupported table block "annotation".*`,
		},
		{
			name: "table block with a nonsense name",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  zzz_nonsense_block {
    anything = "here"
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
}
`,
			strictErr: `.*unsupported table block "zzz_nonsense_block".*`,
		},
		{
			name: "column attribute",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  column "secret" {
    type      = int
    invisible = true
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
  column "secret" {
    type = int
  }
}
`,
			strictErr: `.*unsupported column attribute "invisible".*`,
		},
		{
			name: "column attribute with a nonsense name",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  column "secret" {
    type              = int
    zzz_nonsense_attr = true
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
  column "secret" {
    type = int
  }
}
`,
			strictErr: `.*unsupported column attribute "zzz_nonsense_attr".*`,
		},
		{
			name: "table attribute with a nonsense name",
			hcl: `
table "t" {
  zzz_table_attr = true
  column "id" {
    type = int
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
}
`,
			strictErr: `.*unsupported table attribute "zzz_table_attr".*`,
		},
		{
			name: "column block with a nonsense name",
			hcl: `
table "t" {
  column "id" {
    type = int
    zzz_nonsense_block {
      anything = "here"
    }
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
}
`,
			strictErr: `.*unsupported column block "zzz_nonsense_block".*`,
		},
		{
			name: "index attribute with a nonsense name",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  index "i" {
    columns           = [column.id]
    zzz_nonsense_attr = true
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
  index "i" {
    columns = [column.id]
  }
}
`,
			strictErr: `.*unsupported index attribute "zzz_nonsense_attr".*`,
		},
		{
			name: "index block with a nonsense name",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  index "i" {
    columns = [column.id]
    zzz_nonsense_block {
      anything = "here"
    }
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
  index "i" {
    columns = [column.id]
  }
}
`,
			strictErr: `.*unsupported index block "zzz_nonsense_block".*`,
		},
		{
			name: "schema attribute with a nonsense name",
			hcl: `
schema "main" {
  zzz_nonsense_attr = true
}
`,
			equivalent: `
schema "main" {
}
`,
			strictErr: `.*unsupported schema attribute "zzz_nonsense_attr".*`,
		},
		{
			name: "primary_key attribute with a nonsense name",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  primary_key {
    columns           = [column.id]
    zzz_nonsense_attr = true
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`,
			strictErr: `.*unsupported primary_key attribute "zzz_nonsense_attr".*`,
		},
		{
			name: "foreign_key attribute with a nonsense name",
			hcl: `
table "p" {
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
table "t" {
  column "pid" {
    type = int
  }
  foreign_key "fk" {
    columns           = [column.pid]
    ref_columns       = [table.p.column.id]
    zzz_nonsense_attr = true
  }
}
`,
			equivalent: `
table "p" {
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
table "t" {
  column "pid" {
    type = int
  }
  foreign_key "fk" {
    columns     = [column.pid]
    ref_columns = [table.p.column.id]
  }
}
`,
			strictErr: `.*unsupported foreign_key attribute "zzz_nonsense_attr".*`,
		},
		{
			name: "check attribute with a nonsense name",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  check "c" {
    expr              = "id > 0"
    zzz_nonsense_attr = true
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
  check "c" {
    expr = "id > 0"
  }
}
`,
			strictErr: `.*unsupported check attribute "zzz_nonsense_attr".*`,
		},
		{
			name: "enum attribute with a nonsense name",
			hcl: `
schema "main" {
}
enum "e" {
  schema            = schema.main
  values            = ["a", "b"]
  zzz_nonsense_attr = true
}
`,
			equivalent: `
schema "main" {
}
enum "e" {
  schema = schema.main
  values = ["a", "b"]
}
`,
			strictErr: `.*unsupported enum attribute "zzz_nonsense_attr".*`,
		},
		{
			name: "partition attribute the community binary drops on MySQL",
			hcl: `
table "t" {
  column "id" {
    type = int
  }
  partition {
    type       = "HASH"
    columns    = [column.id]
    partitions = 4
  }
}
`,
			equivalent: `
table "t" {
  column "id" {
    type = int
  }
  partition {
    type    = "HASH"
    columns = [column.id]
  }
}
`,
			strictErr: `.*unsupported partition attribute "partitions".*`,
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

// TestParseIgnoreUnknownNamesStillEvaluatesTheBody pins that the tolerance is
// name-level, not subtree-level.
//
// The community binary evaluates the whole file before it decides which names
// to decode, so anything inside a construct it is about to drop that does not
// evaluate is still fatal. Every case below exits 1 on that binary, measured
// with `schema inspect -u file://...`, and they do not all fail the same way:
// an unresolvable root, a call to a function that is not in scope and an
// operand of the wrong type are three different diagnostics, and a check that
// only looked at reference roots would accept the last two.
//
// The rows from "member of a block declared in the file" down are the ones an
// earlier revision of this parser ACCEPTED, because it modeled the file's
// blocks and variables as reference roots and fell back to a wildcard wherever
// it could not enumerate them. Each is a file the community binary refuses.
// They are here so that reintroducing any such wildcard fails a test rather
// than shipping.
func TestParseIgnoreUnknownNamesStillEvaluatesTheBody(t *testing.T) {

	tests := []struct {
		name    string
		hcl     string
		wantErr string
	}{
		{
			name: "reference under a dropped column attribute",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type              = int
    zzz_nonsense_attr = not_a_real_identifier
  }
}
`,
			wantErr: `parse HCL schema at schema\.hcl:7,25-46: unknown variable "not_a_real_identifier"`,
		},
		{
			name: "reference under a dropped table block",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  annotation {
    gql = not_a_real_identifier
  }
}
`,
			wantErr: `parse HCL schema at schema\.hcl:9,11-32: unknown variable "not_a_real_identifier"`,
		},
		{
			name: "reference under a dropped top-level block",
			hcl: `schema "main" {
}
annotation "gql" {
  this_is = not_a_real_identifier
}
`,
			wantErr: `parse HCL schema at schema\.hcl:4,13-34: unknown variable "not_a_real_identifier"`,
		},
		{
			name: "column root is out of scope at the top level",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
annotation "gql" {
  ref = column.id
}
`,
			wantErr: `parse HCL schema at schema\.hcl:10,9-15: unknown variable "column"`,
		},
		{
			name: "variable is not a reference root even when the file declares one",
			hcl: `variable "v" {
  type    = string
  default = "x"
}
schema "main" {
}
annotation "gql" {
  ref = variable.v
}
`,
			wantErr: `parse HCL schema at schema\.hcl:8,9-17: unknown variable "variable"`,
		},
		{
			name: "call to a function that is not in scope",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = frobnicate_nonsense("a")
}
`,
			wantErr: `parse HCL schema at schema\.hcl:4,9-28: call to unknown function: There is no function named "frobnicate_nonsense".*`,
		},
		{
			name: "operand of the wrong type",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = 1 + "abc"
}
`,
			wantErr: `parse HCL schema at schema\.hcl:4,13-18: invalid operand: .*number.*`,
		},
		{
			name: "type keyword is not a number",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = 1 + string
}
`,
			wantErr: `parse HCL schema at schema\.hcl:4,13-19: invalid operand: .*number.*`,
		},
		{
			name: "member of a block declared in the file",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
annotation "gql" {
  ref = table.nope
}
`,
			wantErr: `parse HCL schema at schema\.hcl:10,9-14: unknown variable "table"`,
		},
		{
			name: "var member with no variable block declaring it",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = var.v
}
`,
			wantErr: `parse HCL schema at schema\.hcl:4,9-12: unknown variable "var"`,
		},
		{
			name: "column root inside a table body",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  annotation {
    ref = column.nope
  }
}
`,
			wantErr: `parse HCL schema at schema\.hcl:9,11-17: unknown variable "column"`,
		},
		{
			name: "scoped enum name is not a reference root",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = HASH
}
`,
			wantErr: `parse HCL schema at schema\.hcl:4,9-13: unknown variable "HASH"`,
		},
		{
			name: "unlabeled block nested in the dropped block",
			hcl: `schema "main" {
}
annotation "gql" {
  inner {
    a = 1
  }
  ref = inner.a
}
`,
			wantErr: `parse HCL schema at schema\.hcl:7,9-14: unknown variable "inner"`,
		},
		{
			name: "index into an unlabeled block nested in the dropped block",
			hcl: `schema "main" {
}
annotation "gql" {
  inner {
    a = 1
  }
  ref = inner["typo"]
}
`,
			wantErr: `parse HCL schema at schema\.hcl:7,9-14: unknown variable "inner"`,
		},
		{
			name: "unlabeled top-level block referenced by its own name",
			hcl: `schema "main" {
}
frobnicate_nonsense {
  a = 1
}
annotation "gql" {
  ref = frobnicate_nonsense.typo
}
`,
			wantErr: `parse HCL schema at schema\.hcl:7,9-28: unknown variable "frobnicate_nonsense"`,
		},
		{
			name: "unlabeled table-body block referenced from a dropped block",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
  annotation {
    ref = primary_key.columns
  }
}
`,
			wantErr: `parse HCL schema at schema\.hcl:12,11-22: unknown variable "primary_key"`,
		},
		{
			// Since issue #926 the file's own variables are in scope here, so
			// this fails on the member access rather than on the root -- the
			// same diagnostic the community binary reports for it.
			name: "attribute access on a declared string variable",
			hcl: `variable "v" {
  type    = string
  default = "x"
}
schema "main" {
}
annotation "gql" {
  ref = var.v.nope
}
`,
			wantErr: `parse HCL schema at schema\.hcl:8,14-19: unsupported attribute: Can't access attributes on a primitive-typed value \(string\)\.`,
		},
		{
			name: "arithmetic over a declared string variable",
			hcl: `variable "v" {
  type    = string
  default = "x"
}
schema "main" {
}
annotation "gql" {
  ref = 1 + var.v
}
`,
			wantErr: `parse HCL schema at schema\.hcl:8,13-18: invalid operand: Unsuitable value for right operand: a number is required\.`,
		},
		{
			name: "reference that stops on a block type rather than a block",
			hcl: `schema "main" {
}
table "p" {
  schema = schema.main
  column "id" {
    type = int
  }
}
annotation "gql" {
  ref = table.p.column
}
`,
			wantErr: `parse HCL schema at schema\.hcl:10,9-14: unknown variable "table"`,
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

// TestParseIgnoreUnknownNamesAcceptsEvaluableBodies is the counterpart to
// TestParseIgnoreUnknownNamesStillEvaluatesTheBody: it pins that the body check
// rejects expressions that do not evaluate, not expressions as such.
//
// Every expression here evaluates on the community binary inside a dropped
// construct, measured with `schema inspect -u file://...` at exit 0. The first
// row is the one stokaro/ptah#1016 exists for -- an Atlas v1.3.0 annotation
// declaration, whose body needs the `string` keyword to resolve.
func TestParseIgnoreUnknownNamesAcceptsEvaluableBodies(t *testing.T) {

	tests := []struct {
		name string
		hcl  string
	}{
		{
			name: "scalar type keyword",
			hcl: `schema "main" {
}
annotation "gql" {
  attr "name" {
    type = string
  }
}
`,
		},
		{
			name: "type keyword as the whole value",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = int
}
`,
		},
		{
			name: "two type keywords compared",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = string == bool
}
`,
		},
		{
			name: "bare object keys are not references",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  annotation {
    gql = {
      name   = "Thing"
      plural = "Things"
    }
  }
}
`,
		},
		{
			name: "arithmetic over literals",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = 1 + 2
}
`,
		},
		{
			name: "call to a function that is in scope",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = upper("a")
}
`,
		},
		{
			name: "comprehension over an in-scope function",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = [for v in ["a", "b"] : upper(v)]
}
`,
		},
		{
			// Moved here from
			// TestParseIgnoreUnknownNamesIsStricterThanTheOracleOnReferences by
			// issue #926, which is the revisit that test's doc comment asked
			// for: the file's own variable blocks now bind real typed values,
			// so a dropped body reading one resolves it exactly as the
			// community binary does instead of being refused.
			name: "declared variable inside a dropped block",
			hcl: `variable "v" {
  type    = string
  default = "x"
}
schema "main" {
}
annotation "gql" {
  gql = var.v
}
`,
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
			c.Assert(err, qt.IsNil)
		})
	}
}

// TestParseIgnoreUnknownNamesIsStricterThanTheOracleOnReferences records, as
// executable text, the price of the closed scope.
//
// Each file here is accepted by the community binary at exit 0 and refused
// here, because the reference it carries would need a reference root this
// parser deliberately does not build. That is the safe direction -- a user gets
// an error message instead of a schema the real tool would never have loaded --
// but it is a divergence, and an undocumented divergence is indistinguishable
// from a bug. If a later change makes any of these parse, this test is where
// the decision gets revisited rather than quietly reversed.
func TestParseIgnoreUnknownNamesIsStricterThanTheOracleOnReferences(t *testing.T) {

	tests := []struct {
		name    string
		hcl     string
		wantErr string
	}{
		{
			name: "column root inside a table body",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  column "b" {
    type              = int
    zzz_nonsense_attr = column.id
  }
}
`,
			wantErr: `.*unknown variable "column"`,
		},
		{
			name: "reference to a table declared later in the file",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = table.t
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			wantErr: `.*unknown variable "table"`,
		},
		{
			name: "block nested in the dropped block",
			hcl: `schema "main" {
}
annotation "gql" {
  attr "name" {
    type = string
  }
  ref = attr.name
}
`,
			wantErr: `.*unknown variable "attr"`,
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

// TestParseIgnoreUnknownNamesKeepsStructuralRefusals pins where the tolerance
// stops: it drops NAMES, and never turns a construct that is structurally
// incomplete into one that parses.
//
// Both cases below hold a construct whose only nested block is dropped. The
// name goes; the requirement the block was supposed to satisfy does not.
func TestParseIgnoreUnknownNamesKeepsStructuralRefusals(t *testing.T) {

	tests := []struct {
		name    string
		hcl     string
		wantErr string
	}{
		{
			name: "partition left with no by blocks",
			hcl: `table "t" {
  column "id" {
    type = int
  }
  partition {
    type = "HASH"
    zzz_nonsense_block {
      anything = "here"
    }
  }
}
`,
			wantErr: `.*partition requires columns attribute or by blocks.*`,
		},
		{
			name: "primary_key left with no on blocks",
			hcl: `table "t" {
  column "id" {
    type = int
  }
  primary_key {
    zzz_nonsense_block {
      anything = "here"
    }
  }
}
`,
			wantErr: `.*primary_key requires columns attribute or on blocks.*`,
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

// TestParseIgnoreUnknownNamesRecordsWhatItDropped pins that the parser can hand
// back what it silently discarded.
//
// The community binary reports nothing, and that silence is the footgun: a
// typo'd attribute name becomes an inert no-op that looks fine. The atlas.hcl
// parser already records its tolerated constructs, so recording them here is
// what makes the two HCL layers the same choice rather than two.
func TestParseIgnoreUnknownNamesRecordsWhatItDropped(t *testing.T) {
	c := qt.New(t)

	const src = `schema "main" {
}
annotation "gql" {
  gql = "Thing"
}
table "t" {
  schema = schema.main
  column "id" {
    type              = int
    zzz_nonsense_attr = true
  }
}
`

	var dropped []atlashcl.IgnoredName
	_, err := atlashcl.ParseWithOptions([]byte(src), "schema.hcl", atlashcl.Options{
		IgnoreUnknownNames: true,
		RecordIgnored:      func(name atlashcl.IgnoredName) { dropped = append(dropped, name) },
	})
	c.Assert(err, qt.IsNil)
	c.Assert(dropped, qt.DeepEquals, []atlashcl.IgnoredName{
		{Name: "annotation", Kind: "block", Scope: "top-level", Filename: "schema.hcl", Line: 3},
		{Name: "zzz_nonsense_attr", Kind: "attribute", Scope: "column", Filename: "schema.hcl", Line: 10},
	})
}

// TestParseStrictRecordsNothing pins that the recorder is only ever fed under
// the tolerance: with it off the same file is refused, so there is nothing to
// record and no way to read a dropped name as accepted.
func TestParseStrictRecordsNothing(t *testing.T) {
	c := qt.New(t)

	const src = `schema "main" {
}
annotation "gql" {
  gql = "Thing"
}
`

	var dropped []atlashcl.IgnoredName
	_, err := atlashcl.ParseWithOptions([]byte(src), "schema.hcl", atlashcl.Options{
		RecordIgnored: func(name atlashcl.IgnoredName) { dropped = append(dropped, name) },
	})
	c.Assert(err, qt.ErrorMatches, `.*unsupported top-level block "annotation".*`)
	c.Assert(dropped, qt.HasLen, 0)
}
