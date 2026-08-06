package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// A `column` value is read from the PARSED expression, so every spelling that
// names a column names it -- not only the spellings whose source text happens to
// be a bare traversal. This is issue #1182.
//
// Reverted to the source-text reader, every row here fails on `err` being
// non-nil with `index on column contains unsupported reference "(column.n)"`
// (and the row's own text for the others); the pinned Atlas community binary
// v1.3.0 plans all of them at exit 0. The two `columns = [...]` rows were never
// part of the #1182 regression -- the list reader has read source text since
// before #1165 -- and they go red the same way, which is why they are here.
//
// Deleting only the ParenthesesExpr arm reddens the four parenthesised rows and
// leaves the two conditional rows green; deleting only the ConditionalExpr arm
// does the opposite. Neither mutation touches TestParseKeepsColumnAttributesThatNameAColumn.
func TestParseReadsAColumnAttributeThroughTheParsedExpression(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		hcl    string
		assert func(c *qt.C, db *goschema.Database)
	}{
		{
			name: "index on parenthesised reference",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = (column.n) }
  }
}
`,
			assert: assertSoleIndexPart("n"),
		},
		{
			name: "index on parenthesised reference across newlines",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on {
      column = (
        column.n
      )
    }
  }
}
`,
			assert: assertSoleIndexPart("n"),
		},
		{
			name: "index on doubly parenthesised qualified reference",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = ((table.t.column.n)) }
  }
}
`,
			assert: assertSoleIndexPart("n"),
		},
		{
			name: "index on conditional over a bool variable",
			hcl: `
variable "by_a" {
  type    = bool
  default = true
}
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.by_a ? column.a : column.b }
  }
}
`,
			assert: assertSoleIndexPart("a"),
		},
		{
			name: "index on conditional that takes the false branch",
			hcl: `
variable "by_a" {
  type    = bool
  default = false
}
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.by_a ? column.a : column.b }
  }
}
`,
			assert: assertSoleIndexPart("b"),
		},
		{
			name: "index on conditional over literals",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = "n" == "n" ? column.n : column.n }
  }
}
`,
			assert: assertSoleIndexPart("n"),
		},
		{
			name: "index columns list holds a parenthesised reference",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    columns = [(column.n)]
  }
}
`,
			assert: assertSoleIndexField("n"),
		},
		{
			name: "index columns list holds a conditional",
			hcl: `
table "t" {
  column "n" { type = int }
  column "m" { type = int }
  index "idx_n" {
    columns = ["n" == "n" ? column.n : column.m]
  }
}
`,
			assert: assertSoleIndexField("n"),
		},
		{
			name: "primary key on parenthesised reference",
			hcl: `
table "t" {
  column "n" { type = int }
  primary_key {
    on { column = (column.n) }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Tables, qt.HasLen, 1)
				c.Assert(db.Tables[0].PrimaryKeyParts, qt.DeepEquals, []goschema.PrimaryKeyPart{{Name: "n"}})
			},
		},
		{
			name: "partition by parenthesised reference",
			hcl: `
table "t" {
  column "n" { type = int }
  partition {
    type = RANGE
    by { column = (column.n) }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Tables, qt.HasLen, 1)
				c.Assert(db.Tables[0].Partition, qt.DeepEquals, &goschema.PartitionSpec{
					Type:  "RANGE",
					Parts: []goschema.PartitionPart{{Name: "n"}},
				})
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")
			c.Assert(err, qt.IsNil)
			test.assert(c, db)
		})
	}
}

func assertSoleIndexPart(name string) func(c *qt.C, db *goschema.Database) {
	return func(c *qt.C, db *goschema.Database) {
		c.Assert(db.Indexes, qt.HasLen, 1)
		c.Assert(db.Indexes[0].Parts, qt.HasLen, 1)
		c.Assert(db.Indexes[0].Parts[0].Name, qt.Equals, name)
	}
}

// The list spelling `columns = [...]` lands in Fields, not Parts -- a different
// reader, which is the point of covering it separately.
func assertSoleIndexField(name string) func(c *qt.C, db *goschema.Database) {
	return func(c *qt.C, db *goschema.Database) {
		c.Assert(db.Indexes, qt.HasLen, 1)
		c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{name})
	}
}

// A conditional whose condition cannot be decided is still refused, and the
// admission rule for a `variable` is what decides it. Each row below is a
// variable shape the pinned Atlas community binary v1.3.0 refuses the whole file
// for, so admitting any of them would make Ptah plan a file that binary will not
// load -- the one direction this parser may never take.
//
// This is the guard's INVERSE-mutant test, and it is the only way the admission
// rule's non-interference is provable. Each row names the part of
// schemaVariableDefault that has to be relaxed to turn it green, and
// TestParseReadsAColumnAttributeThroughTheParsedExpression stays green under
// every one of those relaxations. Today each row prints
// `index on column contains unsupported reference "<the conditional as written>"`;
// under the mutation it is aimed at it prints nothing and the file parses.
//
// The last two rows are the pair worth reading together. Dropping the
// declared-type check alone does NOT redden "default contradicts its type" --
// a string default under `type = bool` still fails the boolean test in
// conditionalBranch -- so that row cannot be cited as the check's discriminator.
// The `type = int, default = "a"` row is: its condition is a string COMPARISON,
// which yields a perfectly good boolean, so without the declared-type check Ptah
// would plan an index on "a" for a file the pinned binary refuses with
// `variable "pick": a number is required`.
func TestParseRefusesAConditionalColumnWhoseConditionCannotBeDecided(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		hcl   string
		match string
	}{
		{
			name: "variable without a type",
			hcl: `
variable "by_a" {
  default = true
}
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.by_a ? column.a : column.b }
  }
}
`,
			match: `.*variable\ "by_a"\ requires\ a\ type.*`,
		},
		{
			name: "variable without a default",
			hcl: `
variable "by_a" {
  type = bool
}
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.by_a ? column.a : column.b }
  }
}
`,
			match: `.*missing\ value\ for\ required\ variable\ "by_a".*`,
		},
		{
			name: "variable typed by an unknown keyword",
			hcl: `
variable "by_a" {
  type    = nonsense
  default = true
}
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.by_a ? column.a : column.b }
  }
}
`,
			match: `.*variable\ "by_a"\ type\ is\ not\ supported.*`,
		},
		{
			name: "no variable of that name",
			hcl: `
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.by_a ? column.a : column.b }
  }
}
`,
			match: refusedConditional,
		},
		{
			name: "condition is not a boolean",
			hcl: `
variable "by_a" {
  type    = string
  default = "a"
}
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.by_a ? column.a : column.b }
  }
}
`,
			match: refusedConditional,
		},
		{
			name: "variable whose default contradicts its type",
			hcl: `
variable "by_a" {
  type    = bool
  default = "yes"
}
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.by_a ? column.a : column.b }
  }
}
`,
			match: `.*variable\ "by_a":\ a\ bool\ is\ required.*`,
		},
		{
			name: "declared type contradicts a default the condition can still compare",
			hcl: `
variable "pick" {
  type    = int
  default = "a"
}
table "t" {
  column "a" { type = int }
  column "b" { type = int }
  index "idx_n" {
    on { column = var.pick == "a" ? column.a : column.b }
  }
}
`,
			match: `.*variable\ "pick":\ a\ number\ is\ required.*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			_, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}

// refusedConditional is the column resolver's own refusal. Only the two rows
// whose variable declaration is well formed still reach it: "no variable of
// that name" and "condition is not a boolean".
//
// The other five now fail earlier and more precisely. #926's evaluation pass
// diagnoses the variable DECLARATION -- missing type, missing value, unsupported
// type keyword, a default contradicting its type -- which is upstream of the
// column reference and names what the author has to change. Every row still
// refuses; what moved is which layer speaks first, not whether a conditional
// column is resolved.
const refusedConditional = `.*index on column contains unsupported reference "var.by_a \? column.a : column.b"`

// A branch that is taken still has to name a column. This is what keeps the
// conditional arm from becoming a way around the #1106 refusal.
//
// The two rows are refused by different guards on purpose. `sql()` inside a
// larger expression never reaches the column reader at all -- the #1131 guard
// runs ahead of the body walk and refuses the file -- so the row pins that the
// conditional arm did not open a route past it. The empty-string row is the one
// the column reader itself refuses, and it is the discriminator for "resolves to
// a name" versus "resolved successfully".
//
// Reverted, both rows still fail: the source-text reader refuses every
// conditional. They go red only under a mutation that lets a taken branch
// resolve to something that is not a column name, which is what they forbid.
func TestParseRefusesAConditionalBranchThatCannotNameAColumn(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		hcl   string
		match string
	}{
		{
			name: "taken branch is a sql call",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = "n" == "n" ? sql("n") : column.n }
  }
}
`,
			match: `.*attribute "column": sql\(\) must be the whole attribute value, not part of a larger expression`,
		},
		{
			name: "taken branch is an empty string",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = "n" == "n" ? "" : column.n }
  }
}
`,
			match: `.*index on column contains unsupported reference "\\"n\\" == \\"n\\" \? \\"\\" : column.n"`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			_, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}
