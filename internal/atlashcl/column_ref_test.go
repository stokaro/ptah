package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// A `column` attribute holds a column REFERENCE, and Atlas HCL's sql() escape
// hatch produces SQL TEXT. Reducing the call to its text -- the answer issue
// #1106 took for every position that reads a plain string -- still leaves text
// where a reference belongs, so these positions needed their own answer: refuse.
//
// Reverted, every row below parses with a nil error and an EMPTY column name
// reaches the renderer. Measured on the reverted build: the index rows plan
// `CREATE UNIQUE INDEX IF NOT EXISTS "main"."u" ON "t" ("")`, the primary_key
// row plans `CREATE TABLE "main"."t" ("n" int NOT NULL)` with the key silently
// gone, and the partition row is the only one another layer catches -- the
// postgres renderer rejects it with `postgres partition key cannot be empty`,
// at render time and without naming the attribute. The pinned Atlas community
// binary v1.3.0 exits 1 on all five.
func TestParseRefusesAColumnAttributeThatCannotNameAColumn(t *testing.T) {

	tests := []struct {
		name  string
		hcl   string
		match string
	}{
		{
			name: "index on sql call",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = sql("n") }
  }
}
`,
			match: `.*index on column contains unsupported reference "sql\(\\"n\\"\)"`,
		},
		{
			name: "index on number",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = 42 }
  }
}
`,
			match: `.*index on column contains unsupported reference "42"`,
		},
		{
			// The quoted-string spelling is accepted for a NON-empty name (see
			// the control test below), so this row pins that the acceptance is
			// "unquoted to something", not "unquoting succeeded".
			name: "index on empty string",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = "" }
  }
}
`,
			match: `.*index on column contains unsupported reference "\\"\\""`,
		},
		{
			name: "primary key on sql call",
			hcl: `
table "t" {
  column "n" { type = int }
  primary_key {
    on { column = sql("n") }
  }
}
`,
			match: `.*primary_key on column contains unsupported reference "sql\(\\"n\\"\)"`,
		},
		{
			name: "partition by sql call",
			hcl: `
table "t" {
  column "n" { type = int }
  partition {
    type = RANGE
    by { column = sql("n") }
  }
}
`,
			match: `.*partition by column contains unsupported reference "sql\(\\"n\\"\)"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}

// The non-interference control for the refusal above. Every value Ptah could
// already turn into a column name still parses and still names the same column.
// The last row is the one that earns its keep: the quoted-string spelling is a
// Ptah-only extension -- the pinned Atlas community binary v1.3.0 exits 1 on it
// with `missing type in reference "n"` -- and tightening `column` to references
// only would have taken it away from anyone relying on it.
//
// Reverted, this still passes: it pins the boundary of the refusal, not the
// refusal. It goes red under the INVERSE mutant -- making the refusal fire for
// every value -- which is the only way a guard's non-interference is provable.
// Measured under that mutant: all five rows fail on `err` being non-nil.
func TestParseKeepsColumnAttributesThatNameAColumn(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		hcl    string
		assert func(c *qt.C, db *goschema.Database)
	}{
		{
			name: "index on column reference",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = column.n }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts[0].Name, qt.Equals, "n")
			},
		},
		{
			name: "index on qualified table reference",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = table.t.column.n }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts[0].Name, qt.Equals, "n")
			},
		},
		{
			name: "primary key on column reference",
			hcl: `
table "t" {
  column "n" { type = int }
  primary_key {
    on { column = column.n }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Tables, qt.HasLen, 1)
				c.Assert(db.Tables[0].PrimaryKeyParts, qt.DeepEquals, []goschema.PrimaryKeyPart{{Name: "n"}})
			},
		},
		{
			name: "partition by column reference",
			hcl: `
table "t" {
  column "n" { type = int }
  partition {
    type = RANGE
    by { column = column.n }
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
		{
			name: "index on quoted string",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { column = "n" }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts[0].Name, qt.Equals, "n")
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

// Raw SQL in an index part is not lost by the refusal -- it has its own
// attribute. This is the row that makes the refusal a redirection rather than a
// removal: `on { expr = sql("n * 2") }` still plans
// `CREATE INDEX IF NOT EXISTS "main"."idx_n" ON "t" (n * 2)` at exit 0.
//
// Reverted, this still passes; it exists so a future widening of the refusal to
// the `expr` attribute is a deliberate edit with a failing test.
func TestParseKeepsRawSQLInTheIndexExprAttribute(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { expr = sql("n * 2") }
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Parts, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Parts[0].Expr, qt.Equals, "n * 2")
	c.Assert(db.Indexes[0].Parts[0].Name, qt.Equals, "")
}
