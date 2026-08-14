package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// Atlas's sql("X") escape hatch must reach the IR as X. Before issue #1106 the
// value helpers fell back to the attribute's SOURCE TEXT for every position the
// grammar reads as a plain string, so the IR carried the literal `sql("X")` and
// the renderer emitted DDL no engine accepts -- `CHECK (sql("n > 0"))`.
//
// Reverted, every row below fails on the same shape: the field holds
// `sql("...")` where the row asserts the SQL inside it.
func TestParseReducesSQLRawExpressionToItsSQL(t *testing.T) {
	tests := []struct {
		name   string
		hcl    string
		assert func(c *qt.C, db *goschema.Database)
	}{
		{
			name: "table check expr",
			hcl: `
table "t" {
  column "n" { type = int }
  check "n_positive" { expr = sql("n > 0") }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Constraints, qt.HasLen, 1)
				c.Assert(db.Constraints[0].CheckExpression, qt.Equals, "n > 0")
			},
		},
		{
			name: "index where",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    columns = [column.n]
    where   = sql("n > 5")
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Condition, qt.Equals, "n > 5")
			},
		},
		{
			name: "index part expr",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    on { expr = sql("n + 1") }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts[0].Expr, qt.Equals, "n + 1")
			},
		},
		{
			name: "index part prefix",
			hcl: `
table "t" {
  column "n" { type = text }
  index "idx_n" {
    on {
      column = column.n
      prefix = sql("4")
    }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Parts[0].Prefix, qt.Equals, "4")
			},
		},
		{
			name: "index comment",
			hcl: `
table "t" {
  column "n" { type = int }
  index "idx_n" {
    columns = [column.n]
    comment = sql("hello")
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].Comment, qt.Equals, "hello")
			},
		},
		{
			name: "generated column as attribute",
			hcl: `
table "t" {
  column "n" { type = int }
  column "g" {
    type = int
    as   = sql("n * 2")
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields, qt.HasLen, 2)
				c.Assert(db.Fields[1].GeneratedExpression, qt.Equals, "n * 2")
			},
		},
		{
			name: "generated column as block expr",
			hcl: `
table "t" {
  column "n" { type = int }
  column "g" {
    type = int
    as {
      expr = sql("n * 2")
      type = VIRTUAL
    }
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields, qt.HasLen, 2)
				c.Assert(db.Fields[1].GeneratedExpression, qt.Equals, "n * 2")
			},
		},
		{
			name: "column type",
			hcl: `
table "t" {
  column "n" { type = sql("varchar(10)") }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields, qt.HasLen, 1)
				c.Assert(db.Fields[0].Type, qt.Equals, "varchar(10)")
			},
		},
		{
			name: "column comment",
			hcl: `
table "t" {
  column "n" {
    type    = int
    comment = sql("hello")
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields, qt.HasLen, 1)
				c.Assert(db.Fields[0].Comment, qt.Equals, "hello")
			},
		},
		{
			name: "table comment",
			hcl: `
table "t" {
  comment = sql("hello")
  column "n" { type = int }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Tables, qt.HasLen, 1)
				c.Assert(db.Tables[0].Comment, qt.Equals, "hello")
			},
		},
		{
			name: "view body",
			hcl: `
table "t" {
  column "n" { type = int }
}
view "v" {
  as = sql("SELECT n FROM t")
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Views, qt.HasLen, 1)
				c.Assert(db.Views[0].Body, qt.Equals, "SELECT n FROM t")
			},
		},
		{
			name: "function body",
			hcl: `
function "f" {
  lang   = SQL
  return = sql("text")
  as     = sql("SELECT 'x'")
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Functions, qt.HasLen, 1)
				c.Assert(db.Functions[0].Body, qt.Equals, "SELECT 'x'")
				c.Assert(db.Functions[0].Returns, qt.Equals, "text")
			},
		},
		{
			name: "column default keeps its expression role",
			hcl: `
table "t" {
  column "n" {
    type    = int
    default = sql("now()")
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields, qt.HasLen, 1)
				// A sql() default is an EXPRESSION, not a literal. The
				// structural match has to keep that branch alive: a default
				// landing in Default instead would be rendered quoted.
				c.Assert(db.Fields[0].DefaultExpr, qt.Equals, "now()")
				c.Assert(db.Fields[0].Default, qt.Equals, "")
			},
		},
		{
			// The call is matched on the parsed expression, not on the source
			// text. The textual match this replaced looked for a `sql(` prefix
			// and a `)` suffix and unquoted whatever sat between them, so a
			// single space inside the parentheses defeated the unquoting and
			// the DEFAULT reached the renderer as ` "now()" ` -- quotes,
			// padding and all.
			name: "argument padded inside the parentheses",
			hcl: `
table "t" {
  column "n" {
    type    = int
    default = sql( "now()" )
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields, qt.HasLen, 1)
				c.Assert(db.Fields[0].DefaultExpr, qt.Equals, "now()")
			},
		},
		{
			name: "function argument type",
			hcl: `
function "f" {
  arg "user_id" { type = sql("bigint") }
  lang   = SQL
  return = text
  as     = "SELECT 'x'"
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Functions, qt.HasLen, 1)
				c.Assert(db.Functions[0].Parameters, qt.Equals, "user_id bigint")
			},
		},
		{
			// Reference-shaped attributes read the attribute's source text by
			// design -- `schema = schema.app` has no string value to evaluate.
			// That path has to reduce sql() too, or the schema name becomes the
			// literal `sql("app")` and every qualified identifier built from it
			// is unusable.
			name: "table schema reference",
			hcl: `
table "t" {
  schema = sql("app")
  column "n" { type = int }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Tables, qt.HasLen, 1)
				c.Assert(db.Tables[0].Schema, qt.Equals, "app")
			},
		},
		{
			// A heredoc is a string to HCL and to the community binary, and it
			// carries no quotes for a textual match to strip.
			name: "heredoc argument",
			hcl: `
table "t" {
  column "n" {
    type    = int
    default = sql(<<-SQL
now()
SQL
    )
  }
}
`,
			assert: func(c *qt.C, db *goschema.Database) {
				c.Assert(db.Fields, qt.HasLen, 1)
				c.Assert(db.Fields[0].DefaultExpr, qt.Equals, "now()\n")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")
			c.Assert(err, qt.IsNil)
			test.assert(c, db)
		})
	}
}

// A sql() call Ptah cannot reduce to SQL text is refused outright rather than
// rendered from its source. This is the guard that keeps the #1106 decision
// honest: reducing sql() is only safe if no other sql() spelling can reach the
// renderer. The pinned Atlas community binary v1.3.0 refuses every shape below
// too, so refusing them costs no drop-in compatibility.
//
// Reverted, each row parses without error and the file plans: `sql(1)` reaches
// DDL as the literal text `sql(1)`, and the "default split across two calls"
// row plans as `DEFAULT "1") + sql("2"` -- the old textual `sql(` prefix match
// handing back the bytes between the first `sql(` and the last `)`.
func TestParseRefusesMalformedSQLRawExpression(t *testing.T) {
	tests := []struct {
		name  string
		hcl   string
		match string
	}{
		{
			name: "no arguments",
			hcl: `
table "t" {
  column "n" { type = int }
  check "c" { expr = sql() }
}
`,
			match: `.*attribute "expr": sql\(\) takes exactly one string argument, got 0`,
		},
		{
			name: "two arguments",
			hcl: `
table "t" {
  column "n" { type = int }
  check "c" { expr = sql("n > 0", "x") }
}
`,
			match: `.*attribute "expr": sql\(\) takes exactly one string argument, got 2`,
		},
		{
			name: "number argument",
			hcl: `
table "t" {
  column "n" { type = int }
  check "c" { expr = sql(1) }
}
`,
			match: `.*attribute "expr": sql\(\) takes a string argument`,
		},
		{
			name: "nested call argument",
			hcl: `
table "t" {
  column "n" { type = int }
  check "c" { expr = sql(sql("n > 0")) }
}
`,
			match: `.*attribute "expr": sql\(\) takes a string argument`,
		},
		{
			name: "expanded argument list",
			hcl: `
table "t" {
  column "n" { type = int }
  check "c" { expr = sql(["n > 0"]...) }
}
`,
			match: `.*attribute "expr": sql\(\) does not take an expanded argument list`,
		},
		{
			name: "part of a larger expression",
			hcl: `
table "t" {
  column "n" { type = int }
  check "c" { expr = sql("n > 0") + sql("1") }
}
`,
			match: `.*attribute "expr": sql\(\) must be the whole attribute value, not part of a larger expression`,
		},
		{
			name: "default split across two calls",
			hcl: `
table "t" {
  column "n" {
    type    = int
    default = sql("1") + sql("2")
  }
}
`,
			match: `.*attribute "default": sql\(\) must be the whole attribute value, not part of a larger expression`,
		},
		{
			name: "inside a list",
			hcl: `
table "t" {
  checks = [sql("n > 0")]
  column "n" { type = int }
}
`,
			match: `.*attribute "checks": sql\(\) must be the whole attribute value, not part of a larger expression`,
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

// The refusal names the offender that comes FIRST in the file. Attributes hang
// off a Go map, so without the explicit ordering the reported position is
// whichever key the runtime happened to visit first and the message flips
// between runs.
//
// Reverted, this test fails intermittently -- the second table's `expr` is
// reported roughly half the time.
func TestParseRefusesTheFirstMalformedSQLRawExpressionInTheFile(t *testing.T) {
	c := qt.New(t)

	// Both offenders sit in the SAME body, which is what puts them in one Go
	// map. Two offenders in two different table blocks would be visited in
	// slice order and would not discriminate.
	source := `
table "t" {
  comment = sql(1)
  charset = sql(2)
  column "n" { type = int }
}
`

	for range 64 {
		_, err := atlashcl.Parse([]byte(source), "schema.hcl")
		c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema.hcl:3,13-19: attribute "comment": .*`)
	}
}

// A sql() argument carrying an interpolation is reduced through the file's own
// variables. Measured against the pinned Atlas community binary v1.3.0,
// `default = sql("${var.floor} + 1")` is planned there as `5 + 1`, so it must
// neither be refused nor pass its source text through -- both of which this
// file did before issue #926 was fixed.
//
// Reverted, DefaultExpr holds the template source `${var.floor} + 1` and the
// equality assertion fails printing that string. Reverted to a refusal
// instead, the nil-error assertion fails.
func TestParseReducesSQLRawExpressionInterpolation(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
variable "floor" {
  type    = number
  default = 5
}
table "t" {
  column "n" {
    type    = int
    default = sql("${var.floor} + 1")
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].DefaultExpr, qt.Equals, "5 + 1")
}

// A function call that is not sql() and is not in the evaluation context's
// function set is refused rather than copied into the IR as source text.
//
// This is the position issue #926 left open and #1106 deliberately did not
// widen into. Measured, the same file on the pinned binary exits 1 with
// `Call to unknown function; There is no function named "sqlx".`, so refusing
// here is what matches rather than what over-reaches.
//
// Reverted, this test fails on the non-nil-error assertion, and the parse
// instead yields a check constraint whose expression is the literal text
// `sqlx("n > 0")`.
func TestParseRefusesUnknownFunctionCalls(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "t" {
  column "n" { type = int }
  check "c" { expr = sqlx("n > 0") }
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*call to unknown function: There is no function named "sqlx".*`)
}
