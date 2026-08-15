package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// The HCL a row needs twice: once for the position that must hold the reduced
// SQL, and once for the position that must stay empty because of it.
const (
	sqlDefaultHCL = `
table "t" {
  column "n" {
    type    = int
    default = sql("now()")
  }
}
`
	sqlFunctionHCL = `
function "f" {
  lang   = SQL
  return = sql("text")
  as     = sql("SELECT 'x'")
}
`
)

// Atlas's sql("X") escape hatch must reach the IR as X. Before issue #1106 the
// value helpers fell back to the attribute's SOURCE TEXT for every position the
// grammar reads as a plain string, so the IR carried the literal `sql("X")` and
// the renderer emitted DDL no engine accepts -- `CHECK (sql("n > 0"))`.
//
// Reverted, every row below fails on the same shape: the field holds
// `sql("...")` where the row wants the SQL inside it.
//
// A row names the IR position with `read` and the whole list that position must
// hold with `want`. The list rather than one element, because "the check
// constraint reduced" and "exactly one check constraint exists" are the same
// measurement: a parse that dropped the object, or invented a second one, is
// not a parse that reduced it.
func TestParseReducesSQLRawExpressionToItsSQL(t *testing.T) {
	tests := []struct {
		name string
		hcl  string
		read func(db *goschema.Database) []string
		want []string
	}{
		{
			name: "table check expr",
			hcl: `
table "t" {
  column "n" { type = int }
  check "n_positive" { expr = sql("n > 0") }
}
`,
			read: checkExpressions,
			want: []string{"n > 0"},
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
			read: indexConditions,
			want: []string{"n > 5"},
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
			read: indexPartExpressions,
			want: []string{"n + 1"},
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
			read: indexPartPrefixes,
			want: []string{"4"},
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
			read: indexComments,
			want: []string{"hello"},
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
			read: fieldGeneratedExpressions,
			want: []string{"", "n * 2"},
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
			read: fieldGeneratedExpressions,
			want: []string{"", "n * 2"},
		},
		{
			name: "column type",
			hcl: `
table "t" {
  column "n" { type = sql("varchar(10)") }
}
`,
			read: fieldTypes,
			want: []string{"varchar(10)"},
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
			read: fieldComments,
			want: []string{"hello"},
		},
		{
			name: "table comment",
			hcl: `
table "t" {
  comment = sql("hello")
  column "n" { type = int }
}
`,
			read: tableComments,
			want: []string{"hello"},
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
			read: viewBodies,
			want: []string{"SELECT n FROM t"},
		},
		{
			name: "function body",
			hcl:  sqlFunctionHCL,
			read: functionBodies,
			want: []string{"SELECT 'x'"},
		},
		{
			name: "function return type",
			hcl:  sqlFunctionHCL,
			read: functionReturns,
			want: []string{"text"},
		},
		{
			name: "column default keeps its expression role",
			hcl:  sqlDefaultHCL,
			read: fieldDefaultExpressions,
			want: []string{"now()"},
		},
		{
			// A sql() default is an EXPRESSION, not a literal. The structural
			// match has to keep that branch alive: a default landing in Default
			// instead would be rendered quoted.
			name: "column default is not also a literal",
			hcl:  sqlDefaultHCL,
			read: fieldDefaults,
			want: []string{""},
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
			read: fieldDefaultExpressions,
			want: []string{"now()"},
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
			read: functionParameters,
			want: []string{"user_id bigint"},
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
			read: tableSchemas,
			want: []string{"app"},
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
			read: fieldDefaultExpressions,
			want: []string{"now()\n"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(test.read(db), qt.DeepEquals, test.want)
		})
	}
}

// The IR positions a sql() call can land in. Each returns what the position
// holds across the whole parsed file, so a row names a position rather than
// indexing into a slice that a regression may have left shorter.

func projectStrings[T any](items []T, read func(item T) string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, read(item))
	}
	return out
}

func checkExpressions(db *goschema.Database) []string {
	return projectStrings(db.Constraints, func(item goschema.Constraint) string { return item.CheckExpression })
}

func indexConditions(db *goschema.Database) []string {
	return projectStrings(db.Indexes, func(item goschema.Index) string { return item.Condition })
}

func indexComments(db *goschema.Database) []string {
	return projectStrings(db.Indexes, func(item goschema.Index) string { return item.Comment })
}

func indexPartExpressions(db *goschema.Database) []string {
	return projectStrings(indexParts(db), func(item goschema.IndexPart) string { return item.Expr })
}

func indexPartPrefixes(db *goschema.Database) []string {
	return projectStrings(indexParts(db), func(item goschema.IndexPart) string { return item.Prefix })
}

func indexParts(db *goschema.Database) []goschema.IndexPart {
	var parts []goschema.IndexPart
	for _, index := range db.Indexes {
		parts = append(parts, index.Parts...)
	}
	return parts
}

func fieldGeneratedExpressions(db *goschema.Database) []string {
	return projectStrings(db.Fields, func(item goschema.Field) string { return item.GeneratedExpression })
}

func fieldTypes(db *goschema.Database) []string {
	return projectStrings(db.Fields, func(item goschema.Field) string { return item.Type })
}

func fieldComments(db *goschema.Database) []string {
	return projectStrings(db.Fields, func(item goschema.Field) string { return item.Comment })
}

func fieldDefaultExpressions(db *goschema.Database) []string {
	return projectStrings(db.Fields, func(item goschema.Field) string { return item.DefaultExpr })
}

func fieldDefaults(db *goschema.Database) []string {
	return projectStrings(db.Fields, func(item goschema.Field) string { return item.Default })
}

func tableComments(db *goschema.Database) []string {
	return projectStrings(db.Tables, func(item goschema.Table) string { return item.Comment })
}

func tableSchemas(db *goschema.Database) []string {
	return projectStrings(db.Tables, func(item goschema.Table) string { return item.Schema })
}

func viewBodies(db *goschema.Database) []string {
	return projectStrings(db.Views, func(item goschema.View) string { return item.Body })
}

func functionBodies(db *goschema.Database) []string {
	return projectStrings(db.Functions, func(item goschema.Function) string { return item.Body })
}

func functionReturns(db *goschema.Database) []string {
	return projectStrings(db.Functions, func(item goschema.Function) string { return item.Returns })
}

func functionParameters(db *goschema.Database) []string {
	return projectStrings(db.Functions, func(item goschema.Function) string { return item.Parameters })
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
