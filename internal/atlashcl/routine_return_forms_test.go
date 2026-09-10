package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// returnFormDocument renders a routine of the given block type carrying the
// given attribute lines.
func returnFormDocument(blockType, attrs string) []byte {
	return []byte(`
schema "public" {}

` + blockType + ` "r" {
  schema = schema.public
  lang   = SQL
  as     = "SELECT 1"
  ` + attrs + `
}
`)
}

// TestParseFunctionReturnForms_HappyPath pins the two return clauses a scalar
// `return` cannot spell.
//
// The keywords stay uppercase while the type names go to lowercase, which is
// what `pg_get_function_result` prints back. A lowercased keyword would differ
// from the catalog row on every comparison and the plan would replace a routine
// nobody touched (stokaro/ptah#3121).
func TestParseFunctionReturnForms_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		attrs   string
		wantSQL string
	}{
		{
			name:    "a set of a scalar",
			attrs:   "return = integer\n  return_set = true",
			wantSQL: "RETURNS SETOF integer",
		},
		{
			name:    "an uppercase scalar is still lowered",
			attrs:   "return = INTEGER\n  return_set = true",
			wantSQL: "RETURNS SETOF integer",
		},
		{
			name:    "a table of columns",
			attrs:   "return_table = {\n    a = integer\n    b = text\n  }",
			wantSQL: "RETURNS TABLE(a integer, b text)",
		},
		{
			name:    "a quoted column name",
			attrs:   "return_table = {\n    \"a\" = integer\n  }",
			wantSQL: "RETURNS TABLE(a integer)",
		},
		{
			name:    "a plain scalar is unchanged",
			attrs:   "return = integer",
			wantSQL: "RETURNS integer",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(returnFormDocument("function", row.attrs), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Functions, qt.HasLen, 1)
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, row.wantSQL)
		})
	}
}

// TestParseFunctionReturnTableKeepsDeclaredOrder_HappyPath pins that the
// columns come back in the order they were written.
//
// A TABLE clause's order is the shape of the row the function returns, and an
// HCL object has no order of its own. Reading the evaluated object and sorting
// it would return the author's columns in a different order than they wrote,
// and no diagnostic could report it: both spellings are valid, and the server
// would accept the wrong one.
func TestParseFunctionReturnTableKeepsDeclaredOrder_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		attrs   string
		wantSQL string
	}{
		{
			name:    "already alphabetical",
			attrs:   "return_table = {\n    aa = integer\n    zz = text\n  }",
			wantSQL: "RETURNS TABLE(aa integer, zz text)",
		},
		{
			name:    "reverse alphabetical",
			attrs:   "return_table = {\n    zz = text\n    aa = integer\n  }",
			wantSQL: "RETURNS TABLE(zz text, aa integer)",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(returnFormDocument("function", row.attrs), "schema.hcl")

			c.Assert(err, qt.IsNil)
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, row.wantSQL)
		})
	}
}

// TestParseFunctionReturnForms_FailurePath pins the combinations the server
// refuses.
//
// Measured on PostgreSQL 17, each is a syntax error there too: `RETURNS integer
// RETURNS TABLE(...)` and `RETURNS SETOF TABLE(...)` both answer `syntax error
// at or near "TABLE"`. Accepting any of them here would render a statement no
// server creates.
func TestParseFunctionReturnForms_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		attrs string
		want  string
	}{
		{
			name:  "a scalar and a table",
			attrs: "return = integer\n  return_table = {\n    a = integer\n  }",
			want:  `function cannot declare both return and return_table`,
		},
		{
			name:  "a set and a table",
			attrs: "return_set = true\n  return_table = {\n    a = integer\n  }",
			want:  `function cannot declare both return_set and return_table`,
		},
		{
			name:  "a set of nothing",
			attrs: "return_set = true",
			want:  `function return_set requires return`,
		},
		{
			name:  "a table of no columns",
			attrs: "return_table = {}",
			want:  `function return_table declares no columns`,
		},
		{
			name:  "a table that is not an object",
			attrs: `return_table = "a integer"`,
			want:  `function return_table must be an object of name = type pairs`,
		},
		{
			name:  "a column with no type",
			attrs: "return_table = {\n    a = \"\"\n  }",
			want:  `function return_table column "a" declares no type`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(returnFormDocument("function", row.attrs), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want+`.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseProcedureReturnForms_FailurePath pins that a procedure is refused
// both, the way it is already refused `return`.
//
// Measured on PostgreSQL 17: `CREATE PROCEDURE ... RETURNS SETOF integer` and
// the TABLE form each answer a syntax error, because a procedure returns
// nothing at all.
func TestParseProcedureReturnForms_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		attrs string
		want  string
	}{
		{name: "a set", attrs: "return_set = true", want: `unsupported procedure attribute "return_set"`},
		{
			name:  "a table",
			attrs: "return_table = {\n    a = integer\n  }",
			want:  `unsupported procedure attribute "return_table"`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(returnFormDocument("procedure", row.attrs), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want+`.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}
