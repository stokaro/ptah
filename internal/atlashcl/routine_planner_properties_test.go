package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// plannerPropertyDocument renders a routine of the given block type carrying
// the given attribute lines.
func plannerPropertyDocument(blockType, attrs string) []byte {
	returns := "\n  return = integer"
	if blockType == "procedure" {
		returns = ""
	}
	return []byte(`
schema "public" {}

` + blockType + ` "r" {
  schema = schema.public
  lang   = SQL` + returns + `
  as     = "SELECT 1"
  ` + attrs + `
}
`)
}

// TestParseFunctionPlannerProperties_HappyPath pins that `leakproof` and
// `parallel` reach the rendered statement.
//
// Both decide how the planner may use the routine, and one of them decides it
// across a security boundary: a leakproof function may have a filter using it
// pushed past a row-level-security predicate. Refusing the attributes left that
// property with no spelling at all.
func TestParseFunctionPlannerProperties_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		attrs   string
		wantSQL string
	}{
		{name: "leakproof", attrs: `leakproof = true`, wantSQL: "LEAKPROOF"},
		{name: "parallel safe", attrs: `parallel = SAFE`, wantSQL: "PARALLEL SAFE"},
		{name: "parallel restricted", attrs: `parallel = RESTRICTED`, wantSQL: "PARALLEL RESTRICTED"},
		{name: "parallel unsafe stated", attrs: `parallel = UNSAFE`, wantSQL: "PARALLEL UNSAFE"},
		{name: "lowercase level", attrs: `parallel = "safe"`, wantSQL: "PARALLEL SAFE"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(plannerPropertyDocument("function", row.attrs), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Functions, qt.HasLen, 1)
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, row.wantSQL)
		})
	}
}

// TestParseFunctionWithoutPlannerProperties_HappyPath is the control for the
// reader above.
//
// Neither clause is the server's default, so a routine that states nothing must
// render exactly as it did. Writing the defaults out would change the DDL of
// every routine that never asked about either property.
func TestParseFunctionWithoutPlannerProperties_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(plannerPropertyDocument("function", `volatility = STABLE`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Leakproof, qt.IsFalse)
	c.Assert(db.Functions[0].Parallel, qt.Equals, "")
	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(sql, qt.Not(qt.Contains), "LEAKPROOF")
	c.Assert(sql, qt.Not(qt.Contains), "PARALLEL")
}

// TestParseProcedurePlannerProperties_FailurePath pins that a procedure is
// refused both attributes.
//
// Measured on PostgreSQL 17: `CREATE PROCEDURE ... LEAKPROOF` and the same
// statement with `PARALLEL SAFE` each answer `ERROR: invalid attribute in
// procedure definition`. Accepting them here would render a statement the
// engine cannot take, which is the refusal `volatility` already carries.
func TestParseProcedurePlannerProperties_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		attrs string
		want  string
	}{
		{name: "leakproof", attrs: `leakproof = true`, want: `unsupported procedure attribute "leakproof"`},
		{name: "parallel", attrs: `parallel = SAFE`, want: `unsupported procedure attribute "parallel"`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(plannerPropertyDocument("procedure", row.attrs), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want+`.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseFunctionParallelRefusesAnythingElse_FailurePath pins that an
// unrecognized level stops the parse.
//
// Neither direction is a defensible guess: folding a misspelled SAFE into
// UNSAFE quietly forbids the parallelism the author asked for, and folding a
// misspelled UNSAFE into SAFE quietly permits what they did not.
func TestParseFunctionParallelRefusesAnythingElse_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		attrs string
	}{
		{name: "misspelled", attrs: `parallel = SAEF`},
		{name: "unrelated word", attrs: `parallel = PARALLEL`},
		{name: "empty string", attrs: `parallel = ""`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(plannerPropertyDocument("function", row.attrs), "schema.hcl")

			c.Assert(err, qt.ErrorMatches,
				`(?s).*function parallel must be SAFE, RESTRICTED or UNSAFE.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}
