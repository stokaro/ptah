package lintexpr_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/lintexpr"
)

// alterAddVarchar is one statement in the forms a rule sees it in.
func alterAddVarchar() lintexpr.Scope {
	return lintexpr.Scope{
		SQL:       "ALTER TABLE users ADD COLUMN nick VARCHAR(20); -- widen later",
		Canonical: "ALTER TABLE USERS ADD COLUMN NICK VARCHAR(20);",
		Words: []string{
			"ALTER", "TABLE", "USERS", "ADD", "COLUMN", "NICK", "VARCHAR", "(", "20", ")", ";",
		},
		Line:    7,
		Path:    "migrations/0000000001_init.up.sql",
		IsUp:    true,
		Dialect: "postgres",
	}
}

// TestCompileAndEvaluate covers the vocabulary a rule is written in.
//
// Each row is a name or a function the rule language promises, so a row failing
// means the promise broke rather than that an expression changed. The scope
// field names ARE the documented surface: renaming one breaks every rule
// already written against it (stokaro/ptah#1706).
func TestCompileAndEvaluate(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       bool
	}{
		{name: "sql substring", expression: `strcontains(lower(statement.sql), "varchar(")`, want: true},
		{name: "sql substring absent", expression: `strcontains(statement.sql, "JSONB")`, want: false},
		{name: "canonical", expression: `strcontains(statement.canonical, "ADD COLUMN")`, want: true},
		// The canonical form drops comments, which is the difference that makes
		// it the right field for a rule about structure.
		{name: "canonical drops comments", expression: `strcontains(statement.canonical, "widen later")`, want: false},
		{name: "words membership", expression: `contains(statement.words, "VARCHAR")`, want: true},
		{name: "words are whole tokens", expression: `contains(statement.words, "VAR")`, want: false},
		{name: "line", expression: `statement.line == 7`, want: true},
		{name: "file path", expression: `strcontains(file.path, ".up.sql")`, want: true},
		{name: "direction up", expression: `file.is_up`, want: true},
		{name: "direction down", expression: `file.is_down`, want: false},
		{name: "dialect", expression: `dialect == "postgres"`, want: true},
		{name: "boolean composition", expression: `file.is_up && !contains(statement.words, "DROP")`, want: true},
		{name: "regex", expression: `length(regexall("VARCHAR\\(\\d+\\)", statement.canonical)) > 0`, want: true},
		{name: "conditional", expression: `dialect == "mysql" ? false : true`, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			expression, err := lintexpr.Compile("XX101", test.expression)
			c.Assert(err, qt.IsNil)
			got, err := expression.Evaluate("XX101", alterAddVarchar())

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestCompileRefusesUnknownNames pins the compile-time name check.
//
// Left to evaluation, a misspelled root would fail once per statement -- or be
// reported as a rule that simply never fires, which reads as "the code is
// clean". A rule that never fires and a rule that passes are the same output.
func TestCompileRefusesUnknownNames(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{name: "misspelled root", expression: `stmt.sql != ""`},
		{name: "invented root", expression: `migration.version > 1`},
		{name: "nested under an unknown root", expression: `env.name == "local"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := lintexpr.Compile("XX101", test.expression)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "unknown name")
			// The message lists what IS available, so the fix is in the error.
			c.Assert(err.Error(), qt.Contains, "dialect, file, statement")
		})
	}
}

// TestCompileRefusesMalformedExpressions covers the two shapes that are not an
// expression at all.
func TestCompileRefusesMalformedExpressions(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		message    string
	}{
		{name: "empty", expression: "   ", message: "match expression is empty"},
		{name: "syntax error", expression: `contains(statement.words,`, message: "parse match expression"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := lintexpr.Compile("XX101", test.expression)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.message)
		})
	}
}

// TestEvaluateRefusesNonBooleanResults keeps a coerced truth value out of the
// language.
//
// An expression returning a string would otherwise fire on every statement or
// on none depending on the coercion, and both look like a working rule.
func TestEvaluateRefusesNonBooleanResults(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{name: "string", expression: `upper(statement.sql)`},
		{name: "number", expression: `statement.line`},
		{name: "list", expression: `statement.words`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			expression, err := lintexpr.Compile("XX101", test.expression)
			c.Assert(err, qt.IsNil)

			_, err = expression.Evaluate("XX101", alterAddVarchar())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "must evaluate to a boolean")
		})
	}
}

// TestEvaluateNamesTheFixForContainsOnAString covers the one mistake this
// language invites.
//
// `contains` tests list membership, so `contains(statement.sql, "x")` -- the
// spelling everyone reaches for first -- fails with a type error about lists
// that says nothing about what to write instead.
func TestEvaluateNamesTheFixForContainsOnAString(t *testing.T) {
	c := qt.New(t)
	expression, err := lintexpr.Compile("XX101", `contains(statement.sql, "VARCHAR")`)
	c.Assert(err, qt.IsNil)

	_, err = expression.Evaluate("XX101", alterAddVarchar())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "strcontains(haystack, needle)")
}

// TestEvaluateOnAStatementWithNoWords is the empty-list case.
//
// An untyped empty list makes `contains` fail with a type error rather than
// returning false, so a rule using statement.words would break on exactly the
// statements it has nothing to say about.
func TestEvaluateOnAStatementWithNoWords(t *testing.T) {
	c := qt.New(t)
	expression, err := lintexpr.Compile("XX101", `contains(statement.words, "DROP")`)
	c.Assert(err, qt.IsNil)

	got, err := expression.Evaluate("XX101", lintexpr.Scope{})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.IsFalse)
}

// TestRuleExpressionsCannotReachTheMachine is the property that makes a lint
// finding reproducible.
//
// A rule that could read a file or the environment would report findings that
// depend on the machine it ran on: the same migration would lint clean on one
// checkout and fail in CI with nothing in the migration to explain it. `print`
// is absent for a different reason -- it would interleave with the report on
// the same stream.
func TestRuleExpressionsCannotReachTheMachine(t *testing.T) {
	tests := []string{"file", "fileset", "getenv", "print"}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			expression, err := lintexpr.Compile("XX101", name+`("x")`)
			c.Assert(err, qt.IsNil)

			_, err = expression.Evaluate("XX101", alterAddVarchar())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "no function named")
		})
	}
}

// TestSourceIsTheAuthorsSpelling keeps diagnostics quoting what was typed.
func TestSourceIsTheAuthorsSpelling(t *testing.T) {
	c := qt.New(t)

	expression, err := lintexpr.Compile("XX101", `  file.is_up  `)

	c.Assert(err, qt.IsNil)
	c.Assert(expression.Source(), qt.Equals, "file.is_up")
}
