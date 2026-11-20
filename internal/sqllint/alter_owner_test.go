package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqllint"
)

func lintOwnerStatement(c *qt.C, dialect, sql string) []sqllint.Finding {
	c.Helper()
	findings, err := sqllint.LintSource(c.Context(),
		sqllint.Source{Name: "t.sql", SQL: sql},
		sqllint.Options{SQL: builtintest.Runtime().SQL(), Bodies: builtintest.Runtime(), Dialect: dialect},
	)
	c.Assert(err, qt.IsNil)
	return findings
}

// An ownership statement is read, so it is not coverage/unsupported-statement. No rule examines it, so
// the file is not reported clean either: it carries the one coverage/unanalyzed-statements line every
// file of statements without a rule gets.
func TestLintSource_AnOwnerChangeIsRead_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "table", sql: "ALTER TABLE public.t OWNER TO postgres;"},
		{name: "schema", sql: "ALTER SCHEMA app OWNER TO postgres;"},
		{name: "function with arguments", sql: "ALTER FUNCTION app.f(integer) OWNER TO postgres;"},
		{name: "materialized view", sql: "ALTER MATERIALIZED VIEW app.mv OWNER TO postgres;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := lintOwnerStatement(c, "postgres", test.sql)

			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Rule, qt.Equals, sqllint.RuleStatementsNotAnalyzed)
			c.Assert(findings[0].Severity, qt.Equals, sqllint.SeverityInfo)
			c.Assert(findings[0].Message, qt.Contains, "ALTER")
		})
	}
}

// A dialect whose server has no OWNER TO still reports the statement as one the
// parser does not model (coverage/unsupported-statement), and a malformed one is a parse error (correctness/syntax-error).
func TestLintSource_AnOwnerChange_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, wantRule, wantMessage string
	}{
		{name: "mysql", dialect: "mysql", sql: "ALTER TABLE t OWNER TO r;", wantRule: "coverage/unsupported-statement", wantMessage: "unsupported ALTER operation: OWNER"},
		{name: "no role", dialect: "postgres", sql: "ALTER TABLE t OWNER TO;", wantRule: "correctness/syntax-error", wantMessage: "expected the role after ALTER TABLE OWNER TO"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := lintOwnerStatement(c, test.dialect, test.sql)

			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Rule, qt.Equals, test.wantRule)
			c.Assert(findings[0].Severity, qt.Equals, sqllint.SeverityError)
			c.Assert(findings[0].Message, qt.Contains, test.wantMessage)
		})
	}
}
