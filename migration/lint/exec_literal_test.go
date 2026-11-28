package lint_test

import (
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// sqlServerRules is each finding's rule and line, in the order lint reports
// them, for one SQL Server migration file holding sql, linted with no dev
// database, so a statement the parser cannot read is reported.
func sqlServerRules(c *qt.C, sql string) []string {
	c.Helper()
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), routineFS(sql), lint.Options{
		DirFormat: migrationfile.DirFormatAtlas,
		Dialect:   "sqlserver",
		Unparsed:  lint.UnparsedAll,
	})
	c.Assert(err, qt.IsNil)
	rules := make([]string, 0, len(findings))
	for _, finding := range findings {
		rules = append(rules, finding.Rule+" "+finding.File+":"+strconv.Itoa(finding.Line))
	}
	return rules
}

// TestLintFS_RatesTheStatementsAnExecLiteralRuns lints a statement SQL Server
// runs from the literal of an EXEC as it lints the same statement written
// directly, on the line of the EXEC: a DROP TABLE and a TRUNCATE report their
// destructive rules, and a DELETE without WHERE reports what the direct form
// reports. The literal's statements are parsed, so no unparsed-statement
// finding stands in for them.
func TestLintFS_RatesTheStatementsAnExecLiteralRuns(t *testing.T) {
	tests := []struct {
		name   string
		direct string
		exec   string
	}{
		{name: "DROP TABLE in EXEC", direct: "SELECT 1;\nDROP TABLE dbo.x;\n", exec: "SELECT 1;\nEXEC(N'DROP TABLE dbo.x');\n"},
		{name: "DROP TABLE in sp_executesql", direct: "SELECT 1;\nDROP TABLE dbo.x;\n", exec: "SELECT 1;\nEXEC sp_executesql N'DROP TABLE dbo.x';\n"},
		{name: "TRUNCATE in EXEC", direct: "SELECT 1;\nTRUNCATE TABLE dbo.x;\n", exec: "SELECT 1;\nEXEC(N'TRUNCATE TABLE dbo.x');\n"},
		{name: "TRUNCATE in sp_executesql", direct: "SELECT 1;\nTRUNCATE TABLE dbo.x;\n", exec: "SELECT 1;\nEXEC sp_executesql @stmt = N'TRUNCATE TABLE dbo.x';\n"},
		{name: "DELETE without WHERE in EXEC", direct: "SELECT 1;\nDELETE FROM dbo.x;\n", exec: "SELECT 1;\nEXEC(N'DELETE FROM dbo.x');\n"},
		{name: "DELETE without WHERE in sp_executesql", direct: "SELECT 1;\nDELETE FROM dbo.x;\n", exec: "SELECT 1;\nEXEC sp_executesql N'DELETE FROM dbo.x';\n"},
		{name: "a doubled quote", direct: "SELECT 1;\nDROP TABLE [it's];\n", exec: "SELECT 1;\nEXEC(N'DROP TABLE [it''s]');\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlServerRules(c, test.exec), qt.DeepEquals, sqlServerRules(c, test.direct))
		})
	}
}

// TestLintFS_RatesADropAnExecLiteralRuns pins the control for the parity rows
// above: a DROP TABLE an EXEC literal runs reports the destructive and the
// rollout drop rules on the line of the EXEC, as a direct one does.
func TestLintFS_RatesADropAnExecLiteralRuns(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlServerRules(c, "SELECT 1;\nEXEC(N'DROP TABLE dbo.x');\n"), qt.DeepEquals, []string{
		"destructive/drop-table 1_routine.sql:2",
		"rollout/drop-table 1_routine.sql:2",
	})
}

// TestLintFS_KeepsTheTextAnExecComputesUnread reports an EXEC of a variable or
// of a concatenation as a statement the parser could not read, since the text
// it runs exists only when it runs.
func TestLintFS_KeepsTheTextAnExecComputesUnread(t *testing.T) {
	for _, sql := range []string{"EXEC(@sql);\n", "EXEC(N'DROP TABLE dbo.x' + N'');\n"} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlServerRules(c, sql), qt.DeepEquals, []string{"coverage/unparsed-statement 1_routine.sql:1"})
		})
	}
}

// TestLintFS_DeclaredRulesReadTheTextAnExecLiteralRuns runs a project rule
// that matches `it's` in the statement text. The literal of the EXEC doubles
// the quote in that name, so the rule matches only the text the server runs,
// and it reports on the line of the EXEC as it does on a direct statement.
func TestLintFS_DeclaredRulesReadTheTextAnExecLiteralRuns(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "direct", sql: "SELECT 1;\nDROP TABLE [it's];\n"},
		{name: "in EXEC", sql: "SELECT 1;\nEXEC(N'DROP TABLE [it''s]');\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rule := lint.RuleConfig{Match: `strcontains(statement.sql, "[it's]")`, Message: "a quote in a name"}

			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), routineFS(test.sql),
				declaredRuleOptions("design/quoted-name", rule, "sqlserver"))

			c.Assert(err, qt.IsNil)
			findings := findingsFor(analysis.Findings(), "design/quoted-name")
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Line, qt.Equals, 2)
		})
	}
}
