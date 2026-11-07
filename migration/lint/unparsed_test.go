package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/lintext"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// analyzeUnparsed lints one up file and one down file with the given scope.
func analyzeUnparsed(c *qt.C, dialect, up, down string, scope lint.UnparsedScope) []lint.Finding {
	c.Helper()
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_x.up.sql":   up + "\n",
		"0000000001_x.down.sql": down + "\n",
	}), lint.Options{Dialect: dialect, Unparsed: scope})
	c.Assert(err, qt.IsNil)
	return findings
}

// ofRule keeps the findings of one rule, so a row can name a statement another rule
// also reports without restating that rule's verdict.
func ofRule(findings []lint.Finding, rule string) []lint.Finding {
	var kept []lint.Finding
	for _, finding := range findings {
		if finding.Rule == rule {
			kept = append(kept, finding)
		}
	}
	return kept
}

// TestAC102_ReportsAStatementNoServerRuns covers each scope that reports, in the
// direction it reports. The reason is the parser's own, without the position it
// counts from the statement.
func TestAC102_ReportsAStatementNoServerRuns(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		up       string
		down     string
		scope    lint.UnparsedScope
		wantFile string
		wantLine int
		want     string
	}{
		{
			name: "up file", dialect: "sqlite", up: "SELECT 1;\nCREATE TABLE b BOGUS;", down: "SELECT 1;",
			scope: lint.UnparsedAll, wantFile: "0000000001_x.up.sql", wantLine: 2, want: "unsupported table option: BOGUS",
		},
		{
			name: "down file", dialect: "sqlite", up: "SELECT 1;", down: "DROP TABEL t;",
			scope: lint.UnparsedAll, wantFile: "0000000001_x.down.sql", wantLine: 1, want: "unsupported DROP target: TABEL",
		},
		{
			name: "down file when a server runs the up file", dialect: "sqlite", up: "SELECT 1;", down: "DROP TABEL t;",
			scope: lint.UnparsedDown, wantFile: "0000000001_x.down.sql", wantLine: 1, want: "unsupported DROP target: TABEL",
		},
		{
			name: "a run that names no dialect", up: "FOO BAR BAZ;", down: "SELECT 1;",
			scope: lint.UnparsedAll, wantFile: "0000000001_x.up.sql", wantLine: 1, want: "unsupported SQL statement: FOO",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			found := ofRule(analyzeUnparsed(c, test.dialect, test.up, test.down, test.scope), "AC102")
			c.Assert(found, qt.HasLen, 1)
			c.Assert(found[0].File, qt.Equals, test.wantFile)
			c.Assert(found[0].Line, qt.Equals, test.wantLine)
			c.Assert(found[0].Severity, qt.Equals, lint.SeverityInfo)
			c.Assert(found[0].Message, qt.Contains, "("+test.want+")")
		})
	}
}

// TestAC102_StaysSilent is the other direction: the same text out of scope, a
// statement the parser reads, and a dialect the parser holds no grammar for.
func TestAC102_StaysSilent(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		up      string
		down    string
		scope   lint.UnparsedScope
	}{
		{name: "the zero scope", dialect: "sqlite", up: "CREATE TABLE b BOGUS;", down: "DROP TABEL t;"},
		{name: "a server runs the up file", dialect: "sqlite", up: "CREATE TABLE b BOGUS;", down: "SELECT 1;", scope: lint.UnparsedDown},
		{name: "the parser reads it", dialect: "sqlite", up: "CREATE TABLE t (id INT);", down: "DROP TABLE t;", scope: lint.UnparsedAll},
		{name: "a comment reads as no statement", dialect: "sqlite", up: "-- nothing\nSELECT 1;", down: "-- nothing", scope: lint.UnparsedAll},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			found := ofRule(analyzeUnparsed(c, test.dialect, test.up, test.down, test.scope), "AC102")
			c.Assert(found, qt.HasLen, 0)
		})
	}
}

// TestAC102_StaysSilentWhereTheProviderReadsTheDialect runs one statement under the
// same owner split on the two kinds of dialect. YQL is read by its provider, so the
// host parser's refusal says nothing about it. The PostgreSQL run is the control that
// the statement reaches the rule at all.
func TestAC102_StaysSilentWhereTheProviderReadsTheDialect(t *testing.T) {
	const statement = "UPSERT INTO t (id) VALUES (1)"
	tests := []struct {
		target string
		want   int
	}{
		{target: "ydb", want: 0},
		{target: "postgres", want: 1},
	}
	for _, test := range tests {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			var requests []lintext.SplitRequest
			runtime := must.Must(engine.New(engine.Provider{ID: "example.org/" + test.target, Targets: []engine.Target{{
				Name: test.target,
				Splitter: recordingSplitter{requests: &requests, statements: []lintext.Statement{{
					SQL: statement, Canonical: statement, Start: 0, End: len(statement), Line: 1,
				}}},
			}}}))

			findings, err := lint.LintFS(c.Context(), runtime, fixture(map[string]string{
				"0000000001_x.up.sql": statement + "\n",
			}), lint.Options{Dialect: test.target, Unparsed: lint.UnparsedAll, Disabled: []string{"MF"}})

			c.Assert(err, qt.IsNil)
			c.Assert(ofRule(findings, "AC102"), qt.HasLen, test.want)
		})
	}
}

// TestAC102_ValidStatementsThatMeetAParserGapStayGreen holds why the rule does not
// fail a run. Every row is a statement measured against a real server of its
// dialect, except Spanner's, which no local server here checks: the server accepts
// it and the parser refuses it. A refusal would turn these files red, so each one is
// reported at a severity no default threshold fails on.
func TestAC102_ValidStatementsThatMeetAParserGapStayGreen(t *testing.T) {
	tests := []struct {
		dialect   string
		statement string
	}{
		{dialect: "postgres", statement: "DROP AGGREGATE a(int);"},
		{dialect: "postgres", statement: "CREATE COLLATION c (locale = 'C');"},
		{dialect: "postgres", statement: "CREATE TABLE t3 (LIKE t INCLUDING ALL);"},
		{dialect: "mysql", statement: "ALTER TABLE t ENGINE=InnoDB;"},
		{dialect: "mariadb", statement: "CREATE TABLE s (a INT) WITH SYSTEM VERSIONING;"},
		{dialect: "sqlite", statement: "ATTACH DATABASE 'x.db' AS x;"},
		{dialect: "sqlserver", statement: "CREATE CLUSTERED INDEX i ON t (a);"},
		{dialect: "sqlserver", statement: "EXEC sp_rename 't.b', 'c', 'COLUMN';"},
		{dialect: "clickhouse", statement: "ALTER TABLE t ADD COLUMN b Int32 AFTER a;"},
		{dialect: "cockroachdb", statement: "ALTER TABLE t SPLIT AT VALUES (1);"},
		{dialect: "yugabytedb", statement: "CREATE TABLE u (a INT) SPLIT INTO 4 TABLETS;"},
		{dialect: "spanner", statement: "CREATE NULL_FILTERED INDEX i ON t (a);"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.statement, func(t *testing.T) {
			c := qt.New(t)
			found := ofRule(analyzeUnparsed(c, test.dialect, test.statement, "SELECT 1;", lint.UnparsedAll), "AC102")
			c.Assert(found, qt.HasLen, 1)
			c.Assert(found[0].Severity, qt.Equals, lint.SeverityInfo)
		})
	}
}

// TestAC102_LeavesTheTokenRulesAlone keeps the report from reading as "nothing ran":
// the rules scan a statement's tokens, so a drop the parser cannot read is still a
// drop, and both findings stand.
func TestAC102_LeavesTheTokenRulesAlone(t *testing.T) {
	c := qt.New(t)
	findings := analyzeUnparsed(c, "postgres", "DROP TABLE t BOGUS;", "SELECT 1;", lint.UnparsedAll)
	c.Assert(ofRule(findings, "AC102"), qt.HasLen, 1)
	c.Assert(ofRule(findings, "DS101"), qt.HasLen, 1)
}

// TestAC102_FollowsThePolicy holds the controls every rule has: a policy can raise
// it to a gate, turn it off by code or by family, and silence one statement.
func TestAC102_FollowsThePolicy(t *testing.T) {
	const unparsed = "CREATE TABLE b BOGUS;"

	t.Run("raised to error", func(t *testing.T) {
		c := qt.New(t)
		findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
			"0000000001_x.up.sql":   unparsed + "\n",
			"0000000001_x.down.sql": "SELECT 1;\n",
		}), lint.Options{
			Dialect: "sqlite", Unparsed: lint.UnparsedAll,
			RuleConfigs: map[string]lint.RuleConfig{"AC102": {Severity: lint.SeverityError}},
		})
		c.Assert(err, qt.IsNil)
		found := ofRule(findings, "AC102")
		c.Assert(found, qt.HasLen, 1)
		c.Assert(found[0].Severity, qt.Equals, lint.SeverityError)
	})
	for _, selector := range []string{"AC102", "AC"} {
		t.Run("disabled by "+selector, func(t *testing.T) {
			c := qt.New(t)
			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_x.up.sql":   unparsed + "\n",
				"0000000001_x.down.sql": "SELECT 1;\n",
			}), lint.Options{Dialect: "sqlite", Unparsed: lint.UnparsedAll, Disabled: []string{selector}})
			c.Assert(err, qt.IsNil)
			c.Assert(ofRule(findings, "AC102"), qt.HasLen, 0)
		})
	}
	t.Run("silenced on one statement", func(t *testing.T) {
		c := qt.New(t)
		found := ofRule(analyzeUnparsed(c, "sqlite", "-- ptah:nolint AC102\n"+unparsed, "SELECT 1;", lint.UnparsedAll), "AC102")
		c.Assert(found, qt.HasLen, 0)
	})
}
