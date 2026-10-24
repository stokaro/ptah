//go:build integration

package providercli_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// sqlLint runs `ptah sql lint` over one file holding sql with the provider's
// manifest, once the provider process has ended, and returns the report's
// findings as line:column rule message, its error, and the exit code.
func (f lintProvider) sqlLint(c *qt.C, sql string, args ...string) ([]string, string, int) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
	out, diagnostics, code := executeProviderCLI(c.Context(),
		append(append([]string{"sql", "lint", "--provider-config", f.manifest, "--format", "json"}, args...), path))
	providerCLI{pidPath: f.pidPath}.assertStopped(c)
	report := struct {
		Findings []struct {
			Rule    string `json:"rule"`
			Line    int    `json:"line"`
			Column  int    `json:"column"`
			Message string `json:"message"`
		} `json:"findings"`
		Error string `json:"error"`
	}{}
	// A refusal is written to stderr and a report to stdout.
	c.Assert(json.Unmarshal([]byte(out+diagnostics), &report), qt.IsNil, qt.Commentf("stdout %q, stderr %q", out, diagnostics))
	findings := make([]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		findings = append(findings, fmt.Sprintf("%d:%d %s %s", finding.Line, finding.Column, finding.Rule, finding.Message))
	}
	return findings, report.Error, code
}

// ownedSQL holds a statement of each kind the fixture judges.
const ownedSQL = "CREATE TABLE a (id INTEGER, price DECIMAL(10,2));\nUPSERT INTO a VALUES (1);\nDROP TABLE a;\nALTER TABLE b ADD COLUMN c INTEGER;\n"

// TestSQLLintReadsTheProvidersStatementFacts lints a file of an engine Ptah
// has no grammar for. The provider splits it and says what each statement is,
// and the rules read that: the table created without a primary key, the
// statement the linter does not lint, and the DROP TABLE no rule examines.
// Ptah knows no release line of the engine, so the capability the DECIMAL
// column needs is reported as unexamined rather than as unavailable. The
// provider's only engine is the run's dialect when --dialect names none.
func TestSQLLintReadsTheProvidersStatementFacts(t *testing.T) {
	c := qt.New(t)
	f := newLintProvider(c, "lint-facts", "example.org/fixture", "fixture")
	for _, row := range []struct {
		name string
		args []string
	}{
		{name: "the named engine", args: []string{"--dialect", "fixture"}},
		{name: "the provider's only engine"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, refusal, code := f.sqlLint(c, ownedSQL, row.args...)

			c.Assert(refusal, qt.Equals, "")
			c.Assert(code, qt.Equals, 1)
			c.Assert(findings, qt.DeepEquals, []string{
				`1:1 DDL001 table "a" has no primary key`,
				"2:1 SQL002 ptah sql lint does not lint UPSERT statements yet",
				"1:1 SQL004 no rule examined the parameterized_decimal requirement of COLUMN price of table a, DROP TABLE in this file",
			})
		})
	}
}

// TestSQLLintComparesTheProvidersRequirementsWithTheServerVersion names the
// ydb dialect, which the fixture serves under the official provider's
// identity, so the host resolves a server version to the capabilities of that
// YDB release line. YDB 25.1 has no parameterized DECIMAL and 26.2 has one.
// The primary key finding carries the provider path's wording, not the
// built-in YQL reading's, which names YDB's refusal.
func TestSQLLintComparesTheProvidersRequirementsWithTheServerVersion(t *testing.T) {
	c := qt.New(t)
	f := newLintProvider(c, "lint-facts", ydbProviderID, "ydb", "FIXTURE_LINT_ENGINE=ydb")
	const sql = "CREATE TABLE a (id Uint64, price Decimal(22,9), PRIMARY KEY (id));\nCREATE TABLE b (id Uint64);\n"
	for _, row := range []struct {
		version string
		code    int
		want    []string
	}{
		{version: "25.1", code: 1, want: []string{
			"1:1 CAP001 COLUMN price of table a requires target capability parameterized_decimal, unavailable on this target",
			`2:1 DDL001 table "b" has no primary key`,
		}},
		{version: "26.2", code: 0, want: []string{`2:1 DDL001 table "b" has no primary key`}},
	} {
		t.Run(row.version, func(t *testing.T) {
			c := qt.New(t)

			findings, refusal, code := f.sqlLint(c, sql, "--dialect", "ydb", "--server-version", row.version)

			c.Assert(refusal, qt.Equals, "")
			c.Assert(findings, qt.DeepEquals, row.want)
			c.Assert(code, qt.Equals, row.code)
		})
	}
}

// TestSQLLintRefusesAProviderThatDoesNotSayWhatAStatementIs selects the
// fixture's contract 25.0 lint, whose split carries no statement kinds. The
// rules have nothing to read, and the built-in reading does not stand in for
// the provider's, so the run is refused before any finding.
func TestSQLLintRefusesAProviderThatDoesNotSayWhatAStatementIs(t *testing.T) {
	c := qt.New(t)
	f := newLintProvider(c, "lint", "example.org/fixture", "fixture")

	findings, refusal, code := f.sqlLint(c, ownedSQL, "--dialect", "fixture")

	c.Assert(code, qt.Equals, 2)
	c.Assert(refusal, qt.Equals, "the owner of fixture does not report what each statement is, so ptah sql lint cannot lint its SQL")
	c.Assert(findings, qt.HasLen, 0)
}
