package sql_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/sqllint"
)

// identifierSQL builds an index PostgreSQL cuts on line 2, and on line 3 an
// index it cuts to the same name and refuses.
var identifierSQL = "CREATE TABLE t (id int PRIMARY KEY, a int, b int);\n" +
	"CREATE INDEX \"" + strings.Repeat("k", 63) + "_alpha\" ON t (a);\n" +
	"  CREATE INDEX \"" + strings.Repeat("k", 63) + "_beta\" ON t (b);\n"

// A file's text report places each name where it is written, and the
// collision fails the run.
func TestSQLLint_IdentifierRulesInText(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c, t.TempDir(), "schema.sql", identifierSQL)

	stdout, _, err := execute("lint", "--dialect", "postgres", path)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout, qt.Contains, "schema.sql:2:14: warning DDL003: index ")
	c.Assert(stdout, qt.Contains, "schema.sql:3:16: error DDL004: index ")
}

// The JSON report read from stdin carries the same findings, with the
// position, the suggested fix and the failed verdict.
func TestSQLLint_IdentifierRulesInJSON(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := executeWithStdin(identifierSQL, "lint", "--dialect", "postgres", "--stdin", "--format", "json")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	var report struct {
		Failed   bool              `json:"failed"`
		Findings []sqllint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Failed, qt.IsTrue)
	var at []string
	for _, finding := range report.Findings {
		at = append(at, finding.Rule+" "+string(finding.Severity)+" "+finding.File+" "+finding.SuggestedFix)
	}
	c.Assert(at, qt.DeepEquals, []string{
		"DDL003 warning <stdin> Write a name of at most 63 bytes, such as \"" + strings.Repeat("k", 63) + "\", so the SQL names what the database holds.",
		"DDL004 error <stdin> Rename one of them so the two differ within the first 63 bytes.",
	})
}
