package lint_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
	migrationlint "ptah.run/migration/lint"
)

// identifierDirectory writes a directory whose second migration builds an
// index PostgreSQL cuts to the name of the first's: the first name is cut,
// and the second is refused. head is written above the first index.
func identifierDirectory(c *qt.C, config, head string) string {
	c.Helper()
	dir := c.TempDir()
	long := strings.Repeat("k", 63)
	writeLintTestFile(c, dir, "0000000001_init.up.sql",
		"CREATE TABLE t (id int PRIMARY KEY, a int, b int);\n"+head+"CREATE INDEX \""+long+"_alpha\" ON t (a);\n")
	writeLintTestFile(c, dir, "0000000001_init.down.sql", "DROP TABLE t;\n")
	writeLintTestFile(c, dir, "0000000002_index.up.sql", "-- the second index\nCREATE INDEX \""+long+"_beta\" ON t (b);\n")
	writeLintTestFile(c, dir, "0000000002_index.down.sql", "DROP INDEX \""+long+"_beta\";\n")
	writeLintTestFile(c, dir, migrationlint.ConfigFileName, config)
	return dir
}

// PG401P and PG402P reach every report format at the line of the statement
// that writes the name, and PG402P fails the run.
func TestRunLint_IdentifierRulesInEveryFormat(t *testing.T) {
	rows := []struct {
		format string
		want   []string
	}{
		{format: "text", want: []string{"0000000001_init.up.sql:2 [warning] PG401P: index ", "0000000002_index.up.sql:2 [error] PG402P: index "}},
		{format: "json", want: []string{`"rule": "PG401P"`, `"rule": "PG402P"`}},
		{format: "sarif", want: []string{`"ruleId": "PG401P"`, `"ruleId": "PG402P"`}},
		{format: "github-actions", want: []string{
			"0000000001_init.up.sql,line=2::PG401P: index ", "::error file=", "0000000002_index.up.sql,line=2::PG402P: index ",
		}},
		{format: "gitlab", want: []string{`"check_name": "PG401P"`, `"check_name": "PG402P"`}},
	}
	for _, row := range rows {
		t.Run(row.format, func(t *testing.T) {
			c := qt.New(t)
			dir := identifierDirectory(c, "", "")

			stdout, _, err := execute("--dir", dir, "--dir-format", "ptah", "--dialect", "postgres", "--format", row.format)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			for _, want := range row.want {
				c.Assert(stdout, qt.Contains, want)
			}
		})
	}
}

// PG401P and PG402P answer the policy every rule answers: a severity in
// .ptah-lint.yaml promotes a cut name to an error, a nolint directive above a
// statement suppresses it, and --disable takes the error out of the verdict.
func TestRunLint_IdentifierRulesTakeThePolicy(t *testing.T) {
	rows := []struct {
		name     string
		config   string
		head     string
		args     []string
		want     []string
		wantCode int
	}{
		{
			name: "the defaults", want: []string{"[warning] PG401P", "[error] PG402P"}, wantCode: 1,
		},
		{
			name: "a cut name promoted to an error", config: "rules:\n  PG401P:\n    severity: error\n",
			want: []string{"[error] PG401P", "[error] PG402P"}, wantCode: 1,
		},
		{
			name: "a cut name suppressed", head: "-- ptah:nolint PG401P\n", want: []string{"[error] PG402P"}, wantCode: 1,
		},
		{
			name: "the collision disabled", args: []string{"--disable", "PG402P"}, want: []string{"[warning] PG401P"},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := identifierDirectory(c, row.config, row.head)

			stdout, _, err := execute(append([]string{"--dir", dir, "--dir-format", "ptah", "--dialect", "postgres"}, row.args...)...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, row.wantCode)
			c.Assert(identifierVerdicts(stdout), qt.DeepEquals, row.want)
		})
	}
}

// identifierVerdicts is the severity and code of each PG401P and PG402P line
// of a text report, in order.
func identifierVerdicts(report string) []string {
	var verdicts []string
	for line := range strings.Lines(report) {
		_, verdict, found := strings.Cut(line, " [")
		code, _, _ := strings.Cut(verdict, ":")
		if found && (strings.HasSuffix(code, "PG401P") || strings.HasSuffix(code, "PG402P")) {
			verdicts = append(verdicts, "["+code)
		}
	}
	return verdicts
}
