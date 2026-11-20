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

// naming/identifier-too-long and naming/identifier-collision reach every report format at the line of the statement
// that writes the name, and naming/identifier-collision fails the run.
func TestRunLint_IdentifierRulesInEveryFormat(t *testing.T) {
	rows := []struct {
		format string
		want   []string
	}{
		{format: "text", want: []string{"0000000001_init.up.sql:2 [warning] naming/identifier-too-long: index ", "0000000002_index.up.sql:2 [error] naming/identifier-collision: index "}},
		{format: "json", want: []string{`"rule": "naming/identifier-too-long"`, `"rule": "naming/identifier-collision"`}},
		{format: "sarif", want: []string{`"ruleId": "naming/identifier-too-long"`, `"ruleId": "naming/identifier-collision"`}},
		{format: "github-actions", want: []string{
			"0000000001_init.up.sql,line=2::naming/identifier-too-long: index ", "::error file=", "0000000002_index.up.sql,line=2::naming/identifier-collision: index ",
		}},
		{format: "gitlab", want: []string{`"check_name": "naming/identifier-too-long"`, `"check_name": "naming/identifier-collision"`}},
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

// naming/identifier-too-long and naming/identifier-collision answer the policy every rule answers: a severity in
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
			name: "the defaults", want: []string{"[warning] naming/identifier-too-long", "[error] naming/identifier-collision"}, wantCode: 1,
		},
		{
			name: "a cut name promoted to an error", config: "rules:\n  naming/identifier-too-long:\n    severity: error\n",
			want: []string{"[error] naming/identifier-too-long", "[error] naming/identifier-collision"}, wantCode: 1,
		},
		{
			name: "a cut name suppressed", head: "-- ptah:nolint naming/identifier-too-long\n", want: []string{"[error] naming/identifier-collision"}, wantCode: 1,
		},
		{
			name: "the collision disabled", args: []string{"--disable", "naming/identifier-collision"}, want: []string{"[warning] naming/identifier-too-long"},
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

// identifierVerdicts is the severity and code of each naming/identifier-too-long and naming/identifier-collision line
// of a text report, in order.
func identifierVerdicts(report string) []string {
	var verdicts []string
	for line := range strings.Lines(report) {
		_, verdict, found := strings.Cut(line, " [")
		head, _, _ := strings.Cut(verdict, ":")
		severity, rule, _ := strings.Cut(head, "] ")
		code, _, _ := strings.Cut(rule, " ")
		if found && (code == "naming/identifier-too-long" || code == "naming/identifier-collision") {
			verdicts = append(verdicts, "["+severity+"] "+code)
		}
	}
	return verdicts
}
