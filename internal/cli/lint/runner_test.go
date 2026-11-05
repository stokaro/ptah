package lint_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
	migrationlint "ptah.run/migration/lint"
)

// ownTransactionDirectory writes a directory whose up file opens and commits a
// transaction of its own, and a ptah.yaml holding config beside it, and runs
// the command from there.
func ownTransactionDirectory(t *testing.T, c *qt.C, config string) string {
	t.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	writeLintTestFile(c, root, "ptah.yaml", config)
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	writeLintTestFile(c, dir, "0000000001_backfill.up.sql", "BEGIN;\nCREATE TABLE t (id int);\nCOMMIT;\n")
	writeLintTestFile(c, dir, "0000000001_backfill.down.sql", "DROP TABLE t;\n")
	t.Chdir(root)
	return dir
}

// decodeReportFindings is the findings of a JSON report.
func decodeReportFindings(c *qt.C, stdout string) []migrationlint.Finding {
	c.Helper()
	var report struct {
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("%s", stdout))
	return report.Findings
}

// TX201 judges a file by the tool that applies the directory: Ptah's
// migrator wraps the file, so its own BEGIN nests, and golang-migrate opens
// no transaction, so the file's block is its only one. The runner comes from
// --runner, or from migration.runner in ptah.yaml when the flag is not set.
func TestRunLint_RunnerDecidesTX201(t *testing.T) {
	tests := []struct {
		name   string
		config string
		flags  []string
		want   []string
	}{
		{name: "the default runner", want: []string{"TX201"}},
		{name: "golang-migrate named by the flag", flags: []string{"--runner", "golang-migrate"}, want: make([]string, 0)},
		{name: "golang-migrate named in ptah.yaml", config: "migration:\n  runner: golang-migrate\n", want: make([]string, 0)},
		{name: "the flag over ptah.yaml", config: "migration:\n  runner: golang-migrate\n", flags: []string{"--runner", "ptah"}, want: []string{"TX201"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := ownTransactionDirectory(t, c, test.config)

			stdout, stderr, err := execute(append([]string{"--dir", dir, "--dialect", "postgres", "--format", "json"}, test.flags...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(reportRulesOf(decodeReportFindings(c, stdout)), qt.DeepEquals, test.want)
		})
	}
}

// A runner Ptah does not model is a usage error, from the flag or from
// ptah.yaml, before any file is read.
func TestRunLint_RunnerRefusesAnUnknownTool(t *testing.T) {
	tests := []struct {
		name   string
		config string
		flags  []string
	}{
		{name: "the flag", flags: []string{"--runner", "goose"}},
		{name: "ptah.yaml", config: "migration:\n  runner: goose\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := ownTransactionDirectory(t, c, test.config)

			stdout, stderr, err := execute(append([]string{"--dir", dir, "--dialect", "postgres"}, test.flags...)...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(strings.TrimSpace(stderr), qt.Equals, `error: invalid runner "goose": expected ptah or golang-migrate`)
		})
	}
}
