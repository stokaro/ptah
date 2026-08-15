package sql_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/sql"
	"go.5x5.cz/ptah/internal/sqllint"
)

func execute(args ...string) (stdout, stderr string, err error) {
	cmd := sql.NewSQLCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func executeWithStdin(stdin string, args ...string) (stdout, stderr string, err error) {
	cmd := sql.NewSQLCommand()
	var out, errOut bytes.Buffer
	cmd.SetIn(bytes.NewBufferString(stdin))
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func TestNewSQLCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := sql.NewSQLCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "sql")
	c.Assert(cmd.Short, qt.Contains, "SQL")
}

func TestSQLLint_FileTextOutput(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c.TB, t.TempDir(), "schema.sql", "CREATE TABLE users (email TEXT NOT NULL);")

	stdout, stderr, err := execute("lint", "--dialect", "postgres", path)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, "warning DDL001")
	c.Assert(stdout, qt.Contains, "table \"users\" has no primary key")
}

func TestSQLLint_MultipleFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	first := writeSQLFile(c.TB, dir, "users.sql", "CREATE TABLE users (email TEXT NOT NULL);")
	second := writeSQLFile(c.TB, dir, "accounts.sql", "CREATE TABLE accounts (name TEXT NOT NULL);")

	stdout, stderr, err := execute("lint", "--dialect", "postgres", first, second)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, "users.sql")
	c.Assert(stdout, qt.Contains, "accounts.sql")
	c.Assert(stdout, qt.Contains, "2 finding(s).")
}

func TestSQLLint_DisableRule(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c.TB, t.TempDir(), "schema.sql", "CREATE TABLE users (email TEXT NOT NULL);")

	stdout, stderr, err := execute("lint", "--dialect", "postgres", "--disable", sqllint.RuleTableWithoutPrimaryKey, path)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, "No SQL lint findings.")
}

func TestSQLLint_JSONOutputForUnsupportedSQLExitsOne(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c.TB, t.TempDir(), "query.sql", "SELECT 1;")

	stdout, stderr, err := execute("lint", "--dialect", "postgres", "--format", "json", path)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	var report struct {
		Failed   bool              `json:"failed"`
		Findings []sqllint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stderr), &report), qt.IsNil)
	c.Assert(report.Failed, qt.IsTrue)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, sqllint.RuleUnsupportedStatement)
}

func TestSQLLint_StdinCleanSQL(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := executeWithStdin(
		"CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL);",
		"lint",
		"--dialect",
		"postgres",
		"--stdin",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, "No SQL lint findings.")
}

func TestSQLLint_CapabilityAwareRuleUsesVersion(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c.TB, t.TempDir(), "index.sql", "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);")

	_, stderr, err := execute("lint", "--dialect", "cockroachdb", "--version", "CockroachDB CCL v23.1.0", path)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "error CAP001")
	c.Assert(stderr, qt.Contains, "create_index_concurrently")
}

func TestSQLLint_UsageErrorsExitTwo(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing input", args: []string{"lint"}},
		{name: "stdin with file", args: []string{"lint", "--stdin", "schema.sql"}},
		{name: "version without dialect", args: []string{"lint", "--version", "16", "--stdin"}},
		{name: "bad format", args: []string{"lint", "--format", "sarif", "--stdin"}},
		{name: "bad dialect", args: []string{"lint", "--dialect", "oracle", "--stdin"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, stderr, err := execute(tt.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error:")
		})
	}
}

// TestSQLLint_VersionThatNamesNoServerExitsTwo is the defect of issue #916
// this command owned: `--version not-a-version` exited 0, linted against the
// dialect default, and printed that string back as the version it had used.
// docs/exit_codes.md reserves 2 for invalid input, and a version string that
// resolves to nothing is invalid input.
func TestSQLLint_VersionThatNamesNoServerExitsTwo(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
	}{
		{name: "postgres word", dialect: "postgres", version: "not-a-version"},
		{name: "postgres sentence", dialect: "postgres", version: "definitely-not-a-version"},
		{name: "mysql word", dialect: "mysql", version: "latest"},
		{name: "mariadb banner without a version", dialect: "mariadb", version: "MariaDB something"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			path := writeSQLFile(c.TB, t.TempDir(), "index.sql", concurrentIndexSQL)

			stdout, stderr, err := execute("lint", "--dialect", tt.dialect, "--version", tt.version, path)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Contains, "--version")
			c.Assert(stderr, qt.Contains, tt.version)
			c.Assert(stderr, qt.Contains, "10.11.6-MariaDB")
		})
	}
}

// TestSQLLint_JSONNeverReportsAVersionThatDidNotResolve is the machine-facing
// half. A report carrying "version": "definitely-not-a-version" beside an
// empty findings list is a record of a lint run against a version that never
// existed, and a consumer has no way to tell it from a real one.
func TestSQLLint_JSONNeverReportsAVersionThatDidNotResolve(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c.TB, t.TempDir(), "index.sql", concurrentIndexSQL)

	stdout, stderr, err := execute(
		"lint", "--dialect", "postgres", "--version", "definitely-not-a-version", "--format", "json", path)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	var report struct {
		Failed  bool   `json:"failed"`
		Version string `json:"version"`
		Error   string `json:"error"`
	}
	c.Assert(json.Unmarshal([]byte(stderr), &report), qt.IsNil)
	c.Assert(report.Failed, qt.IsTrue)
	c.Assert(report.Version, qt.Equals, "")
	c.Assert(report.Error, qt.Contains, "definitely-not-a-version")
}

// TestSQLLint_RecognizedVersionOutcomes pins what the three surviving
// outcomes say. Silence belongs to the exact measured line alone: every other
// resolution planned with a preset the operator did not name, and a run that
// says nothing there reads as a run against the server they asked for.
func TestSQLLint_RecognizedVersionOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		assert  func(c *qt.C, stdout, stderr string, err error)
	}{
		{
			name:    "exact measured line stays silent",
			dialect: "postgres",
			version: "17",
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(stderr, qt.Equals, "")
				c.Assert(stdout, qt.Contains, "No SQL lint findings.")
			},
		},
		{
			name:    "a server newer than the ladder names the line it planned as",
			dialect: "postgres",
			version: "99.0",
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(stderr, qt.Contains, "warning: postgres 99.0 is newer than the newest measured release line 18.x")
				c.Assert(stderr, qt.Contains, "planned as 18.x")
				c.Assert(stdout, qt.Contains, "No SQL lint findings.")
			},
		},
		{
			name:    "a dialect with no ladder says the version changed nothing",
			dialect: "sqlserver",
			version: "16.0.4115.5",
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
				c.Assert(stderr, qt.Contains, "warning: the sqlserver dialect has no measured version ladder")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			path := writeSQLFile(c.TB, t.TempDir(), "index.sql", concurrentIndexSQL)

			stdout, stderr, err := execute("lint", "--dialect", tt.dialect, "--version", tt.version, path)

			tt.assert(c, stdout, stderr, err)
		})
	}
}

// TestSQLLint_JSONCarriesTheVersionNote gives the machine the same fact the
// stderr warning gives a person, rather than leaving JSON consumers with a
// version field they cannot qualify.
func TestSQLLint_JSONCarriesTheVersionNote(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c.TB, t.TempDir(), "index.sql", concurrentIndexSQL)

	stdout, _, err := execute(
		"lint", "--dialect", "postgres", "--version", "99.0", "--format", "json", path)

	c.Assert(err, qt.IsNil)
	var report struct {
		Version     string `json:"version"`
		VersionNote string `json:"version_note"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Version, qt.Equals, "99.0")
	c.Assert(report.VersionNote, qt.Contains, "planned as 18.x")
}

// TestSQLLint_VersionWithoutDialectStillReportsTheOlderError guards the
// pre-existing rule at the seam the new refusal was added to. Resolving the
// version before that rule ran would answer "invalid --version value" to
// someone whose actual mistake was omitting --dialect.
func TestSQLLint_VersionWithoutDialectStillReportsTheOlderError(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("lint", "--version", "not-a-version", "--stdin")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "--version requires --dialect")
	c.Assert(stderr, qt.Not(qt.Contains), "invalid --version value")
}

// TestSQLLint_RefusesABannerFromAnotherServer is the sibling of the same
// refusal on `ptah schema render`.
//
// The defect is one contract with two spellings, so it is closed in
// internal/servertarget rather than at either command: a version naming a
// different product than --dialect resolved silently and the report attributed
// the findings to the dialect the operator asked for. Measured before the
// refusal existed, `--dialect mysql --version 10.11.6-MariaDB` linted against
// MariaDB capabilities at exit 1 and said nothing about it.
func TestSQLLint_RefusesABannerFromAnotherServer(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := executeWithStdin(concurrentIndexSQL,
		"lint", "--dialect", "mysql", "--version", "10.11.6-MariaDB", "--stdin")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "invalid --version: ")
	c.Assert(stderr, qt.Contains, `"10.11.6-MariaDB" names a mariadb server, but the target dialect is mysql`)
}

// TestSQLLint_AcceptsAMatchingBanner is the control for the test above: the
// same banner on the dialect it names still lints.
func TestSQLLint_AcceptsAMatchingBanner(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := executeWithStdin(concurrentIndexSQL,
		"lint", "--dialect", "mariadb", "--version", "10.11.6-MariaDB", "--stdin")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr, qt.Not(qt.Contains), "invalid --version")
	c.Assert(stderr, qt.Contains, "CAP001")
}

const concurrentIndexSQL = "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);"

func writeSQLFile(tb testing.TB, dir, name, statement string) string {
	c := qt.New(tb)
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(statement), 0o600), qt.IsNil)
	return path
}
