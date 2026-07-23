package sql_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/sql"
	"github.com/stokaro/ptah/internal/sqllint"
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
	path := writeSQLFile(c, t.TempDir(), "schema.sql", "CREATE TABLE users (email TEXT NOT NULL);")

	stdout, stderr, err := execute("lint", "--dialect", "postgres", path)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, "warning DDL001")
	c.Assert(stdout, qt.Contains, "table \"users\" has no primary key")
}

func TestSQLLint_MultipleFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	first := writeSQLFile(c, dir, "users.sql", "CREATE TABLE users (email TEXT NOT NULL);")
	second := writeSQLFile(c, dir, "accounts.sql", "CREATE TABLE accounts (name TEXT NOT NULL);")

	stdout, stderr, err := execute("lint", "--dialect", "postgres", first, second)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, "users.sql")
	c.Assert(stdout, qt.Contains, "accounts.sql")
	c.Assert(stdout, qt.Contains, "2 finding(s).")
}

func TestSQLLint_DisableRule(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c, t.TempDir(), "schema.sql", "CREATE TABLE users (email TEXT NOT NULL);")

	stdout, stderr, err := execute("lint", "--dialect", "postgres", "--disable", sqllint.RuleTableWithoutPrimaryKey, path)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, "No SQL lint findings.")
}

func TestSQLLint_JSONOutputForUnsupportedSQLExitsOne(t *testing.T) {
	c := qt.New(t)
	path := writeSQLFile(c, t.TempDir(), "query.sql", "SELECT 1;")

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
	path := writeSQLFile(c, t.TempDir(), "index.sql", "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);")

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

func writeSQLFile(c *qt.C, dir, name, statement string) string {
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(statement), 0o600), qt.IsNil)
	return path
}
