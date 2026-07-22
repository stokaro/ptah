package lint_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	cmdlint "github.com/stokaro/ptah/cmd/lint"
	"github.com/stokaro/ptah/dbschema"
	migrationlint "github.com/stokaro/ptah/migration/lint"
)

func TestRunLint_DevURLReplaysMigrationAndInfersDialect(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeLintDevURLFile(c, migrationsDir, "1_create_lint_dev_url.sql",
		"CREATE TABLE lint_dev_url (id INTEGER PRIMARY KEY);\n")

	stdout, _, err := executeLintCommand(
		"--dir", migrationsDir,
		"--dev-url", "sqlite://"+devDBPath,
		"--format", "json",
		"--fail-on", "none",
	)

	c.Assert(err, qt.IsNil)
	var report struct {
		Dialect  string                  `json:"dialect"`
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Dialect, qt.Equals, "sqlite")
	assertLintDevURLSQLiteTableCount(c, devDBPath, "lint_dev_url", 1)
}

func TestRunLint_DevURLFailureExitsTwo(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeLintDevURLFile(c, migrationsDir, "1_bad_lint_dev_url.sql",
		"INSERT INTO missing_lint_dev_url_table (id) VALUES (1);\n")

	_, stderr, err := executeLintCommand(
		"--dir", migrationsDir,
		"--dev-url", "sqlite://"+devDBPath,
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "error validating migration SQL on dev database")
}

func TestRunLint_DevURLRejectsDialectMismatch(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeLintDevURLFile(c, migrationsDir, "1_create_lint_dev_url.sql",
		"CREATE TABLE lint_dev_url_mismatch (id INTEGER PRIMARY KEY);\n")

	_, stderr, err := executeLintCommand(
		"--dir", migrationsDir,
		"--dialect", "postgres",
		"--dev-url", "sqlite://"+devDBPath,
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, `lint dialect "postgres" does not match --dev-url dialect "sqlite"`)
}

func executeLintCommand(args ...string) (stdout, stderr string, err error) {
	cmd := cmdlint.NewLintCommand()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func writeLintDevURLFile(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
}

func assertLintDevURLSQLiteTableCount(c *qt.C, dbPath, table string, want int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}
