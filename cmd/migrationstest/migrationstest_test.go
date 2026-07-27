package migrationstest_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrationstest"
)

func writeMigrationPair(c *qt.C, dir, name, up, down string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name+".up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name+".down.sql"), []byte(down), 0o600), qt.IsNil)
}

func runTestCommand(args ...string) (string, error) {
	cmd := migrationstest.NewMigrationsTestCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

func TestMigrationsTestCommand_Passes(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	testsDir := t.TempDir()
	writeMigrationPair(c, migrationsDir, "0000000001_create_users",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", "DROP TABLE users;")
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users table works\n"+
			"    steps:\n"+
			"      - name: migrate\n"+
			"        migrate_to: latest\n"+
			"      - name: insert\n"+
			"        exec: INSERT INTO users (name) VALUES ('ada')\n"+
			"      - name: one user\n"+
			"        assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"1\"\n"), 0o600), qt.IsNil)

	out, err := runTestCommand("--dir", testsDir, "--migrations-dir", migrationsDir, "--dir-format", "ptah")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "PASS  case \"users table works\"")
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestMigrationsTestCommand_FailsWithNonZeroError(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "fail.yaml"), []byte(
		"cases:\n"+
			"  - name: bad expectation\n"+
			"    steps:\n"+
			"      - name: create\n"+
			"        exec: CREATE TABLE t (id INTEGER PRIMARY KEY)\n"+
			"      - name: wrong count\n"+
			"        assert:\n"+
			"          query: SELECT id FROM t\n"+
			"          row_count: 5\n"), 0o600), qt.IsNil)

	out, err := runTestCommand("--dir", testsDir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "migration tests failed")
	c.Assert(out, qt.Contains, "FAIL  case \"bad expectation\"")
}

func TestMigrationsTestCommand_NoCasesFound(t *testing.T) {
	c := qt.New(t)
	_, err := runTestCommand("--dir", t.TempDir())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no test cases found")
}

func TestMigrationsTestCommand_RejectsUnsupportedReport(t *testing.T) {
	c := qt.New(t)
	_, err := runTestCommand("--dir", t.TempDir(), "--report", "xml")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "unsupported report format")
}
