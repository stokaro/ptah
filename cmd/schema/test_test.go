package schema_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/schema"
)

var errTestWrite = errors.New("test writer failed")

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errTestWrite
}

func writeUsersModel(c *qt.C, dir string) {
	c.Helper()
	content := `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "user.go"), []byte(content), 0o600), qt.IsNil)
}

// runSchemaTestCommand runs "schema test" through the full command tree so
// registration in NewSchemaCommand is exercised alongside the command itself.
func runSchemaTestCommand(args ...string) (string, error) {
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"test"}, args...))
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

func TestSchemaTestCommand_Passes(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users schema works\n"+
			"    steps:\n"+
			"      - name: starts empty\n"+
			"        assert:\n"+
			"          query: SELECT * FROM users\n"+
			"          row_count: 0\n"+
			"      - name: insert\n"+
			"        exec: INSERT INTO users (id, name) VALUES (1, 'ada')\n"+
			"      - name: one user\n"+
			"        assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"1\"\n"), 0o600), qt.IsNil)

	out, err := runSchemaTestCommand("--dir", testsDir, "--root-dir", modelsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "PASS  case \"users schema works\"")
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestSchemaTestCommand_DefaultSeedDirectory(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	seedsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(seedsDir, "010_users.test.sql"),
		[]byte("INSERT INTO users (id, name) VALUES (1, 'ada');"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "seed.yaml"), []byte(
		"cases:\n"+
			"  - name: default seed directory works\n"+
			"    steps:\n"+
			"      - seed:\n"+
			"          env: test\n"+
			"      - assert:\n"+
			"          query: SELECT name FROM users\n"+
			"          scalar: ada\n"), 0o600), qt.IsNil)

	out, err := runSchemaTestCommand(
		"--dir", testsDir,
		"--root-dir", modelsDir,
		"--seed-dir", seedsDir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "default seed directory works"`)
}

func TestSchemaTestCommand_ReportWriteFailure(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "pass.yaml"), []byte(
		"cases:\n"+
			"  - name: passing case\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT id FROM users\n"+
			"          row_count: 0\n"), 0o600), qt.IsNil)
	cmd := schema.NewSchemaCommand()
	cmd.SetOut(failingWriter{})
	cmd.SetArgs([]string{"test", "--dir", testsDir, "--root-dir", modelsDir})

	err := cmd.ExecuteContext(context.Background())

	c.Assert(err, qt.ErrorIs, errTestWrite)
}

func TestSchemaTestCommand_FailsWithNonZeroError(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "fail.yaml"), []byte(
		"cases:\n"+
			"  - name: bad expectation\n"+
			"    steps:\n"+
			"      - name: wrong count\n"+
			"        assert:\n"+
			"          query: SELECT * FROM users\n"+
			"          row_count: 5\n"), 0o600), qt.IsNil)

	out, err := runSchemaTestCommand("--dir", testsDir, "--root-dir", modelsDir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "schema tests failed")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, "FAIL  case \"bad expectation\"")
}

func TestSchemaTestCommand_NoCasesFound(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	_, err := runSchemaTestCommand("--dir", t.TempDir(), "--root-dir", modelsDir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no test cases found")
}

func TestSchemaTestCommand_RejectsUnsupportedReport(t *testing.T) {
	c := qt.New(t)
	_, err := runSchemaTestCommand("--dir", t.TempDir(), "--report", "xml")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "unsupported report format")
}

func TestSchemaTestCommand_RunPattern(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: selected case\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT id FROM users\n"+
			"          row_count: 0\n"+
			"  - name: excluded case\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT id FROM missing_table\n"+
			"          row_count: 0\n"), 0o600), qt.IsNil)

	out, err := runSchemaTestCommand("--dir", testsDir, "--root-dir", modelsDir, "--run", "^selected")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "selected case"`)
	c.Assert(out, qt.Not(qt.Contains), "excluded case")
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestSchemaTestCommand_RunPatternNoMatches(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: existing case\n"+
			"    steps:\n"+
			"      - exec: SELECT 1\n"), 0o600), qt.IsNil)

	_, err := runSchemaTestCommand("--dir", testsDir, "--run", "^missing$")

	c.Assert(err, qt.ErrorMatches, `no test cases match --run "\^missing\$"`)
}
