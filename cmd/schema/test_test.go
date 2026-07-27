package schema_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/schema"
)

func writeUsersModel(c *qt.C, dir string) {
	c.Helper()
	content := `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//migrator:schema:field name="name" type="TEXT" not_null="true"
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
