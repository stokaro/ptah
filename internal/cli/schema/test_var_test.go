package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// writeVariableTestCase writes a `.test.hcl` suite whose single case asserts
// that `var.who` resolved to want.
func writeVariableTestCase(c *qt.C, dir, variableBody, want string) {
	c.Helper()
	content := `variable "who" {
` + variableBody + `
}

test "schema" "greets" {
  exec {
    sql    = "SELECT '${var.who}' AS who"
    output = "` + want + `"
  }
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "who.test.hcl"), []byte(content), 0o600), qt.IsNil)
}

// TestSchemaTestCommand_VarReachesTheTestDocument_HappyPath is the wiring the
// unit tests in migration/dbtest cannot see.
//
// `--var` was registered on this verb, forwarded to the schema-file load, and
// never handed to the test-case loader, so the flag was live on the run and
// inert on the payload (stokaro/ptah#3119). Only a run through the command tree
// shows that the two ends are joined.
func TestSchemaTestCommand_VarReachesTheTestDocument_HappyPath(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	writeVariableTestCase(c, testsDir, "  type = string", "ada")

	out, err := runSchemaTestCommand("--root-dir", modelsDir, "--dir", testsDir, "--var", "who=ada")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(out, qt.Contains, "1 passed")
}

// TestSchemaTestCommand_VarOverridesTheDefault_HappyPath pins the precedence
// through the command, with the default deliberately wrong so the case can only
// pass on the supplied value.
func TestSchemaTestCommand_VarOverridesTheDefault_HappyPath(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	writeVariableTestCase(c, testsDir, "  type    = string\n  default = \"ada\"", "grace")

	out, err := runSchemaTestCommand("--root-dir", modelsDir, "--dir", testsDir, "--var", "who=grace")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(out, qt.Contains, "1 passed")
}

// TestSchemaTestCommand_WithoutVarTheDefaultStands_FailurePath is the control
// for the test above: the same suite without the flag runs on the default and
// fails, which is what makes the passing run evidence that the flag arrived.
func TestSchemaTestCommand_WithoutVarTheDefaultStands_FailurePath(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	writeVariableTestCase(c, testsDir, "  type    = string\n  default = \"ada\"", "grace")

	out, err := runSchemaTestCommand("--root-dir", modelsDir, "--dir", testsDir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, `expected result set "grace", got "ada"`)
}
