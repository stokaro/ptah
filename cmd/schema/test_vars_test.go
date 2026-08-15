package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestSchemaTestCommand_HCLVariable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaFile := writeSchemaTestHCLVariableFixture(c, dir)
	testsDir := writeSchemaTestHCLVariableCase(c, dir, "native")

	out, err := runSchemaTestCommand(
		"--dir", testsDir,
		"--root-dir", schemaFile,
		"--var", "tenant=native",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "HCL variable reaches desired schema"`)
}

func writeSchemaTestHCLVariableFixture(c *qt.C, dir string) string {
	c.Helper()
	schemaFile := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaFile, []byte(`variable "tenant" {
  type    = string
  default = "fallback"
}

schema "main" {
}

table "users" {
  schema = schema.main
  column "tenant" {
    type    = text
    default = var.tenant
  }
}
`), 0o600), qt.IsNil)
	return schemaFile
}

func writeSchemaTestHCLVariableCase(c *qt.C, dir, expected string) string {
	c.Helper()
	testsDir := filepath.Join(dir, "tests")
	c.Assert(os.MkdirAll(testsDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "variables.yaml"), []byte(`cases:
  - name: HCL variable reaches desired schema
    steps:
      - exec: INSERT INTO users DEFAULT VALUES
      - assert:
          query: SELECT tenant FROM users
          scalar: `+expected+`
`), 0o600), qt.IsNil)
	return testsDir
}
