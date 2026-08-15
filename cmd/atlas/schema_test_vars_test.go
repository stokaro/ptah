package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

func TestCompatCommand_SchemaTestForwardsExplicitHCLVariable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaFile := writeCompatSchemaTestHCLVariableFixture(c, dir)
	testsDir := writeCompatSchemaTestHCLVariableCase(c, dir, "explicit")

	out, err := runCompatSchemaTestWithArgs(
		"schema", "test", testsDir,
		"--url", "file://"+schemaFile,
		"--var", "tenant=explicit",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "HCL variable reaches desired schema"`)
}

func TestCompatCommand_SchemaTestExplicitSourceKeepsRunVariable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	schemaFile := writeCompatSchemaTestHCLVariableFixture(c, dir)
	testsDir := writeCompatSchemaTestHCLVariableCase(c, dir, "explicit")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`data "hcl_schema" "app" {
  paths = ["schema.hcl"]
  vars = {
    tenant = "project-scope"
  }
}

env "local" {
  src = data.hcl_schema.app.url
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)

	out, err := runCompatSchemaTestWithArgs(
		"schema", "test", testsDir,
		"--env", "local",
		"--url", "file://"+schemaFile,
		"--var", "tenant=explicit",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "HCL variable reaches desired schema"`)
}

func TestCompatCommand_SchemaTestUsesDataSourceHCLVariables(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	writeCompatSchemaTestHCLVariableFixture(c, dir)
	testsDir := writeCompatSchemaTestHCLVariableCase(c, dir, "scoped,inc")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`data "hcl_schema" "app" {
  paths = ["schema.hcl"]
  vars = {
    tenant = "scoped,inc"
  }
}

env "local" {
  src = data.hcl_schema.app.url
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)

	out, err := runCompatSchemaTestWithArgs(
		"schema", "test", testsDir,
		"--env", "local",
		"--var", "tenant=must-not-leak",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "HCL variable reaches desired schema"`)
}

func TestCompatCommand_SchemaTestEmptyDataSourceScopeRejectsRunVariable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	writeCompatSchemaTestHCLVariableFixture(c, dir)
	testsDir := writeCompatSchemaTestHCLVariableCase(c, dir, "fallback")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`data "hcl_schema" "app" {
  paths = ["schema.hcl"]
}

env "local" {
  src = data.hcl_schema.app.url
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)

	out, err := runCompatSchemaTestWithArgs(
		"schema", "test", testsDir,
		"--env", "local",
		"--var", "tenant=must-not-leak",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "HCL variable reaches desired schema"`)
}

func TestCompatCommand_SchemaTestLiteralProjectSourceUsesRunVariable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	writeCompatSchemaTestHCLVariableFixture(c, dir)
	testsDir := writeCompatSchemaTestHCLVariableCase(c, dir, "literal")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  src = "file://schema.hcl"
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)

	out, err := runCompatSchemaTestWithArgs(
		"schema", "test", testsDir,
		"--env", "local",
		"--var", "tenant=literal",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "HCL variable reaches desired schema"`)
}

func writeCompatSchemaTestHCLVariableFixture(c *qt.C, dir string) string {
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

func writeCompatSchemaTestHCLVariableCase(c *qt.C, dir, expected string) string {
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

func runCompatSchemaTestWithArgs(args ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}
