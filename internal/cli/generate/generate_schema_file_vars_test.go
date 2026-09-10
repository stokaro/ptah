package generate_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/generate"
)

// schemaFileWithVariable declares a variable with no default and uses it, so
// the document is unusable until a value is supplied.
const schemaFileWithVariable = `
variable "prefix" {
  type = string
}

schema "public" {
}

table "orders" {
  schema  = schema.public
  comment = "${var.prefix} orders"
  column "id" {
    type = int
  }
}
`

// writeVariableSchemaFile writes the document above and returns its path.
func writeVariableSchemaFile(c *qt.C) string {
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(schemaFileWithVariable), 0o600), qt.IsNil)
	return path
}

// TestGenerateCommand_SuppliesASchemaFileVariable pins that `--var` reaches the
// schema file, not only an atlas.hcl.
//
// A schema file declaring a variable without a default was unusable through
// every native command: the flag was registered, the diagnostic named it, and
// the value went nowhere. The same flag against the same file worked on the
// compatibility surface, which put a capability behind the adapter
// (stokaro/ptah#3113).
func TestGenerateCommand_SuppliesASchemaFileVariable(t *testing.T) {
	c := qt.New(t)
	path := writeVariableSchemaFile(c)
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--schema-file", path, "--dialect", "postgres", "--var", "prefix=shop"})

	stdout, _, err := executeGenerate(c, cmd)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "'shop orders'")
}

// TestGenerateCommand_StillRefusesAVariableNobodySupplied is the control.
//
// The refusal is the diagnostic that sends a reader to the flag, so supplying
// the value must not have been bought by defaulting the variable to nothing.
func TestGenerateCommand_StillRefusesAVariableNobodySupplied(t *testing.T) {
	c := qt.New(t)
	path := writeVariableSchemaFile(c)
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--schema-file", path, "--dialect", "postgres"})

	_, _, err := executeGenerate(c, cmd)

	c.Assert(err, qt.ErrorMatches, `(?s).*missing value for required variable "prefix".*`)
}
