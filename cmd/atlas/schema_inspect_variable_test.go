package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// typedVariableAtlasHCL mirrors the measured b-variable-var fixture from the
// Atlas Pro trial campaign: the official Atlas binary requires the type
// attribute on variable blocks, so this exact file must parse on both
// binaries for atlas.hcl portability (issue #959).
const typedVariableAtlasHCL = `variable "schema_file" {
  type        = string
  description = "Path to the SQL schema file used as the desired state"
}

env "dev" {
  src = "file://${var.schema_file}"
  dev = "sqlite://dev?mode=memory"
}
`

// writeTypedVariableProject writes the campaign fixture pair (atlas.hcl plus
// the schema.sql it points at), makes the fixture directory the working
// directory the way the campaign invoked both binaries, and returns the
// config path. The chdir also keeps the dev database artifact inside the
// temporary directory.
func writeTypedVariableProject(t *testing.T) string {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(configPath, []byte(typedVariableAtlasHCL), 0o600), qt.IsNil)
	schemaSQL := "CREATE TABLE users (\n  id integer PRIMARY KEY,\n  email text NOT NULL UNIQUE\n);\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(schemaSQL), 0o600), qt.IsNil)
	t.Chdir(dir)
	return configPath
}

// TestSchemaInspectTypedVariableAtlasHCL is the acceptance test for issue
// #959: an atlas.hcl whose variable block carries the type attribute the
// official binary requires must inspect successfully with a --var override.
func TestSchemaInspectTypedVariableAtlasHCL(t *testing.T) {
	c := qt.New(t)
	configPath := writeTypedVariableProject(t)

	out, err := runCompatCommand(t,
		"schema", "inspect",
		"--config", "file://"+configPath,
		"--env", "dev",
		"--url", "env://src",
		"--var", "schema_file=schema.sql",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, `table "users"`)
	c.Assert(out, qt.Contains, `column "email"`)
}

// TestSchemaInspectTypedVariableMissingVar mirrors the official binary's
// "missing value for required variable" failure mode: without --var, a typed
// variable that has no default fails with Ptah's named error.
func TestSchemaInspectTypedVariableMissingVar(t *testing.T) {
	c := qt.New(t)
	configPath := writeTypedVariableProject(t)

	_, err := runCompatCommand(t,
		"schema", "inspect",
		"--config", "file://"+configPath,
		"--env", "dev",
		"--url", "env://src",
	)

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl variable "schema_file" requires a default or --var schema_file=value`)
}
