package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// schemaFileVariableHCL declares a variable with no default and reads it from a
// COLUMN DEFAULT.
//
// The placement is load-bearing. Put the reference in a table comment instead
// and the fixture proves nothing: Ptah drops table comments on SQLite whether
// or not the reference resolved, so the control and the variant agree. A column
// default is a position Ptah does render, which is what makes the byte
// assertions below discriminate.
const schemaFileVariableHCL = `variable "status" {
  type = string
}
schema "main" {
}
table "t" {
  schema = schema.main
  column "state" {
    type    = text
    default = var.status
  }
}
`

const schemaFileEmptyHCL = "schema \"main\" {\n}\n"

// writeSchemaFileVariableFixture writes the fixture pair into a directory that
// deliberately holds NO atlas.hcl, and makes it the working directory. The
// absence is the point: the pinned Atlas community binary v1.3.0 accepts --var
// there, and Ptah used to demand a project file that binary never reads.
func writeSchemaFileVariableFixture(t *testing.T) (from, to string) {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := filepath.Join(dir, "empty.hcl")
	toPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(fromPath, []byte(schemaFileEmptyHCL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(schemaFileVariableHCL), 0o600), qt.IsNil)
	t.Chdir(dir)
	return "file://" + fromPath, "file://" + toPath
}

// TestSchemaDiffVarReachesTheSchemaFile is the acceptance test for the --var
// half of issue #926.
//
// Measured on the pinned binary in a directory with no atlas.hcl:
//
//	schema diff --from file://empty.hcl --to file://schema.hcl --var status=live
//	  -> exit 0, CREATE TABLE `t` (`state` text NOT NULL DEFAULT 'live');
//
// Ptah answered `failed to read atlas config atlas.hcl: openat atlas.hcl: no
// such file or directory` at exit 1, because --var reached only
// config/projectconfig and made the project file required on the way.
//
// Reverted, this test fails on the nil-error assertion and prints that
// atlas-config message.
func TestSchemaDiffVarReachesTheSchemaFile(t *testing.T) {
	c := qt.New(t)
	from, to := writeSchemaFileVariableFixture(t)

	out, err := runCompatCommand(t,
		"schema", "diff",
		"--dev-url", "sqlite://file?mode=memory",
		"--from", from,
		"--to", to,
		"--var", "status=live",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "DEFAULT 'live'")
}

// TestSchemaDiffWithoutVarNamesTheMissingVariable is the other half: with no
// --var the same file is refused, byte-identically to the pinned binary's
// `missing value for required variable "status"`.
//
// Reverted, this test fails on the non-nil-error assertion, and the command
// instead succeeds printing the DDL literal DEFAULT 'var.status'.
func TestSchemaDiffWithoutVarNamesTheMissingVariable(t *testing.T) {
	c := qt.New(t)
	from, to := writeSchemaFileVariableFixture(t)

	_, err := runCompatCommand(t,
		"schema", "diff",
		"--dev-url", "sqlite://file?mode=memory",
		"--from", from,
		"--to", to,
	)

	c.Assert(err, qt.ErrorMatches, `.*missing value for required variable "status".*`)
}
