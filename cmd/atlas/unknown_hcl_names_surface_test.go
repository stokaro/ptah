package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/root"
)

// unknownNameSchemaHCL carries a top-level block and a column attribute that
// Ptah's schema-HCL parser does not model. The community binary reads this file
// to completion and emits DDL identical to the same schema without them.
const unknownNameSchemaHCL = `
annotation "gql" {
  attr "name" {
    type = string
  }
}
schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type      = int
    invisible = true
  }
}
`

// TestSchemaInspectUnknownHCLNamesSplitByCommandTree pins the split
// stokaro/ptah#1016 left to this change: the SAME file is accepted by the
// Atlas-compatible command tree and refused by Ptah's own.
//
// One fixture, two command trees, opposite verdicts -- which is the only shape
// that can catch the tolerance leaking from one tree into the other. It leaked
// in exactly that direction once already: the option was set unconditionally
// inside the shared resolver, so `ptah schema inspect` silently dropped typos
// while the documentation said it named them.
func TestSchemaInspectUnknownHCLNamesSplitByCommandTree(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte(unknownNameSchemaHCL), 0o600), qt.IsNil)

	compat := atlas.NewCompatCommand("atlas")
	var compatOut bytes.Buffer
	compat.SetOut(&compatOut)
	compat.SetErr(&compatOut)
	compat.SetArgs([]string{
		"schema", "inspect",
		"--url", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "compat.db"),
	})
	compatErr := compat.Execute()

	native := root.NewRootCommand()
	var nativeOut bytes.Buffer
	native.SetOut(&nativeOut)
	native.SetErr(&nativeOut)
	native.SetArgs([]string{
		"schema", "inspect",
		"--schema-file", schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "native.db"),
	})
	nativeErr := native.Execute()

	c.Assert(compatErr, qt.IsNil)
	c.Assert(compatOut.String(), qt.Contains, `table "t"`)
	c.Assert(compatOut.String(), qt.Contains, `column "id"`)
	c.Assert(nativeErr, qt.ErrorMatches, `.*unsupported top-level block "annotation".*`)
}

// TestSchemaDiffUnknownHCLNamesSplitByCommandTree is the same split on the diff
// command, which reaches the loader through a different resolver call than
// inspect does. Both had the tolerance hard-wired on; a test on one of them
// would not have caught the other.
func TestSchemaDiffUnknownHCLNamesSplitByCommandTree(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte(unknownNameSchemaHCL), 0o600), qt.IsNil)
	emptyPath := filepath.Join(dir, "empty.hcl")
	c.Assert(os.WriteFile(emptyPath, []byte("schema \"main\" {\n}\n"), 0o600), qt.IsNil)

	compat := atlas.NewCompatCommand("atlas")
	var compatOut bytes.Buffer
	compat.SetOut(&compatOut)
	compat.SetErr(&compatOut)
	compat.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + emptyPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "compat.db"),
	})
	compatErr := compat.Execute()

	native := root.NewRootCommand()
	var nativeOut bytes.Buffer
	native.SetOut(&nativeOut)
	native.SetErr(&nativeOut)
	native.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + emptyPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "native.db"),
	})
	nativeErr := native.Execute()

	c.Assert(compatErr, qt.IsNil)
	c.Assert(compatOut.String(), qt.Contains, `"t"`)
	c.Assert(nativeErr, qt.ErrorMatches, `.*unsupported top-level block "annotation".*`)
}
