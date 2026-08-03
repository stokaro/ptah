package dbtest_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

// The Atlas fixture shape, reproduced exactly as Atlas writes it.
const atlasSchemaTestHCL = `test "schema" "users_insert_select" {
  exec {
    sql = "INSERT INTO users (id, name) VALUES (1, 'ada')"
  }
  exec {
    sql    = "SELECT name FROM users WHERE id = 1"
    output = "ada"
  }
}
`

const atlasMigrateTestHCL = `test "migrate" "users_table_exists" {
  migrate {
    to = "20240101000000"
  }
  exec {
    sql    = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'"
    output = "1"
  }
}
`

func TestParseAtlasTestCasesTranslatesExecAndOutput(t *testing.T) {
	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(atlasSchemaTestHCL), "schema.test.hcl", dbtest.AtlasTestKindSchema)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Name, qt.Equals, "users_insert_select")
	c.Assert(cases[0].Steps, qt.HasLen, 2)

	// An `exec` without `output` is a bare statement; with `output` it is an
	// assertion. Getting this backwards would make the second step run and
	// check nothing, so both halves are pinned.
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "INSERT INTO users (id, name) VALUES (1, 'ada')")
	c.Assert(cases[0].Steps[0].Assert, qt.IsNil)

	c.Assert(cases[0].Steps[1].Exec, qt.Equals, "")
	c.Assert(cases[0].Steps[1].Assert, qt.IsNotNil)
	c.Assert(cases[0].Steps[1].Assert.Query, qt.Equals, "SELECT name FROM users WHERE id = 1")
	c.Assert(cases[0].Steps[1].Assert.Scalar, qt.IsNotNil)
	c.Assert(*cases[0].Steps[1].Assert.Scalar, qt.Equals, "ada")
}

// TestParseAtlasTestCasesPreservesStepOrder is the discriminator for the
// decision to walk Body.Blocks rather than decode against an hcl schema: a
// schema-driven decode groups blocks by type, which would move the `migrate`
// step after both `exec` steps and silently change what the case does.
func TestParseAtlasTestCasesPreservesStepOrder(t *testing.T) {
	c := qt.New(t)

	const interleaved = `test "migrate" "ordered" {
  exec {
    sql = "SELECT 1"
  }
  migrate {
    to = "20240101000000"
  }
  exec {
    sql = "SELECT 2"
  }
}
`
	cases, err := dbtest.ParseAtlasTestCases([]byte(interleaved), "m.test.hcl", dbtest.AtlasTestKindMigrate)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Steps, qt.HasLen, 3)
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT 1")
	c.Assert(cases[0].Steps[1].MigrateTo, qt.Equals, "20240101000000")
	c.Assert(cases[0].Steps[2].Exec, qt.Equals, "SELECT 2")
}

// TestParseAtlasTestCasesSelectsByKind pins that the two kinds are separated.
// A migrate case loaded into a schema run would drive the migration directory,
// which the caller did not ask for.
func TestParseAtlasTestCasesSelectsByKind(t *testing.T) {
	c := qt.New(t)

	both := atlasSchemaTestHCL + "\n" + atlasMigrateTestHCL

	schemaCases, err := dbtest.ParseAtlasTestCases([]byte(both), "both.test.hcl", dbtest.AtlasTestKindSchema)
	c.Assert(err, qt.IsNil)
	c.Assert(schemaCases, qt.HasLen, 1)
	c.Assert(schemaCases[0].Name, qt.Equals, "users_insert_select")

	migrateCases, err := dbtest.ParseAtlasTestCases([]byte(both), "both.test.hcl", dbtest.AtlasTestKindMigrate)
	c.Assert(err, qt.IsNil)
	c.Assert(migrateCases, qt.HasLen, 1)
	c.Assert(migrateCases[0].Name, qt.Equals, "users_table_exists")
}

func TestParseAtlasTestCasesRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "unknown attribute on exec",
			input:    "test \"schema\" \"x\" {\n  exec {\n    sql = \"SELECT 1\"\n    outpu = \"1\"\n  }\n}\n",
			contains: "does not take",
		},
		{
			name:     "unknown step block",
			input:    "test \"schema\" \"x\" {\n  frobnicate {\n    sql = \"SELECT 1\"\n  }\n}\n",
			contains: "unsupported step",
		},
		{
			name:     "exec without sql",
			input:    "test \"schema\" \"x\" {\n  exec {\n    output = \"1\"\n  }\n}\n",
			contains: "requires sql",
		},
		{
			name:     "unknown test kind",
			input:    "test \"frobnicate\" \"x\" {\n  exec {\n    sql = \"SELECT 1\"\n  }\n}\n",
			contains: "unsupported test kind",
		},
		{
			name:     "wrong label count",
			input:    "test \"schema\" {\n  exec {\n    sql = \"SELECT 1\"\n  }\n}\n",
			contains: "exactly two labels",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := dbtest.ParseAtlasTestCases([]byte(tt.input), "bad.test.hcl", dbtest.AtlasTestKindSchema)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, tt.contains)
		})
	}
}

// TestLoadCasesOfKindReadsAtlasFilesAndLeavesSchemaHCLAlone is the file-suffix
// discriminator. A directory holding both a `.test.hcl` and a plain `.hcl`
// schema file must load the first and ignore the second -- matching on
// filepath.Ext, which yields ".hcl" for both, would try to parse the schema as
// a test document and fail the whole run.
func TestLoadCasesOfKindReadsAtlasFilesAndLeavesSchemaHCLAlone(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.test.hcl"), []byte(atlasSchemaTestHCL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.hcl"),
		[]byte("table \"users\" {\n  schema = schema.main\n}\n"), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindSchema)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Name, qt.Equals, "users_insert_select")
}

// TestLoadCasesOfKindStillReadsNativeYAML pins that adding the Atlas reader did
// not displace the native format; both live in one directory.
func TestLoadCasesOfKindStillReadsNativeYAML(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "native.yaml"),
		[]byte("cases:\n  - name: native_case\n    steps:\n      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.test.hcl"), []byte(atlasSchemaTestHCL), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindSchema)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)

	names := []string{cases[0].Name, cases[1].Name}
	c.Assert(names, qt.Contains, "native_case")
	c.Assert(names, qt.Contains, "users_insert_select")
}

// TestLoadCasesKeepsItsYAMLOnlyContract pins that the pre-existing exported
// LoadCases was not widened. It skips `.test.hcl` exactly as it did before, so
// no caller of the old function changes behavior under it.
func TestLoadCasesKeepsItsYAMLOnlyContract(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.test.hcl"), []byte(atlasSchemaTestHCL), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCases(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 0)
}

// TestLoadCasesOfKind_RejectsDuplicateAcrossFormats is the cross-format half of
// issue #1038. It is a separate code path from the YAML/YAML case: a different
// parser, reached through the other branch of the isAtlasCaseFile switch, so
// the YAML/YAML test passing does not imply this one does. Converting a YAML
// case to `.test.hcl` and leaving the original in place is exactly the mid-
// migration mistake that made this worth closing.
func TestLoadCasesOfKind_RejectsDuplicateAcrossFormats(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	c.Assert(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("cases:\n  - name: dup\n    steps:\n      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "b.test.hcl"),
		[]byte("test \"schema\" \"dup\" {\n  exec {\n    sql = \"SELECT 2\"\n  }\n}\n"), 0o600), qt.IsNil)

	_, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindSchema)
	c.Assert(err, qt.ErrorMatches, `duplicate test case "dup" in a\.yaml and b\.test\.hcl`)
}

// TestLoadCasesOfKind_KindFilterPrecedesUniqueness pins the check to the
// post-filter set. ParseAtlasTestCases drops blocks of the other kind before
// returning, so a schema-kind load never sees the `test "migrate"` case and
// must not report a collision. A fix that scanned raw names out of the files
// would pass every other test here and fail only this one.
func TestLoadCasesOfKind_KindFilterPrecedesUniqueness(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	c.Assert(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("cases:\n  - name: dup\n    steps:\n      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "b.test.hcl"),
		[]byte("test \"migrate\" \"dup\" {\n  migrate {\n    to = \"latest\"\n  }\n}\n"), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindSchema)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Name, qt.Equals, "dup")
}
