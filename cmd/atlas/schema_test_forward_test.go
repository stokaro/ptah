package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

// writeSchemaTestFixture fills modelsDir with an annotated Go entity and
// testsDir with a passing Ptah-native YAML test case.
func writeSchemaTestFixture(c *qt.C, modelsDir, testsDir string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users schema works\n"+
			"    steps:\n"+
			"      - name: insert\n"+
			"        exec: INSERT INTO users (id, name) VALUES (1, 'ada')\n"+
			"      - name: the user is named ada\n"+
			"        assert:\n"+
			"          query: SELECT name FROM users\n"+
			"          scalar: ada\n"), 0o600), qt.IsNil)
}

func TestCompatCommand_SchemaTestForwardsToNative(t *testing.T) {
	c := qt.New(t)
	modelsDir, testsDir := t.TempDir(), t.TempDir()
	writeSchemaTestFixture(c, modelsDir, testsDir)
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "test", testsDir,
		"-u", "file://" + modelsDir,
		"--dev-url", devDB,
	})

	err := cmd.Execute()

	// The Atlas verb forwards to `ptah schema test`: -u/--url maps to the
	// native Go-annotation --root-dir, --dev-url to the native throwaway
	// --db-url, and the positional path to the native test-case --dir.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `PASS  case "users schema works"`)
	c.Assert(out.String(), qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestCompatCommand_SchemaTestFailingCaseExits1(t *testing.T) {
	c := qt.New(t)
	modelsDir, testsDir := t.TempDir(), t.TempDir()
	writeSchemaTestFixture(c, modelsDir, testsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "fail.yaml"), []byte(
		"cases:\n"+
			"  - name: failing expectation\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"42\"\n"), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", testsDir, "--url", "file://" + modelsDir})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "schema tests failed")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out.String(), qt.Contains, `FAIL  case "failing expectation"`)
}

func TestCompatCommand_SchemaTestRunFilterSelectsCases(t *testing.T) {
	c := qt.New(t)
	modelsDir, testsDir := t.TempDir(), t.TempDir()
	writeSchemaTestFixture(c, modelsDir, testsDir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "test", testsDir,
		"--url", "file://" + modelsDir,
		"--run", "^does-not-match$",
	})

	err := cmd.Execute()

	// The Atlas --run filter forwards to the native --run pattern; a pattern
	// with no matches fails loudly instead of reporting an empty pass.
	c.Assert(err, qt.ErrorMatches, `no test cases match --run "\^does-not-match\$"`)
}

func TestCompatCommand_SchemaTestRejectsRemoteSchemaURL(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--url", "postgres://localhost/db"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas schema test --url: only local file:// migration directories are supported`)
}

func TestCompatCommand_SchemaTestUsesAtlasProjectConfig(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	modelsDir := filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	testsDir := t.TempDir()
	writeSchemaTestFixture(c, modelsDir, testsDir)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  src = "file://models"
  dev = "sqlite://`+filepath.ToSlash(filepath.Join(dir, "dev.db"))+`"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", testsDir, "--env", "local"})

	err := cmd.Execute()

	// env schema.src supplies the desired schema URL and env dev the throwaway
	// database; env url (the target database URL) is deliberately never
	// injected into the desired schema flag.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `PASS  case "users schema works"`)
}

func TestCompatCommand_SchemaTestRejectsMultipleProjectSchemaSources(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  src = ["file://a", "file://b"]
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas schema test supports one atlas.hcl schema source, got 2`)
}

func TestNewCompatCommand_SchemaTestResolvesAtRoot(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--help"})

	err := cmd.Execute()

	// The verb resolves as a working forward through the compat binary too; it
	// no longer prints the Atlas CE community-version unsupported boundary.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "atlas schema test [flags] [paths]")
	c.Assert(out.String(), qt.Contains, "-u, --url")
	c.Assert(out.String(), qt.Not(qt.Contains), "not supported by the community version")
}
