package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
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

	// The verb resolves as a working forward through the compatibility binary.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "atlas schema test [flags] [paths]")
	c.Assert(out.String(), qt.Contains, "-u, --url")
}

// writeDistinctSourceFixture writes three desired-schema sources that each
// declare a DIFFERENT table, plus a test case asserting the SQL-sourced one.
//
// The distinct table names are the point. With every source declaring the same
// table, a run passes whether or not the source was read at all, so it would
// not show that -u resolved anything.
func writeDistinctSourceFixture(c *qt.C, dir, testsDir string) (sqlFile, hclFile, modelsDir string) {
	c.Helper()
	modelsDir = filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(`package models

//ptah:schema:table name="users_from_go"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	sqlFile = filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(sqlFile,
		[]byte("CREATE TABLE orders_from_sql (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	hclFile = filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(hclFile, []byte(`schema "main" {
}
table "widgets_from_hcl" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)

	c.Assert(os.WriteFile(filepath.Join(testsDir, "sql.yaml"), []byte(`cases:
  - name: sql-sourced table exists
    steps:
      - assert:
          query: "SELECT count(*) FROM orders_from_sql"
          row_count: 1
`), 0o600), qt.IsNil)
	return sqlFile, hclFile, modelsDir
}

func runCompatSchemaTest(c *qt.C, testsDir, source string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "test", testsDir,
		"-u", "file://" + source,
		"--dev-url", "sqlite://" + filepath.Join(c.TempDir(), "dev.db"),
	})
	return func() (string, error) { err := cmd.Execute(); return out.String(), err }()
}

// TestCompatCommand_SchemaTestAcceptsSQLFileSource covers a .sql desired schema.
// The Go-annotation directory is the control: it declares a different table, so
// the same case fails against it. Without that half, a pass would not
// distinguish "read the SQL file" from "read anything at all".
func TestCompatCommand_SchemaTestAcceptsSQLFileSource(t *testing.T) {
	c := qt.New(t)
	dir, testsDir := t.TempDir(), t.TempDir()
	sqlFile, _, modelsDir := writeDistinctSourceFixture(c, dir, testsDir)

	out, err := runCompatSchemaTest(c, testsDir, sqlFile)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")

	controlOut, controlErr := runCompatSchemaTest(c, testsDir, modelsDir)
	c.Assert(controlErr, qt.IsNotNil)
	c.Assert(controlOut, qt.Contains, "no such table: orders_from_sql")
}

// TestCompatCommand_SchemaTestAcceptsHCLFileSource covers a .hcl desired schema.
func TestCompatCommand_SchemaTestAcceptsHCLFileSource(t *testing.T) {
	c := qt.New(t)
	dir, testsDir := t.TempDir(), t.TempDir()
	_, hclFile, _ := writeDistinctSourceFixture(c, dir, testsDir)
	c.Assert(os.Remove(filepath.Join(testsDir, "sql.yaml")), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "hcl.yaml"), []byte(`cases:
  - name: hcl-sourced table exists
    steps:
      - assert:
          query: "SELECT count(*) FROM widgets_from_hcl"
          row_count: 1
`), 0o600), qt.IsNil)

	out, err := runCompatSchemaTest(c, testsDir, hclFile)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}
