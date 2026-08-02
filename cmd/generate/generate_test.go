package generate_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/generate"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func runGenerateHelperProcess() {
	if os.Getenv("GO_WANT_GENERATE_HELPER_PROCESS") != "1" {
		return
	}
	fmt.Fprint(os.Stdout, "tables:\n  configured_widgets:\n    columns:\n      id: {type: INTEGER, primary: true}\n")
	os.Exit(0)
}

// TestGenerateHelperProcess is not a real test; the config integration test
// re-executes this binary as an external schema loader.
func TestGenerateHelperProcess(t *testing.T) {
	runGenerateHelperProcess()
}

func executeGenerate(c *qt.C, cmd interface {
	SetOut(io.Writer)
	SetErr(io.Writer)
	Execute() error
}) (stdoutText, stderrText string, executeErr error) {
	c.Helper()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	err := cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func outputStatementContaining(output, marker string) string {
	for statement := range strings.SplitSeq(output, "-- Statement ") {
		if strings.Contains(statement, marker) {
			return statement
		}
	}
	return ""
}

func TestGenerateCommand_UsesExternalSchemaFromPtahConfigEnv(t *testing.T) {
	c := qt.New(t)

	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	config := fmt.Sprintf(`env:
  verified:
    external_schema:
      program: [%s, "-test.run=TestGenerateHelperProcess"]
      format: yaml
      env: ["GO_WANT_GENERATE_HELPER_PROCESS=1"]
`, strconv.Quote(os.Args[0]))
	// configPath is rooted in t.TempDir and is not influenced by production input.
	c.Assert(os.WriteFile(configPath, []byte(config), 0o600), qt.IsNil) //nolint:gosec // controlled test-only path

	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{
		"--config", configPath,
		"--env", "verified",
		"--allow-external-schema",
		"--dialect", "postgres",
	})

	stdout, stderr, err := executeGenerate(c, cmd)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, `CREATE TABLE "configured_widgets"`)
	c.Assert(stdout, qt.Not(qt.Contains), "Found 1 tables")
	c.Assert(stderr, qt.Contains, "Found 1 tables")
}

func TestGenerateCommand_RejectsImplicitExternalSchemaFromConfig(t *testing.T) {
	c := qt.New(t)

	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	config := fmt.Sprintf(`external_schema:
  program: [%s, "-test.run=TestGenerateHelperProcess"]
  format: yaml
  env: ["GO_WANT_GENERATE_HELPER_PROCESS=1"]
`, strconv.Quote(os.Args[0]))
	c.Assert(
		os.WriteFile(configPath, []byte(config), 0o600), //nolint:gosec // controlled test-only path
		qt.IsNil,
	)

	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--config", configPath, "--dialect", "postgres"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "ptah.yaml external_schema is disabled by default; pass --allow-external-schema to execute it")
}

func TestGenerateCommandUnsupportedDialectExits2WithoutPanicTrace(t *testing.T) {
	c := qt.New(t)

	cmd := generate.NewGenerateCommand()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"--root-dir", filepath.Join("..", "..", "stubs"), "--dialect", "oracle"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(errOut.String(), qt.Contains, "error: error rendering oracle schema: unsupported database dialect: oracle")
	c.Assert(errOut.String(), qt.Not(qt.Contains), "panic:")
	c.Assert(errOut.String(), qt.Not(qt.Contains), "goroutine")
	c.Assert(errOut.String(), qt.Not(qt.Contains), "Usage:")
}

func TestGenerateCommand_MutualForeignKeysAreTwoPhase(t *testing.T) {
	c := qt.New(t)

	fixtureDir := filepath.Join("..", "..", "integration", "fixtures", "entities", "029-roundtrip-mutual-cycle")
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--root-dir", fixtureDir, "--dialect", "postgres"})
	stdout, stderr, err := executeGenerate(c, cmd)
	c.Assert(err, qt.IsNil, qt.Commentf("generate stderr:\n%s", stderr))

	sql := stdout
	leftCreate := outputStatementContaining(sql, `CREATE TABLE "left_nodes"`)
	rightCreate := outputStatementContaining(sql, `CREATE TABLE "right_nodes"`)
	c.Assert(leftCreate, qt.Not(qt.Equals), "")
	c.Assert(rightCreate, qt.Not(qt.Equals), "")
	c.Assert(leftCreate, qt.Not(qt.Contains), "FOREIGN KEY")
	c.Assert(rightCreate, qt.Not(qt.Contains), "FOREIGN KEY")
	c.Assert(sql, qt.Contains, `ALTER TABLE "left_nodes" ADD CONSTRAINT "fk_left_nodes_right_id" FOREIGN KEY ("right_id") REFERENCES "right_nodes"("id")`)
	c.Assert(sql, qt.Contains, `ALTER TABLE "right_nodes" ADD CONSTRAINT "fk_right_nodes_left_id" FOREIGN KEY ("left_id") REFERENCES "left_nodes"("id")`)
}

func TestGenerateCommand_JsonEmbeddedFieldRendersOnce(t *testing.T) {
	c := qt.New(t)

	fixtureDir := filepath.Join("..", "..", "integration", "fixtures", "entities", "023-go-annotations-objects")
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--root-dir", fixtureDir, "--dialect", "postgres"})
	stdout, stderr, err := executeGenerate(c, cmd)
	c.Assert(err, qt.IsNil, qt.Commentf("generate stderr:\n%s", stderr))

	usersCreate := outputStatementContaining(stdout, `CREATE TABLE "users"`)
	c.Assert(usersCreate, qt.Not(qt.Equals), "")
	c.Assert(strings.Count(usersCreate, `"metadata" JSONB NOT NULL`), qt.Equals, 1)
}

func TestGenerateCommand_StdoutIsExecutableSQL(t *testing.T) {
	c := qt.New(t)

	schemaPath := filepath.Join(t.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(schemaPath, []byte(`enums:
  account_status: [active, suspended]
tables:
  accounts:
    columns:
      id: {type: INTEGER, primary: true}
      status: {type: account_status, not_null: true}
`), 0o600), qt.IsNil)

	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--schema-file", schemaPath, "--dialect", "postgres"})
	stdout, stderr, err := executeGenerate(c, cmd)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(stdout, `CREATE TYPE "account_status"`), qt.Equals, 1)
	c.Assert(stdout, qt.Contains, `CREATE TABLE "accounts"`)
	c.Assert(stdout, qt.Not(qt.Contains), "Found 1 tables")
	c.Assert(stdout, qt.Not(qt.Contains), "Table Dependencies:")
	c.Assert(stdout, qt.Not(qt.Contains), "=== POSTGRES SCHEMA ===")
	c.Assert(stderr, qt.Contains, "Found 1 tables")
	c.Assert(stderr, qt.Contains, "Table Dependencies:")
}

func TestGenerateCommand_OmittedDialectForeignKeyFailureKeepsStdoutEmpty(t *testing.T) {
	c := qt.New(t)

	fixtureDir := filepath.Join("..", "..", "integration", "fixtures", "entities", "029-roundtrip-mutual-cycle")
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--root-dir", fixtureDir})
	stdout, stderr, err := executeGenerate(c, cmd)

	c.Assert(err, qt.ErrorMatches, "error rendering clickhouse schema: clickhouse does not support foreign keys")
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, "Found 2 tables")
}
