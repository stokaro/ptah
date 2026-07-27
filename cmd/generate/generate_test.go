package generate_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
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

func captureGenerateStdout(c *qt.C, run func() error) (string, error) {
	c.Helper()

	oldStdout := os.Stdout
	outR, outW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		os.Stdout = oldStdout
		c.Assert(outR.Close(), qt.IsNil)
	})

	os.Stdout = outW
	runErr := run()
	c.Assert(outW.Close(), qt.IsNil)
	os.Stdout = oldStdout

	output, err := io.ReadAll(outR)
	c.Assert(err, qt.IsNil)
	return string(output), runErr
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

	output, err := captureGenerateStdout(c, cmd.Execute)

	c.Assert(err, qt.IsNil)
	c.Assert(output, qt.Contains, "Found 1 tables")
	c.Assert(output, qt.Contains, `CREATE TABLE "configured_widgets"`)
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
	cmd := exec.Command("go", "run", "../main.go", "schema", "render", "--root-dir", fixtureDir, "--dialect", "postgres")
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("generate output:\n%s", output))

	sql := string(output)
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
	cmd := exec.Command("go", "run", "../main.go", "schema", "render", "--root-dir", fixtureDir, "--dialect", "postgres")
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("generate output:\n%s", output))

	usersCreate := outputStatementContaining(string(output), `CREATE TABLE "users"`)
	c.Assert(usersCreate, qt.Not(qt.Equals), "")
	c.Assert(strings.Count(usersCreate, `"metadata" JSONB NOT NULL`), qt.Equals, 1)
}
