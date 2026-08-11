//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

const malformedSchemaHCLProcessDiagnostic = ":1,15-16: Unclosed configuration block; " +
	"There is no closing brace for this block before the end of the file. " +
	"This may be caused by incorrect brace nesting elsewhere in this file."

func TestSchemaApplyHCLDiagnosticsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)

	repoRoot := e2eRepoRoot(t)
	compatBinary := filepath.Join(t.TempDir(), "ptah-compat")
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtahCompat(c, ctx, repoRoot, compatBinary)
	buildPtah(c, ctx, repoRoot, nativeBinary)

	c.Run("compat malformed HCL matches Atlas error shape", func(c *qt.C) {
		dir := c.TempDir()
		schemaPath := filepath.Join(dir, "schema.hcl")
		c.Assert(os.WriteFile(schemaPath, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runSchemaApplyProcess(ctx, dir, compatBinary,
			"schema", "apply",
			"--url", "sqlite://"+filepath.Join(dir, "target.db"),
			"--to", "file://"+schemaPath,
			"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
			"--dry-run",
		)
		want := canonicalSchemaApplyProcessPath(c, schemaPath) + malformedSchemaHCLProcessDiagnostic

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: "+want+"\n")
	})

	c.Run("compat unrelated loader error keeps context", func(c *qt.C) {
		dir := c.TempDir()
		schemaPath := filepath.Join(dir, "missing.hcl")

		stdout, stderr, err := runSchemaApplyProcess(ctx, dir, compatBinary,
			"schema", "apply",
			"--url", "sqlite://"+filepath.Join(dir, "target.db"),
			"--to", "file://"+schemaPath,
			"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
			"--dry-run",
		)
		want := "load --to schema: schema file does not exist: " + canonicalSchemaApplyProcessPath(c, schemaPath)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: "+want+"\n")
	})

	c.Run("native malformed HCL keeps native context", func(c *qt.C) {
		dir := c.TempDir()
		schemaPath := filepath.Join(dir, "schema.hcl")
		c.Assert(os.WriteFile(schemaPath, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runSchemaApplyProcess(ctx, dir, nativeBinary,
			"schema", "apply",
			"--db-url", "sqlite://"+filepath.Join(dir, "target.db"),
			"--to", "file://"+schemaPath,
			"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
			"--dry-run",
		)
		want := "load --to schema: parse HCL schema: " + canonicalSchemaApplyProcessPath(c, schemaPath) +
			malformedSchemaHCLProcessDiagnostic

		c.Assert(exitStatusOf(c, err), qt.Equals, 2)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "error: "+want+"\n")
	})
}

func runSchemaApplyProcess(
	ctx context.Context,
	dir string,
	binaryPath string,
	args ...string,
) (stdoutText, stderrText string, err error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = dir
	cmd.Env = environmentWithoutPtahVariables()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	return stdout.String(), stderr.String(), err
}

func environmentWithoutPtahVariables() []string {
	environment := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "PTAH_") {
			environment = append(environment, entry)
		}
	}
	return environment
}

func canonicalSchemaApplyProcessPath(c *qt.C, path string) string {
	c.Helper()
	dir, err := filepath.EvalSymlinks(filepath.Dir(path))
	c.Assert(err, qt.IsNil)
	return filepath.Join(dir, filepath.Base(path))
}
