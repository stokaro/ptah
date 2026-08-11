package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const malformedSchemaHCLProcessDiagnostic = ":1,15-16: Unclosed configuration block; " +
	"There is no closing brace for this block before the end of the file. " +
	"This may be caused by incorrect brace nesting elsewhere in this file."

func TestCompatBinarySchemaApplyMalformedHCLMatchesAtlasErrorShape(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	dir := c.TempDir()
	schemaPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)
	run := newCompatProcess(binPath,
		"schema", "apply",
		"--url", "sqlite://"+filepath.Join(dir, "target.db"),
		"--to", "file://"+schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--dry-run",
	)
	run.Dir = dir
	run.Env = environmentWithoutPtahVariables()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()
	var exitErr *exec.ExitError
	want := canonicalProcessSchemaPath(c, schemaPath) + malformedSchemaHCLProcessDiagnostic

	c.Assert(err, qt.ErrorAs, &exitErr, qt.Commentf("stderr: %s", stderr.String()))
	c.Assert(exitErr.ExitCode(), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals, "Error: "+want+"\n")
}

func TestNativeBinarySchemaApplyMalformedHCLKeepsNativeContext(t *testing.T) {
	c := qt.New(t)
	binPath := buildNativeBinary(c)
	dir := c.TempDir()
	schemaPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)
	run := exec.Command(binPath,
		"schema", "apply",
		"--db-url", "sqlite://"+filepath.Join(dir, "target.db"),
		"--to", "file://"+schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--dry-run",
	)
	run.Dir = dir
	run.Env = environmentWithoutPtahVariables()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()
	var exitErr *exec.ExitError
	want := "load --to schema: parse HCL schema: " + canonicalProcessSchemaPath(c, schemaPath) +
		malformedSchemaHCLProcessDiagnostic

	c.Assert(err, qt.ErrorAs, &exitErr, qt.Commentf("stderr: %s", stderr.String()))
	c.Assert(exitErr.ExitCode(), qt.Equals, 2)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals, "error: "+want+"\n")
}

func buildNativeBinary(c *qt.C) string {
	c.Helper()
	binPath := filepath.Join(c.TempDir(), "ptah")
	build := exec.Command("go", "build", "-o", binPath, "../ptah")
	build.Env = append(environmentWithoutPtahVariables(), "GOWORK=off")
	buildOut, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", buildOut))
	return binPath
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

func canonicalProcessSchemaPath(c *qt.C, path string) string {
	c.Helper()
	dir, err := filepath.EvalSymlinks(filepath.Dir(path))
	c.Assert(err, qt.IsNil)
	return filepath.Join(dir, filepath.Base(path))
}
