package atlas_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

const malformedSchemaHCLDiagnostic = ":1,15-16: Unclosed configuration block; " +
	"There is no closing brace for this block before the end of the file. " +
	"This may be caused by incorrect brace nesting elsewhere in this file."

func TestSchemaApplyMalformedHCLUsesAtlasErrorShape(t *testing.T) {
	c := qt.New(t)
	unsetSchemaApplyPtahEnvironment(t)
	dir := c.TempDir()
	schemaPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaApplyErrorShape(c,
		"--url", "sqlite://"+filepath.Join(dir, "target.db"),
		"--to", "file://"+schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--dry-run",
	)

	want := canonicalSchemaApplyPath(c, schemaPath) + malformedSchemaHCLDiagnostic
	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(want))
	displayErr := errors.Unwrap(err)
	c.Assert(displayErr, qt.IsNotNil)
	originalErr := errors.Unwrap(displayErr)
	c.Assert(originalErr, qt.IsNotNil)
	c.Assert(originalErr.Error(), qt.Equals, "load --to schema: parse HCL schema: "+want)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+want+"\n")
}

func TestSchemaApplyUnrelatedDesiredSchemaErrorKeepsContext(t *testing.T) {
	c := qt.New(t)
	unsetSchemaApplyPtahEnvironment(t)
	dir := c.TempDir()
	schemaPath := filepath.Join(dir, "missing.hcl")

	stdout, stderr, err := runSchemaApplyErrorShape(c,
		"--url", "sqlite://"+filepath.Join(dir, "target.db"),
		"--to", "file://"+schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--dry-run",
	)

	want := "load --to schema: schema file does not exist: " + canonicalSchemaApplyPath(c, schemaPath)
	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(want))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+want+"\n")
}

func runSchemaApplyErrorShape(c *qt.C, args ...string) (stdoutText, stderrText string, err error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(append([]string{"schema", "apply"}, args...))
	err = cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func unsetSchemaApplyPtahEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if !found || !strings.HasPrefix(name, "PTAH_") {
			continue
		}
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
}

func canonicalSchemaApplyPath(c *qt.C, path string) string {
	c.Helper()
	dir, err := filepath.EvalSymlinks(filepath.Dir(path))
	c.Assert(err, qt.IsNil)
	return filepath.Join(dir, filepath.Base(path))
}
