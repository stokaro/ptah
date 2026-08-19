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

	t.Run("compat malformed HCL matches Atlas error shape", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		schemaPath := filepath.Join(dir, "schema.hcl")
		c.Assert(os.WriteFile(schemaPath, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
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

	t.Run("compat unrelated loader error keeps context", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		schemaPath := filepath.Join(dir, "missing.hcl")

		stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
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

	// Cell 9.13's second half. The pinned community binary v1.3.0 echoes the
	// --to path in the form it was given, and every form above is absolute, so
	// the rows here are the ones the absolute cases cannot see.
	t.Run("compat relative --to reports the relative path", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		c.Assert(os.MkdirAll(filepath.Join(dir, "fx", "sub"), 0o750), qt.IsNil)
		relatives := []struct {
			name    string
			written string
			given   string
			want    string
		}{
			{
				name:    "plain relative",
				written: filepath.Join("fx", "bad.hcl"),
				given:   "file://fx/bad.hcl",
				want:    filepath.Join("fx", "bad.hcl"),
			},
			{
				// The oracle normalizes the leading "./" away rather than
				// echoing the literal spelling.
				name:    "dot relative",
				written: filepath.Join("fx", "dot.hcl"),
				given:   "file://./fx/dot.hcl",
				want:    filepath.Join("fx", "dot.hcl"),
			},
			{
				name:    "nested relative",
				written: filepath.Join("fx", "sub", "deep.hcl"),
				given:   "file://fx/sub/deep.hcl",
				want:    filepath.Join("fx", "sub", "deep.hcl"),
			},
			{
				name:    "URL-escaped relative",
				written: filepath.Join("fx", "escaped name.hcl"),
				given:   "file://fx/escaped%20name.hcl",
				want:    filepath.Join("fx", "escaped name.hcl"),
			},
		}
		for _, relative := range relatives {
			t.Run(relative.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(os.WriteFile(
					filepath.Join(dir, relative.written), []byte("schema \"main\" {\n"), 0o600,
				), qt.IsNil)
				stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
					"schema", "apply",
					"--url", "sqlite://"+filepath.Join(dir, "target.db"),
					"--to", relative.given,
					"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
					"--dry-run",
				)

				c.Check(exitStatusOf(c, err), qt.Equals, 1)
				c.Check(stdout, qt.Equals, "")
				c.Check(stderr, qt.Equals,
					"Error: "+relative.want+malformedSchemaHCLProcessDiagnostic+"\n")
			})
		}

		t.Run("in-tree symlink keeps the authored path", func(t *testing.T) {
			c := qt.New(t)
			target := filepath.Join(dir, "fx", "target.hcl")
			link := filepath.Join(dir, "fx", "linked.hcl")
			c.Assert(os.WriteFile(target, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)
			c.Assert(os.Symlink(target, link), qt.IsNil)

			stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
				"schema", "apply",
				"--url", "sqlite://"+filepath.Join(dir, "target.db"),
				"--to", "file://fx/linked.hcl",
				"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
				"--dry-run",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				"Error: "+filepath.Join("fx", "linked.hcl")+malformedSchemaHCLProcessDiagnostic+"\n")
		})

		t.Run("relative directory keeps its member path", func(t *testing.T) {
			c := qt.New(t)
			schemaDir := filepath.Join(dir, "schemas")
			c.Assert(os.MkdirAll(schemaDir, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(schemaDir, "bad.hcl"),
				[]byte("schema \"main\" {\n"),
				0o600,
			), qt.IsNil)

			stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
				"schema", "apply",
				"--url", "sqlite://"+filepath.Join(dir, "target.db"),
				"--to", "file://schemas",
				"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
				"--dry-run",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				"Error: "+filepath.Join("schemas", "bad.hcl")+
					malformedSchemaHCLProcessDiagnostic+"\n")
		})

		t.Run("relative directory keeps a symlinked member's authored path", func(t *testing.T) {
			c := qt.New(t)
			schemaDir := filepath.Join(dir, "linked-schemas")
			fixtureDir := filepath.Join(dir, "fixtures")
			c.Assert(os.MkdirAll(schemaDir, 0o750), qt.IsNil)
			c.Assert(os.MkdirAll(fixtureDir, 0o750), qt.IsNil)
			target := filepath.Join(fixtureDir, "target.hcl")
			c.Assert(os.WriteFile(target, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)
			c.Assert(os.Symlink(target, filepath.Join(schemaDir, "bad.hcl")), qt.IsNil)

			stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
				"schema", "apply",
				"--url", "sqlite://"+filepath.Join(dir, "target.db"),
				"--to", "file://linked-schemas",
				"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
				"--dry-run",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				"Error: "+filepath.Join("linked-schemas", "bad.hcl")+
					malformedSchemaHCLProcessDiagnostic+"\n")
		})

		t.Run("SQL-named symlink to HCL keeps its authored path", func(t *testing.T) {
			c := qt.New(t)
			schemaDir := filepath.Join(dir, "sql-link-schemas")
			fixtureDir := filepath.Join(dir, "sql-link-fixtures")
			c.Assert(os.MkdirAll(schemaDir, 0o750), qt.IsNil)
			c.Assert(os.MkdirAll(fixtureDir, 0o750), qt.IsNil)
			target := filepath.Join(fixtureDir, "target.hcl")
			c.Assert(os.WriteFile(target, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)
			c.Assert(os.Symlink(target, filepath.Join(schemaDir, "bad.sql")), qt.IsNil)

			stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
				"schema", "apply",
				"--url", "sqlite://"+filepath.Join(dir, "target.db"),
				"--to", "file://sql-link-schemas",
				"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
				"--dry-run",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				"Error: "+filepath.Join("sql-link-schemas", "bad.sql")+
					malformedSchemaHCLProcessDiagnostic+"\n")
		})
	})

	t.Run("native relative --to keeps the resolved absolute path", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		c.Assert(os.MkdirAll(filepath.Join(dir, "fx"), 0o750), qt.IsNil)
		schemaPath := filepath.Join(dir, "fx", "native.hcl")
		c.Assert(os.WriteFile(schemaPath, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runCLIProcess(ctx, dir, nativeBinary,
			"schema", "apply",
			"--db-url", "sqlite://"+filepath.Join(dir, "target.db"),
			"--to", "file://fx/native.hcl",
			"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
			"--dry-run",
		)
		want := "load --to schema: parse HCL schema: " +
			canonicalSchemaApplyProcessPath(c, schemaPath) + malformedSchemaHCLProcessDiagnostic

		c.Check(exitStatusOf(c, err), qt.Equals, 2)
		c.Check(stdout, qt.Equals, "")
		c.Check(stderr, qt.Equals, "error: "+want+"\n")
	})

	t.Run("native malformed HCL keeps native context", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		schemaPath := filepath.Join(dir, "schema.hcl")
		c.Assert(os.WriteFile(schemaPath, []byte("schema \"main\" {\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runCLIProcess(ctx, dir, nativeBinary,
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

func runCLIProcess(
	ctx context.Context,
	dir,
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
