package viz

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCommandWritesMermaid(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)

	cmd := NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "mermaid",
		"--include-columns",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "erDiagram\n")
	c.Assert(stdout.String(), qt.Contains, "  users {\n")
	c.Assert(stdout.String(), qt.Contains, "    SERIAL id PK\n")
	c.Assert(stdout.String(), qt.Contains, "    INTEGER author_id FK\n")
	c.Assert(stdout.String(), qt.Contains, `  users ||--o{ posts : "fk_posts_author"`)
}

func TestREADMEExampleMatchesGeneratedMermaid(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)

	cmd := NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "mermaid",
		"--include-columns",
	})

	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

	readme, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	c.Assert(err, qt.IsNil)
	example, ok := readmeMermaidExample(string(readme))
	c.Assert(ok, qt.IsTrue)
	c.Assert(strings.TrimSpace(example), qt.Equals, strings.TrimSpace(stdout.String()))
}

func TestCommandExcludesTables(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)

	cmd := NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "dot",
		"--exclude-tables", "users",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "digraph ptah_schema")
	c.Assert(stdout.String(), qt.Contains, `"posts"`)
	c.Assert(stdout.String(), qt.Not(qt.Contains), `"users"`)
	c.Assert(stdout.String(), qt.Not(qt.Contains), "fk_posts_author")
}

func TestDOTParsesWithGraphvizWhenInstalled(t *testing.T) {
	c := qt.New(t)
	if _, err := exec.LookPath("dot"); err != nil {
		t.Skipf("Graphviz dot not installed: %v", err)
	}
	dir := t.TempDir()
	writeModel(c, dir)

	cmd := NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "dot",
		"--include-columns",
	})

	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

	dotCmd := exec.Command("dot", "-Tsvg")
	dotCmd.Stdin = bytes.NewReader(stdout.Bytes())
	var svg bytes.Buffer
	var dotStderr bytes.Buffer
	dotCmd.Stdout = &svg
	dotCmd.Stderr = &dotStderr
	err = dotCmd.Run()

	c.Assert(err, qt.IsNil, qt.Commentf("dot stderr:\n%s", dotStderr.String()))
	c.Assert(svg.String(), qt.Contains, "<svg")
}

func TestCommandWritesSVGWhenGraphvizIsInstalled(t *testing.T) {
	c := qt.New(t)
	if _, err := exec.LookPath("dot"); err != nil {
		t.Skipf("Graphviz dot not installed: %v", err)
	}
	dir := t.TempDir()
	writeModel(c, dir)

	cmd := NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "svg",
		"--theme", "dark",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "<svg")
	c.Assert(stdout.String(), qt.Contains, "#111827")
}

func TestExampleArtifactsMatchGeneratedOutput(t *testing.T) {
	currentDir, err := os.Getwd()
	c := qt.New(t)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(filepath.Join("..", "..")), qt.IsNil)
	t.Cleanup(func() {
		c.Assert(os.Chdir(currentDir), qt.IsNil)
	})

	exampleDir := filepath.Join("examples", "viz")
	rootDir := filepath.Join(exampleDir, "models")
	tests := []struct {
		name     string
		format   string
		wantPath string
	}{
		{
			name:     "mermaid",
			format:   "mermaid",
			wantPath: filepath.Join(exampleDir, "schema.mmd"),
		},
		{
			name:     "dot",
			format:   "dot",
			wantPath: filepath.Join(exampleDir, "schema.dot"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := NewCommand()
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs([]string{
				"--root-dir", rootDir,
				"--format", tt.format,
				"--include-columns",
			})

			err := cmd.Execute()
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

			want, err := os.ReadFile(tt.wantPath)
			c.Assert(err, qt.IsNil)
			c.Assert(stdout.String(), qt.Equals, string(want))
		})
	}
}

func TestSVGReportsFriendlyGraphvizErrorWhenDotIsMissing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)
	t.Setenv("PATH", t.TempDir())

	cmd := NewCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "svg",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `Graphviz dot is required for --format svg; install graphviz or use --format dot: .*`)
	c.Assert(stderr.String(), qt.Contains, "Graphviz dot is required for --format svg")
}

func TestSVGReportsGraphvizStderrOnFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses a POSIX shell script")
	}
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)
	binDir := t.TempDir()
	dotPath := filepath.Join(binDir, "dot")
	c.Assert(os.WriteFile(dotPath, []byte("#!/bin/sh\necho graphviz exploded >&2\nexit 42\n"), 0o600), qt.IsNil)
	c.Assert(os.Chmod(dotPath, 0o700), qt.IsNil)
	t.Setenv("PATH", binDir)

	cmd := NewCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "svg",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `render SVG with Graphviz dot: .*: graphviz exploded`)
	c.Assert(stderr.String(), qt.Contains, "graphviz exploded")
}

func readmeMermaidExample(readme string) (string, bool) {
	const marker = "Example Mermaid output:\n\n```mermaid\n"
	_, after, ok := strings.Cut(readme, marker)
	if !ok {
		return "", false
	}
	example, _, ok := strings.Cut(after, "\n```")
	return example, ok
}

func writeModel(c *qt.C, dir string) {
	path := filepath.Join(dir, "model.go")
	content := `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="TEXT"
	Email string
}

//migrator:schema:table name="posts"
type Post struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="author_id" type="INTEGER" foreign="users(id)" foreign_key_name="fk_posts_author"
	AuthorID int64
}
`
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
}
