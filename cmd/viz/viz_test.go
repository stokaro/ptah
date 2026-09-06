package viz_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/viz"
	"ptah.run/internal/testutils"
)

func TestCommandWritesMermaid(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)

	cmd := viz.NewCommand()
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

func TestCommandDoesNotDuplicateJSONEmbeddedColumns(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeJSONEmbeddedModel(c, dir)

	cmd := viz.NewCommand()
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
	c.Assert(strings.Count(stdout.String(), "    JSONB metadata\n"), qt.Equals, 1)
}

func TestCommandExcludesTables(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)

	cmd := viz.NewCommand()
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
			cmd := viz.NewCommand()
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

	cmd := viz.NewCommand()
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
	testutils.SkipWithoutPOSIXShell(t)
	skipOnWindows(t)
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)
	binDir := t.TempDir()
	dotPath := filepath.Join(binDir, "dot")
	c.Assert(os.WriteFile(dotPath, []byte("#!/bin/sh\necho graphviz exploded >&2\nexit 42\n"), 0o600), qt.IsNil)
	c.Assert(os.Chmod(dotPath, 0o700), qt.IsNil)
	t.Setenv("PATH", binDir)

	cmd := viz.NewCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "svg",
	})

	// A deadline of this test's own, because the assertion is about the
	// diagnostic and not about the clock. The render used to impose ten seconds
	// on every caller, so on a loaded machine the fake `dot` lost to that
	// deadline and this reported a broken Graphviz diagnostic when the
	// diagnostic was fine -- twice, in one sitting.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err := cmd.ExecuteContext(ctx)

	c.Assert(err, qt.ErrorMatches, `render SVG with Graphviz dot: .*: graphviz exploded`)
	c.Assert(stderr.String(), qt.Contains, "graphviz exploded")
}

// TestSVGRespectsTheCallersDeadline is the property the fix adds: a caller that
// named a budget keeps it.
//
// A deadline already past means the render must not run at all, and the failure
// has to name the deadline rather than Graphviz -- which is what distinguishes
// "the caller's context governs" from "the default happened to be longer".
func TestSVGRespectsTheCallersDeadline(t *testing.T) {
	testutils.SkipWithoutPOSIXShell(t)
	skipOnWindows(t)
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)
	binDir := t.TempDir()
	dotPath := filepath.Join(binDir, "dot")
	c.Assert(os.WriteFile(dotPath, []byte("#!/bin/sh\nexit 0\n"), 0o600), qt.IsNil)
	c.Assert(os.Chmod(dotPath, 0o700), qt.IsNil)
	t.Setenv("PATH", binDir)

	cmd := viz.NewCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "svg",
	})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := cmd.ExecuteContext(ctx)

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}

func skipOnWindows(t *testing.T) {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("test uses a POSIX shell script")
	}
}

func writeModel(c *qt.C, dir string) {
	path := filepath.Join(dir, "model.go")
	content := `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT"
	Email string
}

//ptah:schema:table name="posts"
type Post struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="author_id" type="INTEGER" foreign="users(id)" foreign_key_name="fk_posts_author"
	AuthorID int64
}
`
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
}

func writeJSONEmbeddedModel(c *qt.C, dir string) {
	path := filepath.Join(dir, "model.go")
	content := `package models

type UserMetadata struct {
	TraceID string
}

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:embedded mode="json" name="metadata" type="JSONB"
	Metadata UserMetadata
}
`
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
}
