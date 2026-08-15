//go:build integration

package viz_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/viz"
)

func TestDOTParsesWithGraphviz(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeGraphvizModel(c, dir)

	cmd := viz.NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "dot",
		"--include-columns",
	})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

	dotCmd := exec.Command("dot", "-Tsvg")
	dotCmd.Stdin = bytes.NewReader(stdout.Bytes())
	var svg bytes.Buffer
	var dotStderr bytes.Buffer
	dotCmd.Stdout = &svg
	dotCmd.Stderr = &dotStderr
	c.Assert(dotCmd.Run(), qt.IsNil, qt.Commentf("dot stderr:\n%s", dotStderr.String()))
	c.Assert(svg.String(), qt.Contains, "<svg")
}

func TestCommandWritesSVGWithGraphviz(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeGraphvizModel(c, dir)

	cmd := viz.NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir,
		"--format", "svg",
		"--theme", "dark",
	})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "<svg")
	c.Assert(stdout.String(), qt.Contains, "#111827")
}

func writeGraphvizModel(c *qt.C, dir string) {
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
