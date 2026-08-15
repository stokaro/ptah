//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

func writeDottedIncludeDiffFiles(c *qt.C, fromSQL, toSQL string) (fromPath, toPath, devPath string) {
	c.Helper()
	dir := c.TempDir()
	fromPath = filepath.Join(dir, "from.sql")
	toPath = filepath.Join(dir, "to.sql")
	devPath = filepath.Join(dir, "dev.db")
	c.Assert(os.WriteFile(fromPath, []byte(fromSQL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(toSQL), 0o600), qt.IsNil)
	return fromPath, toPath, devPath
}

func runDottedIncludeDiff(fromPath, toPath, devPath, selector string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + fromPath,
		"--to", "file://" + toPath,
		"--dev-url", "sqlite://" + devPath,
		"--include", selector,
	})
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// TestAtlasCompatDottedIncludeMissFailsE2E pins the stokaro/ptah#933 safety
// decision at the filesystem and command boundary. The pinned CE v1.3.0 binary
// refuses --include as non-community, so it provides no selector-semantic
// answer. Ptah's full surface retains the Pro-like flag but refuses a selector
// that meets neither side instead of reporting a false synced result to CI.
func TestAtlasCompatDottedIncludeMissFailsE2E(t *testing.T) {
	c := qt.New(t)
	fromPath, toPath, devPath := writeDottedIncludeDiffFiles(c,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
			"CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);\n")

	stdout, stderr, err := runDottedIncludeDiff(fromPath, toPath, devPath, "posts.title")

	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "posts.title"`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, `Error: the --include selection matched no objects: "posts.title"`+"\n")
}

// TestAtlasCompatDottedIncludeControlsE2E proves the refusal is outcome-based:
// one-sided matches remain valid creates and drops, and a top-level table whose
// identifier contains a dot remains selectable on the full Pro-like surface.
func TestAtlasCompatDottedIncludeControlsE2E(t *testing.T) {
	c := qt.New(t)
	fromPath, toPath, devPath := writeDottedIncludeDiffFiles(c, "",
		"CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);\n")

	plainOut, plainErrOut, plainErr := runDottedIncludeDiff(fromPath, toPath, devPath, "posts")

	c.Assert(plainErr, qt.IsNil, qt.Commentf("stderr:\n%s", plainErrOut))
	c.Assert(plainOut, qt.Contains, "CREATE TABLE")
	c.Assert(plainOut, qt.Contains, "posts")

	dropFrom, dropTo, dropDev := writeDottedIncludeDiffFiles(c,
		"CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);\n", "")
	dropOut, dropErrOut, dropErr := runDottedIncludeDiff(dropFrom, dropTo, dropDev, "posts")

	c.Assert(dropErr, qt.IsNil, qt.Commentf("stderr:\n%s", dropErrOut))
	c.Assert(dropOut, qt.Contains, "DROP TABLE")
	c.Assert(dropOut, qt.Contains, "posts")

	dottedFrom, dottedTo, dottedDev := writeDottedIncludeDiffFiles(c, "",
		"CREATE TABLE \"posts.title\" (id INTEGER PRIMARY KEY);\n")

	dottedOut, dottedErrOut, dottedErr := runDottedIncludeDiff(dottedFrom, dottedTo, dottedDev, "posts.title")

	c.Assert(dottedErr, qt.IsNil, qt.Commentf("stderr:\n%s", dottedErrOut))
	c.Assert(dottedOut, qt.Contains, `"posts.title"`)
}
