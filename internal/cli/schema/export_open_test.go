package schema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// openModel is the smallest schema that renders a document worth opening.
const openModel = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`

func writeOpenModel(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(openModel), 0o600), qt.IsNil)
	return dir
}

// `--open` is the native half of the capability `ptah-compat schema inspect
// --web` uses. It exists so the behavior is not implemented only inside the
// Atlas-compatible tree, which AGENTS.md forbids for anything general -- and
// showing a rendered document is general.
//
// CI is set, so the run is one that cannot open a browser: the assertion is
// about the artifact and the report, not about this machine's desktop.
func TestSchemaExportOpenWritesADocumentToShow(t *testing.T) {
	c := qt.New(t)
	t.Setenv("CI", "true")
	dir := writeOpenModel(c)

	_, stderr, err := runSchemaExport("--to", "html", "--root-dir", dir, "--open")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stderr, qt.Contains, "ERD written to ")
	c.Assert(stderr, qt.Contains, "not opened: CI is set")
}

// With --out the file the operator named is the one that opens. Writing a
// second copy somewhere else would leave two documents and no way to tell which
// the browser showed.
func TestSchemaExportOpenShowsTheFileTheOperatorNamed(t *testing.T) {
	c := qt.New(t)
	t.Setenv("CI", "true")
	dir := writeOpenModel(c)
	out := filepath.Join(c.TempDir(), "schema.html")

	_, stderr, err := runSchemaExport("--to", "html", "--root-dir", dir, "--out", out, "--open")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stderr, qt.Contains, "ERD written to ")
	c.Assert(stderr, qt.Contains, filepath.Base(out))
	_, statErr := os.Stat(out)
	c.Assert(statErr, qt.IsNil)
}

// Without --open the export is what it was: a document on standard output and
// nothing said about browsers. A flag that changed the default output would
// break every pipeline reading the export.
func TestSchemaExportWithoutOpenSaysNothingAboutBrowsers(t *testing.T) {
	c := qt.New(t)
	dir := writeOpenModel(c)

	stdout, stderr, err := runSchemaExport("--to", "html", "--root-dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(strings.HasPrefix(stdout, "<!doctype html>"), qt.IsTrue)
	c.Assert(stderr, qt.Not(qt.Contains), "ERD written to ")
	c.Assert(stderr, qt.Not(qt.Contains), "not opened")
}

// A malformed suppression is a configuration error on every export, not only on
// the one that asks to open something.
//
// Both rows matter, and the second is the one that would rot: an operator who
// wrote `yes` in a CI environment file believes they suppressed the browser,
// and a check that only ran under --open would leave that belief standing until
// the day somebody passed the flag. AGENTS.md states the rule -- resolve the
// variables a command owns before its early returns.
func TestSchemaExportFailurePath(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "asking to open", args: []string{"--open"}},
		{name: "not asking to open", args: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("PTAH_SKIP_BROWSER_OPEN", "yes")
			dir := writeOpenModel(c)

			args := append([]string{"--to", "html", "--root-dir", dir}, test.args...)
			_, stderr, err := runSchemaExport(args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Contains, "PTAH_SKIP_BROWSER_OPEN")
		})
	}
}
