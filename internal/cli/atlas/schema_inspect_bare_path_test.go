package atlas_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
)

// TestCompatSchemaInspect_BarePathNamingNothing_FailurePath covers a `--url`
// written with no scheme that names nothing in the working directory. `-u=`
// gives the flag the value `=`, which reads as such a path. The pinned
// community binary v1.4.0 answers each row with the line below, measured on
// 2026-11-21 in an empty directory at exit 1 with standard output empty. Read
// as a schema file instead, each one was refused for a `--dev-url` the
// operator never gave.
func TestCompatSchemaInspect_BarePathNamingNothing_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "an equals sign attached to the shorthand", args: []string{"schema", "inspect", "-u="}},
		{name: "a bare word", args: []string{"schema", "inspect", "-u", "foo"}},
		{name: "a value that starts with an equals sign", args: []string{"schema", "inspect", "--url", "=foo"}},
		{name: "a relative path", args: []string{"schema", "inspect", "--url", "./absent.sql"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())

			run := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(run.code, qt.Equals, 1)
			c.Assert(run.stderr, qt.Equals, compatURLMissingScheme)
			c.Assert(run.stdout, qt.Equals, "")
		})
	}
}

// TestCompatSchemaInspect_BarePathNamingAFile_HappyPath is the control for the
// test above: a bare path that names a file is still read as a schema file.
// The pinned binary refuses it with `missing scheme. Did you mean
// file://schema.sql?`; Ptah keeps the capability, a retained divergence
// docs/conformance.md names.
func TestCompatSchemaInspect_BarePathNamingAFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	c.Assert(os.WriteFile("schema.sql", []byte("CREATE TABLE t (id int);\n"), 0o600), qt.IsNil)

	run := runTree(atlas.NewCompatCommand("atlas"),
		"schema", "inspect", "--url", "schema.sql", "--dev-url", "sqlite://dev?mode=memory")

	c.Assert(run.code, qt.Equals, 0, qt.Commentf("stderr=%q", run.stderr))
	c.Assert(run.stdout, qt.Contains, `table "t" {`)
	c.Assert(run.stderr, qt.Equals, "")
}

// TestCompatSchemaInspect_FileURLNamingNothing_FailurePath is the control for
// the `file://` exclusion: a URL that carries the scheme keeps the loader's
// answer, which names the path it could not find, rather than being told it has
// no scheme.
func TestCompatSchemaInspect_FileURLNamingNothing_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())

	run := runTree(atlas.NewCompatCommand("atlas"),
		"schema", "inspect", "--url", "file://absent.sql", "--dev-url", "sqlite://dev?mode=memory")

	c.Assert(run.code, qt.Equals, 1)
	c.Assert(run.stderr, qt.Matches, `Error: schema file does not exist: .*absent\.sql\n`)
	c.Assert(run.stdout, qt.Equals, "")
}

// TestCompatSchemaInspect_OpaqueSQLiteURL_HappyPath is the control for the
// source-kind check: `sqlite:t.db` carries no `://` but names a database, the
// form a Windows drive path takes, so it is opened rather than read as a bare
// path.
func TestCompatSchemaInspect_OpaqueSQLiteURL_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	c.Assert(os.WriteFile("t.db", nil, 0o600), qt.IsNil)

	run := runTree(atlas.NewCompatCommand("atlas"), "schema", "inspect", "--url", "sqlite:t.db")

	c.Assert(run.code, qt.Equals, 0, qt.Commentf("stderr=%q", run.stderr))
	c.Assert(run.stdout, qt.Contains, `schema "main" {`)
	c.Assert(run.stderr, qt.Equals, "")
}
