package atlas_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
)

// schemaValueRefusal is the refusal the pinned community binary prints for a
// -s/--schema value that is not one line of comma-separated fields, on the
// command line and in `env { schemas }` alike. pflag writes it for a string
// slice flag, which is how the pinned binary declares --schema.
const schemaValueRefusal = `invalid argument "a\"b" for "-s, --schema" flag: parse error on line 1, column 2: bare " in non-quoted-field`

// TestAtlasSchemaValueGrammar_FailurePath pins that a --schema value with a
// bare quote is refused on every verb that reads the flag. The pinned binary
// refuses it at exit 1 while it parses the flags; a reader that splits on commas
// alone accepts the value and looks for a schema called `a"b`.
func TestAtlasSchemaValueGrammar_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema inspect", args: []string{"schema", "inspect", "-u", "sqlite://dev.db"}},
		{name: "schema apply", args: []string{"schema", "apply", "-u", "sqlite://dev.db", "--to", "file://schema.sql"}},
		{name: "schema diff", args: []string{"schema", "diff", "--from", "sqlite://dev.db", "--to", "file://schema.sql"}},
		{name: "migrate diff", args: []string{"migrate", "diff", "--to", "file://schema.sql", "--dev-url", "sqlite://file?mode=memory"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)

			out, err := runSchemaValueCommand(c, root, append(test.args, "--schema", `a"b`))

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(schemaValueRefusal))
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestAtlasConfiguredSchemaValueGrammar_FailurePath is the project-file half:
// the pinned binary reads each element of `env { schemas }` as one --schema
// value, so the same text is refused with the same message, naming the flag.
func TestAtlasConfiguredSchemaValueGrammar_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema inspect", args: []string{"schema", "inspect", "-c", "file://atlas.hcl", "--env", "local"}},
		{name: "schema apply", args: []string{"schema", "apply", "-c", "file://atlas.hcl", "--env", "local", "--to", "file://schema.sql"}},
		{name: "schema diff", args: []string{"schema", "diff", "-c", "file://atlas.hcl", "--env", "local", "--from", "sqlite://dev.db", "--to", "file://schema.sql"}},
		{name: "migrate diff", args: []string{"migrate", "diff", "-c", "file://atlas.hcl", "--env", "local", "--to", "file://schema.sql"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)
			writeSchemaValueProject(c, root)

			out, err := runSchemaValueCommand(c, root, test.args)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(schemaValueRefusal))
			c.Assert(out, qt.Equals, "")
		})
	}
}

// writeSchemaValueFixture makes a working directory holding the desired schema
// file the verbs read, and returns it.
func writeSchemaValueFixture(c *qt.C) string {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte("CREATE TABLE t (id INTEGER);\n"), 0o600), qt.IsNil)
	return root
}

// runSchemaValueCommand runs the compatibility tree in root and returns what it
// wrote to stdout.
func runSchemaValueCommand(c *qt.C, root string, args []string) (string, error) {
	c.Helper()
	c.Chdir(root)
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// writeSchemaValueProject writes an atlas.hcl whose local env sets a schema
// list holding the value -s/--schema refuses.
func writeSchemaValueProject(c *qt.C, root string) {
	c.Helper()
	body := fmt.Sprintf("env \"local\" {\n  url = %q\n  dev = %q\n  schemas = [%q]\n}\n", "sqlite://dev.db", "sqlite://file?mode=memory", `a"b`)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(body), 0o600), qt.IsNil)
}
