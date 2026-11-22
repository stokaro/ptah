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

// TestAtlasSchemaEmptyValue_FailurePath pins that a --schema value whose every
// field is empty is refused on every verb that reads the flag. Read as no name,
// it would leave the run with no selection, which is the whole realm, and
// `schema apply` would plan a DROP for every schema the desired state leaves
// out; the pinned binary selects nothing for it.
func TestAtlasSchemaEmptyValue_FailurePath(t *testing.T) {
	verbs := []struct {
		name string
		args []string
	}{
		{name: "schema inspect", args: []string{"schema", "inspect", "-u", "sqlite://dev.db"}},
		{name: "schema apply", args: []string{"schema", "apply", "-u", "sqlite://dev.db", "--to", "file://schema.sql"}},
		{name: "schema diff", args: []string{"schema", "diff", "--from", "sqlite://dev.db", "--to", "file://schema.sql"}},
		{name: "migrate diff", args: []string{"migrate", "diff", "--to", "file://schema.sql", "--dev-url", "sqlite://file?mode=memory"}},
	}
	values := []string{`""`, `,`, `" "`, `"",""`}

	for _, verb := range verbs {
		for _, value := range values {
			t.Run(verb.name+" "+value, func(t *testing.T) {
				c := qt.New(t)
				root := writeSchemaValueFixture(c)

				out, err := runSchemaValueCommand(c, root, append(verb.args, "--schema", value))

				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(fmt.Sprintf(
					`invalid argument %q for "-s, --schema" flag: every field is empty, so the value names no schema`, value)))
				c.Assert(out, qt.Equals, "")
			})
		}
	}
}

// TestAtlasConfiguredSchemaEmptyValue_FailurePath is the project-file and
// environment half: an `env { schemas }` element and PTAH_SCHEMA go through the
// flag's grammar, so a value naming no schema is refused there too.
func TestAtlasConfiguredSchemaEmptyValue_FailurePath(t *testing.T) {
	t.Run("env schemas", func(t *testing.T) {
		c := qt.New(t)
		root := writeSchemaValueFixture(c)
		writeSchemaValueProjectWith(c, root, `""`)

		out, err := runSchemaValueCommand(c, root, []string{"schema", "inspect", "-c", "file://atlas.hcl", "--env", "local"})

		c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
			`invalid argument "\"\"" for "-s, --schema" flag: every field is empty, so the value names no schema`))
		c.Assert(out, qt.Equals, "")
	})
	t.Run("PTAH_SCHEMA", func(t *testing.T) {
		c := qt.New(t)
		root := writeSchemaValueFixture(c)
		c.Setenv("PTAH_SCHEMA", `""`)

		out, err := runSchemaValueCommand(c, root, []string{"schema", "inspect", "-u", "sqlite://dev.db"})

		c.Assert(err, qt.ErrorMatches, `.*every field is empty, so the value names no schema`)
		c.Assert(out, qt.Equals, "")
	})
}

// TestAtlasSchemaValueKeepsEmptyFieldsBesideAName_HappyPath pins the other side
// of the refusal: an empty field beside a name, and a value with no field at
// all, are accepted as the pinned binary accepts them.
func TestAtlasSchemaValueKeepsEmptyFieldsBesideAName_HappyPath(t *testing.T) {
	for _, value := range []string{`main,`, `,main`, `"",main`, ``} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)

			_, err := runSchemaValueCommand(c, root, []string{"schema", "inspect", "-u", "sqlite://dev.db", "--schema", value})

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestAtlasForwardedSchemaValueGrammar_FailurePath pins that the verbs which
// pass --schema on to a native command read it with the same grammar: a bare
// quote and a value naming no schema are refused as the flag refuses them, and
// a name that holds a comma, which the native list cannot carry, is refused
// rather than split into two names.
func TestAtlasForwardedSchemaValueGrammar_FailurePath(t *testing.T) {
	verbs := []struct {
		name string
		args []string
	}{
		{name: "migrate checkpoint", args: []string{"migrate", "checkpoint", "--dev-url", "sqlite://file?mode=memory"}},
		{name: "schema test", args: []string{"schema", "test", "--dev-url", "sqlite://file?mode=memory"}},
	}
	values := []struct {
		value, wantErr string
	}{
		{value: `a"b`, wantErr: schemaValueRefusal},
		{value: `""`, wantErr: `invalid argument "\"\"" for "-s, --schema" flag: every field is empty, so the value names no schema`},
		{value: `"a,b"`, wantErr: `schema "a,b" holds a comma, and this command passes its schema names on as one comma-separated list`},
	}

	for _, verb := range verbs {
		for _, value := range values {
			t.Run(verb.name+" "+value.value, func(t *testing.T) {
				c := qt.New(t)
				root := writeSchemaValueFixture(c)

				out, err := runSchemaValueCommand(c, root, append(verb.args, "--schema", value.value))

				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("atlas "+verb.name+" --schema: "+value.wantErr))
				c.Assert(out, qt.Equals, "")
			})
		}
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
	writeSchemaValueProjectWith(c, root, `a"b`)
}

// writeSchemaValueProjectWith writes an atlas.hcl whose local env sets a schema
// list holding value.
func writeSchemaValueProjectWith(c *qt.C, root, value string) {
	c.Helper()
	body := fmt.Sprintf("env \"local\" {\n  url = %q\n  dev = %q\n  schemas = [%q]\n}\n", "sqlite://dev.db", "sqlite://file?mode=memory", value)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(body), 0o600), qt.IsNil)
}
