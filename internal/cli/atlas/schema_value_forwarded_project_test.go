package atlas_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/envbinding"
)

// forwardedSchemaVerbs are the verbs that pass -s/--schema on to a native
// command, with the arguments that reach the project file's env.
var forwardedSchemaVerbs = []struct {
	name string
	args []string
}{
	{name: "migrate checkpoint", args: []string{"migrate", "checkpoint", "-c", "file://atlas.hcl", "--env", "local", "scoped"}},
	{name: "schema test", args: []string{"schema", "test", "-c", "file://atlas.hcl", "--env", "local", "tests"}},
}

// TestAtlasForwardedConfiguredSchemaValue_FailurePath pins that `migrate
// checkpoint` and `schema test` read `env { schemas }` through the flag's
// grammar. An element that names no schema, and one holding a comma the native
// list cannot carry, are refused, and the refusal names the file and the env
// the element came from. A bare quote is refused in the pinned binary's words,
// naming the flag, as on the verbs that read the list themselves. A verb that
// ignores the list reaches none of these refusals.
func TestAtlasForwardedConfiguredSchemaValue_FailurePath(t *testing.T) {
	values := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "an empty element", value: "", wantErr: `atlas.hcl: env "local": schemas value "": every field is empty, so the value names no schema`},
		{name: "an empty quoted name", value: `""`, wantErr: `atlas.hcl: env "local": schemas value "\"\"": every field is empty, so the value names no schema`},
		{
			name:    "a name holding a comma",
			value:   `"a,b"`,
			wantErr: `atlas.hcl: env "local": schemas value "\"a,b\"": schema "a,b" holds a comma, and this command passes its schema names on as one comma-separated list`,
		},
		{name: "a bare quote", value: `a"b`, wantErr: schemaValueRefusal},
	}

	for _, verb := range forwardedSchemaVerbs {
		for _, value := range values {
			t.Run(verb.name+" "+value.name, func(t *testing.T) {
				c := qt.New(t)
				root := writeForwardedSchemaFixture(c, value.value)

				out, err := runSchemaValueCommand(c, root, verb.args)

				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(value.wantErr))
				c.Assert(out, qt.Equals, "")
			})
		}
	}
}

// TestAtlasForwardedConfiguredSchemaValueNamesTheFile_FailurePath pins that
// the refusal names the project file --config selects, not atlas.hcl.
func TestAtlasForwardedConfiguredSchemaValueNamesTheFile_FailurePath(t *testing.T) {
	for _, verb := range forwardedSchemaVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeForwardedSchemaFixture(c, "")
			c.Assert(os.Rename(filepath.Join(root, "atlas.hcl"), filepath.Join(root, "custom.hcl")), qt.IsNil)
			args := slices.Clone(verb.args)
			args[slices.Index(args, "file://atlas.hcl")] = "file://custom.hcl"

			out, err := runSchemaValueCommand(c, root, args)

			c.Assert(err, qt.ErrorMatches, `.*custom\.hcl: env "local": schemas value "": every field is empty, so the value names no schema`)
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestAtlasSchemaTestConfiguredSchemas_FailurePath pins that the list reaches
// the native command: `schemas = ["other"]` restricts the desired schema, whose
// one table SQLite creates in `main`, to nothing, and the native command
// refuses a selection that keeps no table. Both variables are absent, so the
// list stands in.
func TestAtlasSchemaTestConfiguredSchemas_FailurePath(t *testing.T) {
	c := qt.New(t)
	root := writeForwardedSchemaFixture(c, "other")
	unsetSchemaEnv(c)

	_, err := runSchemaValueCommand(c, root, []string{"schema", "test", "-c", "file://atlas.hcl", "--env", "local", "tests"})

	c.Assert(err, qt.ErrorMatches, `.*--schema other selects no tables out of the desired schema`)
}

// TestAtlasForwardedConfiguredSchemas_HappyPath pins the precedence: the list
// stands in only where the run leaves the flag unset, as on the verbs that read
// it themselves. The env's element is one the verb refuses, `""` or `other`, so
// a run that read it would fail; the flag, a PTAH_SCHEMA and, on `migrate
// checkpoint`, a PTAH_SCHEMAS each win over it. Each row sets only the
// variables it names. The rows that set none are the controls: an element
// naming the schema the desired schema holds is accepted, and a typed flag
// wins over an empty variable without reading it.
func TestAtlasForwardedConfiguredSchemas_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		element string
		env     map[string]string
	}{
		{name: "migrate checkpoint flag", args: []string{"migrate", "checkpoint", "-c", "file://atlas.hcl", "--env", "local", "-s", "main", "scoped"}},
		{
			name: "migrate checkpoint flag over empty variables",
			args: []string{"migrate", "checkpoint", "-c", "file://atlas.hcl", "--env", "local", "-s", "main", "scoped"},
			env:  map[string]string{"PTAH_SCHEMA": "", "PTAH_SCHEMAS": ""},
		},
		{
			name: "migrate checkpoint PTAH_SCHEMA",
			args: []string{"migrate", "checkpoint", "-c", "file://atlas.hcl", "--env", "local", "scoped"},
			env:  map[string]string{"PTAH_SCHEMA": "main"},
		},
		{
			name: "migrate checkpoint PTAH_SCHEMAS",
			args: []string{"migrate", "checkpoint", "-c", "file://atlas.hcl", "--env", "local", "scoped"},
			env:  map[string]string{"PTAH_SCHEMAS": "main"},
		},
		{name: "migrate checkpoint list", args: []string{"migrate", "checkpoint", "-c", "file://atlas.hcl", "--env", "local", "scoped"}, element: "main"},
		{name: "schema test flag", args: []string{"schema", "test", "-c", "file://atlas.hcl", "--env", "local", "-s", "main", "tests"}, element: "other"},
		{
			name:    "schema test flag over an empty variable",
			args:    []string{"schema", "test", "-c", "file://atlas.hcl", "--env", "local", "-s", "main", "tests"},
			element: "other",
			env:     map[string]string{"PTAH_SCHEMA": ""},
		},
		{
			name:    "schema test PTAH_SCHEMA",
			args:    []string{"schema", "test", "-c", "file://atlas.hcl", "--env", "local", "tests"},
			element: "other",
			env:     map[string]string{"PTAH_SCHEMA": "main"},
		},
		{name: "schema test list", args: []string{"schema", "test", "-c", "file://atlas.hcl", "--env", "local", "tests"}, element: "main"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeForwardedSchemaFixture(c, test.element)
			unsetSchemaEnv(c)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			_, err := runSchemaValueCommand(c, root, test.args)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestAtlasForwardedConfiguredSchemasEmptyEnvironment_FailurePath pins that a
// variable naming no schema is refused on `migrate checkpoint` and `schema
// test` when the env's `schemas` would stand in, as on a run with no list. The
// variable was set, and the list does not say what it was meant to name. The
// element names the schema the desired schema holds, so a run that let the
// list stand in would pass. The controls leave the variables absent: the list
// rows of [TestAtlasForwardedConfiguredSchemas_HappyPath] accept the list, and
// [TestAtlasSchemaTestConfiguredSchemas_FailurePath] shows `schema test`
// applying it. A SQLite checkpoint is the same whatever the list names, so
// the live checkpoint test shows `migrate checkpoint` applying it.
func TestAtlasForwardedConfiguredSchemasEmptyEnvironment_FailurePath(t *testing.T) {
	variables := []struct {
		verb     string
		args     []string
		variable string
		flag     string
		omitted  string
	}{
		{
			verb:     "migrate checkpoint",
			args:     forwardedSchemaVerbs[0].args,
			variable: "PTAH_SCHEMA",
			flag:     "schema",
			omitted:  connectionDefault,
		},
		{
			verb:     "migrate checkpoint",
			args:     forwardedSchemaVerbs[0].args,
			variable: "PTAH_SCHEMAS",
			flag:     "schemas",
			omitted:  connectionDefault,
		},
		{
			verb:     "schema test",
			args:     forwardedSchemaVerbs[1].args,
			variable: "PTAH_SCHEMA",
			flag:     "schema",
			omitted:  everySchema,
		},
	}

	for _, variable := range variables {
		for _, value := range namelessSchemaValues {
			t.Run(variable.verb+" "+variable.variable+" "+strconv.Quote(value.value), func(t *testing.T) {
				c := qt.New(t)
				root := writeForwardedSchemaFixture(c, "main")
				unsetSchemaEnv(c)
				c.Setenv(variable.variable, value.value)

				out, err := runSchemaValueCommand(c, root, variable.args)

				c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(variable.variable+" is "+value.state+
					": give it a value, or unset it and leave --"+variable.flag+" out to "+variable.omitted))
				c.Assert(out, qt.Equals, "")
			})
		}
	}
}

// writeForwardedSchemaFixture makes a working directory for `migrate
// checkpoint` and `schema test` on SQLite: a hashed migration directory, a
// desired schema whose one unqualified table SQLite creates in `main`, a test
// case that reads it, and an atlas.hcl whose local env sets
// `schemas = [element]`.
func writeForwardedSchemaFixture(c *qt.C, element string) string {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(root, "migrations"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "migrations", "20240101000000_init.sql"), []byte("CREATE TABLE t (id INTEGER);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte("CREATE TABLE t (id INTEGER);\n"), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(root, "tests"), 0o700), qt.IsNil)
	testCase := "cases:\n  - name: t\n    steps:\n      - assert:\n          query: \"SELECT count(*) FROM t\"\n          scalar: \"0\"\n"
	c.Assert(os.WriteFile(filepath.Join(root, "tests", "t.yaml"), []byte(testCase), 0o600), qt.IsNil)
	body := fmt.Sprintf(
		"env \"local\" {\n  dev = %q\n  migration {\n    dir = \"file://migrations\"\n  }\n  schema {\n    src = \"file://schema.sql\"\n  }\n  schemas = [%q]\n}\n",
		"sqlite://file?mode=memory", element,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(body), 0o600), qt.IsNil)
	_, err := runSchemaValueCommand(c, root, []string{"migrate", "hash", "--dir", "file://migrations"})
	c.Assert(err, qt.IsNil)
	return root
}
