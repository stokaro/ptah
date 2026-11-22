package root_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/envbinding"
)

// connectionDefault is what leaving --schemas out does on the verbs that read
// the schema the connection landed in, in the words of the refusal.
const connectionDefault = "read the connection default schema, and on YDB every directory"

// nativeSchemaVariableVerbs are the native verbs whose schema-selecting flag
// reads PTAH_SCHEMAS, or PTAH_SCHEMA on `schema test`.
var nativeSchemaVariableVerbs = []struct {
	variable string
	flag     string
	omitted  string
	args     []string
}{
	{"PTAH_SCHEMAS", "schemas", "let the URL decide which schemas are read", []string{"schema", "apply"}},
	{"PTAH_SCHEMAS", "schemas", "let the URL decide which schemas are read", []string{"schema", "plan"}},
	{"PTAH_SCHEMAS", "schemas", "let the URL decide which schemas are read", []string{"schema", "diff"}},
	{"PTAH_SCHEMAS", "schemas", "let the URL decide which schemas are read", []string{"schema", "inspect"}},
	{"PTAH_SCHEMAS", "schemas", "let the URL decide which schemas are read", []string{"schema", "compare"}},
	{"PTAH_SCHEMAS", "schemas", "let the URL decide which schemas are read", []string{"schema", "drift"}},
	{"PTAH_SCHEMAS", "schemas", "let the URL decide which schemas are read", []string{"migrations", "plan"}},
	{"PTAH_SCHEMAS", "schemas", "use the project's schemas, else let the URL decide", []string{"migrations", "generate"}},
	{"PTAH_SCHEMAS", "schemas", "use the project's schemas, else let the URL decide", []string{"schema", "serve"}},
	{"PTAH_SCHEMAS", "schemas", "read each connection's default schema with --shadow-db, and without it what ptah schema compare reads", []string{"migrations", "baseline"}},
	{"PTAH_SCHEMAS", "schemas", connectionDefault, []string{"migrations", "checkpoint"}},
	{"PTAH_SCHEMAS", "schemas", connectionDefault, []string{"db", "read"}},
	{"PTAH_SCHEMAS", "schemas", connectionDefault, []string{"introspect"}},
	{"PTAH_SCHEMAS", "schemas", connectionDefault, []string{"schema", "lineage"}},
	{"PTAH_SCHEMAS", "schemas", connectionDefault, []string{"schema", "security"}},
	{"PTAH_SCHEMAS", "schemas", connectionDefault, []string{"schema", "stats"}},
	{"PTAH_SCHEMA", "schema", "keep every schema of the desired schema", []string{"schema", "test"}},
}

// unsetSchemaVariables removes both schema variables for the rest of the test.
func unsetSchemaVariables(c *qt.C) {
	c.Helper()
	for _, name := range []string{"PTAH_SCHEMA", "PTAH_SCHEMAS"} {
		c.Setenv(name, "")
		c.Assert(os.Unsetenv(name), qt.IsNil)
	}
}

// namelessSchemaValues are the values of a schema variable that name no schema
// once split on commas and trimmed, with the words the refusal gives each.
var namelessSchemaValues = []struct {
	value string
	state string
}{
	{value: "", state: "set but empty"},
	{value: " ", state: `set to " ", which names nothing`},
	{value: ",", state: `set to ",", which names nothing`},
	{value: " , ", state: `set to " , ", which names nothing`},
}

// TestNativeSchemaVariableEmpty_FailurePath pins that a schema-selecting
// variable naming no schema is refused, naming itself, on every native verb
// that binds it, before the verb reads its arguments. Read as no selection, it
// would widen the run to what leaving the flag out selects: on `schema apply`
// every schema of a URL that pins none, with a DROP planned for each one the
// desired schema leaves out.
func TestNativeSchemaVariableEmpty_FailurePath(t *testing.T) {
	for _, verb := range nativeSchemaVariableVerbs {
		for _, value := range namelessSchemaValues {
			t.Run(verb.variable+" "+filepath.Join(verb.args...)+" "+strconv.Quote(value.value), func(t *testing.T) {
				c := qt.New(t)
				unsetSchemaVariables(c)
				c.Setenv(verb.variable, value.value)

				stdout, _, err := executeRootCommand(verb.args...)

				c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(verb.variable+" is "+value.state+
					": give it a value, or unset it and leave --"+verb.flag+" out to "+verb.omitted))
				c.Assert(stdout, qt.Equals, "")
			})
		}
	}
}

// TestNativeSchemaVariable_HappyPath is the control: unset, or set to a
// schema, the variable lets `schema apply` run.
func TestNativeSchemaVariable_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{name: "unset"},
		{name: "a schema", env: map[string]string{"PTAH_SCHEMAS": "main"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			schemaFile := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE t (id INTEGER);\n"), 0o600), qt.IsNil)
			unsetSchemaVariables(c)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			stdout, stderr, err := executeRootCommand("schema", "apply",
				"--db-url", "sqlite://"+filepath.ToSlash(filepath.Join(dir, "target.db")),
				"--schema-file", schemaFile, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
			c.Assert(filepath.Join(dir, "target.db"), qt.Satisfies, fileExists)
		})
	}
}

// fileExists reports whether path names a file.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
