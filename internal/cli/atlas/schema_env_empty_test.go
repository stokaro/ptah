package atlas_test

import (
	"os"
	"regexp"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/envbinding"
)

// everySchema and connectionDefault are what leaving -s/--schema out does, in
// the words the refusal of an empty variable gives: every verb selects every
// schema except `migrate checkpoint`, which forwards no --schemas and reads
// what the native checkpoint reads.
const (
	everySchema       = "select every schema"
	connectionDefault = "read the connection default schema, and on YDB every directory"
)

// schemaEnvVerbs are the verbs whose -s/--schema reads PTAH_SCHEMA, each with
// arguments that reach the point where the variable is read and what leaving
// the flag out does.
var schemaEnvVerbs = []struct {
	name    string
	args    []string
	omitted string
}{
	{name: "schema inspect", args: []string{"schema", "inspect", "-u", "sqlite://dev.db"}, omitted: everySchema},
	{name: "schema apply", args: []string{"schema", "apply", "-u", "sqlite://dev.db", "--to", "file://schema.sql"}, omitted: everySchema},
	{name: "schema diff", args: []string{"schema", "diff", "--from", "sqlite://dev.db", "--to", "file://schema.sql"}, omitted: everySchema},
	{name: "schema plan", args: []string{"schema", "plan", "--from", "sqlite://dev.db", "--to", "file://schema.sql"}, omitted: everySchema},
	{name: "schema plan new", args: []string{"schema", "plan", "new", "--from", "sqlite://dev.db", "--to", "file://schema.sql"}, omitted: everySchema},
	{name: "schema plan validate", args: []string{"schema", "plan", "validate", "--from", "sqlite://dev.db", "--file", "file://x.plan.hcl"}, omitted: everySchema},
	{name: "schema plan lint", args: []string{"schema", "plan", "lint", "--from", "sqlite://dev.db", "--file", "file://x.plan.hcl"}, omitted: everySchema},
	{name: "migrate diff", args: []string{"migrate", "diff", "--to", "file://schema.sql", "--dev-url", "sqlite://file?mode=memory"}, omitted: everySchema},
	{name: "migrate checkpoint", args: []string{"migrate", "checkpoint", "--dev-url", "sqlite://file?mode=memory"}, omitted: connectionDefault},
	{name: "schema test", args: []string{"schema", "test", "--dev-url", "sqlite://file?mode=memory"}, omitted: everySchema},
}

// unsetSchemaEnv removes both schema variables for the rest of the test.
func unsetSchemaEnv(c *qt.C) {
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

// TestAtlasSchemaEnvironmentEmpty_FailurePath pins that a PTAH_SCHEMA naming no
// schema is refused on every verb whose -s/--schema reads it, as an empty -s
// value is. Read as no selection, it would select every schema, and `schema
// apply` would plan a DROP for each one the desired schema leaves out. The
// refusal names what leaving the flag out does on that verb.
func TestAtlasSchemaEnvironmentEmpty_FailurePath(t *testing.T) {
	for _, verb := range schemaEnvVerbs {
		for _, value := range namelessSchemaValues {
			t.Run(verb.name+" "+strconv.Quote(value.value), func(t *testing.T) {
				c := qt.New(t)
				root := writeSchemaValueFixture(c)
				unsetSchemaEnv(c)
				c.Setenv("PTAH_SCHEMA", value.value)

				out, err := runSchemaValueCommand(c, root, verb.args)

				c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("PTAH_SCHEMA is "+value.state+
					": give it a value, or unset it and leave --schema out to "+verb.omitted))
				c.Assert(out, qt.Equals, "")
			})
		}
	}
}

// TestAtlasMigrateCheckpointSchemasEnvironmentEmpty_FailurePath is the native
// variable `migrate checkpoint` reads when PTAH_SCHEMA is absent: naming no
// schema, it is refused, and the refusal names --schemas, the flag the
// variable is named for.
func TestAtlasMigrateCheckpointSchemasEnvironmentEmpty_FailurePath(t *testing.T) {
	for _, value := range namelessSchemaValues {
		t.Run(strconv.Quote(value.value), func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)
			unsetSchemaEnv(c)
			c.Setenv("PTAH_SCHEMAS", value.value)

			out, err := runSchemaValueCommand(c, root, []string{"migrate", "checkpoint", "--dev-url", "sqlite://file?mode=memory"})

			c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("PTAH_SCHEMAS is "+value.state+
				": give it a value, or unset it and leave --schemas out to "+connectionDefault))
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestAtlasSchemaEnvironment_HappyPath holds the refusal to the empty value:
// a value selects the schema, an unset variable selects every schema, and the
// command line wins over an empty variable without reading it.
func TestAtlasSchemaEnvironment_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		args []string
	}{
		{name: "a value", env: map[string]string{"PTAH_SCHEMA": "main"}},
		{name: "unset"},
		{name: "the command line over an empty variable", env: map[string]string{"PTAH_SCHEMA": ""}, args: []string{"-s", "main"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)
			unsetSchemaEnv(c)
			_, err := runSchemaValueCommand(c, root, []string{
				"schema", "apply", "-u", "sqlite://dev.db", "--to", "file://schema.sql",
				"--dev-url", "sqlite://file?mode=memory", "--auto-approve",
			})
			c.Assert(err, qt.IsNil)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			out, err := runSchemaValueCommand(c, root, append([]string{"schema", "inspect", "-u", "sqlite://dev.db"}, test.args...))

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `table "t" {`)
		})
	}
}
