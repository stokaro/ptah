package atlas_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/migratecheckpoint"
	"ptah.run/internal/cmdforward"
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

// schemaEmptyValueArgs are the spellings of a --schema value that names no
// schema, each with the value pflag reports for it.
var schemaEmptyValueArgs = []struct {
	name  string
	args  []string
	value string
}{
	{name: `""`, args: []string{"--schema", `""`}, value: `""`},
	{name: `,`, args: []string{"--schema", `,`}, value: `,`},
	{name: `" "`, args: []string{"--schema", `" "`}, value: `" "`},
	{name: `"",""`, args: []string{"--schema", `"",""`}, value: `"",""`},
	{name: "an empty argument", args: []string{"--schema", ""}, value: ""},
	{name: "an empty argument to -s", args: []string{"-s", ""}, value: ""},
	{name: "--schema=", args: []string{"--schema="}, value: ""},
}

// TestAtlasSchemaEmptyValue_FailurePath pins that a --schema value naming no
// schema is refused on every verb that reads the flag: the empty value, and a
// value whose every field is empty. Read as no name, it would leave the run with
// no selection, which is the whole realm, and `schema apply` would plan a DROP
// for every schema the desired schema leaves out. The pinned binary plans those
// DROPs for the empty value, so refusing it is stricter on purpose.
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

	for _, verb := range verbs {
		for _, value := range schemaEmptyValueArgs {
			t.Run(verb.name+" "+value.name, func(t *testing.T) {
				c := qt.New(t)
				root := writeSchemaValueFixture(c)

				out, err := runSchemaValueCommand(c, root, slices.Concat(verb.args, value.args))

				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(fmt.Sprintf(
					`invalid argument %q for "-s, --schema" flag: every field is empty, so the value names no schema`, value.value)))
				c.Assert(out, qt.Equals, "")
			})
		}
	}
}

// TestAtlasConfiguredSchemaEmptyValue_FailurePath is the project-file half: an
// `env { schemas }` element goes through the flag's grammar, so an element
// naming no schema is refused there too. The refusal is Ptah's own, so it names
// the file and the env rather than the flag.
func TestAtlasConfiguredSchemaEmptyValue_FailurePath(t *testing.T) {
	verbs := []struct {
		name string
		args []string
	}{
		{name: "schema inspect", args: []string{"schema", "inspect", "-c", "file://atlas.hcl", "--env", "local"}},
		{name: "schema apply", args: []string{"schema", "apply", "-c", "file://atlas.hcl", "--env", "local", "--to", "file://schema.sql"}},
		{name: "schema diff", args: []string{"schema", "diff", "-c", "file://atlas.hcl", "--env", "local", "--from", "sqlite://dev.db", "--to", "file://schema.sql"}},
		{name: "migrate diff", args: []string{"migrate", "diff", "-c", "file://atlas.hcl", "--env", "local", "--to", "file://schema.sql"}},
	}

	for _, verb := range verbs {
		for _, value := range []string{"", `""`, `,`} {
			t.Run(verb.name+" "+strconv.Quote(value), func(t *testing.T) {
				c := qt.New(t)
				root := writeSchemaValueFixture(c)
				writeSchemaValueProjectWith(c, root, value)

				out, err := runSchemaValueCommand(c, root, verb.args)

				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(fmt.Sprintf(
					`atlas.hcl: env "local": schemas value %q: every field is empty, so the value names no schema`, value)))
				c.Assert(out, qt.Equals, "")
			})
		}
	}
}

// TestAtlasConfiguredSchemaEmptyValueNamesTheFile_FailurePath pins that the
// refusal names the project file that declares the env, not atlas.hcl: a run
// whose --config points elsewhere is told which file to fix.
func TestAtlasConfiguredSchemaEmptyValueNamesTheFile_FailurePath(t *testing.T) {
	c := qt.New(t)
	root := writeSchemaValueFixture(c)
	c.Assert(os.Mkdir(filepath.Join(root, "cfgdir"), 0o700), qt.IsNil)
	body := fmt.Sprintf("env \"local\" {\n  url = %q\n  schemas = [%q]\n}\n", "sqlite://dev.db", "")
	c.Assert(os.WriteFile(filepath.Join(root, "cfgdir", "custom.hcl"), []byte(body), 0o600), qt.IsNil)

	out, err := runSchemaValueCommand(c, root, []string{"schema", "inspect", "-c", "file://cfgdir/custom.hcl", "--env", "local"})

	c.Assert(err, qt.ErrorMatches, `.*custom\.hcl: env "local": schemas value "": every field is empty, so the value names no schema`)
	c.Assert(out, qt.Equals, "")
}

// TestAtlasSchemaEnvironmentEmptyValue_FailurePath is the environment half:
// PTAH_SCHEMA sets the flag, so a value naming no schema is refused there too.
func TestAtlasSchemaEnvironmentEmptyValue_FailurePath(t *testing.T) {
	c := qt.New(t)
	root := writeSchemaValueFixture(c)
	c.Setenv("PTAH_SCHEMA", `""`)

	out, err := runSchemaValueCommand(c, root, []string{"schema", "inspect", "-u", "sqlite://dev.db"})

	c.Assert(err, qt.ErrorMatches, `.*every field is empty, so the value names no schema`)
	c.Assert(out, qt.Equals, "")
}

// TestAtlasSchemaValueKeepsEmptyFieldsBesideAName_HappyPath pins the other side
// of the refusal: an empty field beside a name is accepted as the pinned binary
// accepts it.
func TestAtlasSchemaValueKeepsEmptyFieldsBesideAName_HappyPath(t *testing.T) {
	for _, value := range []string{`main,`, `,main`, `"",main`} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)

			_, err := runSchemaValueCommand(c, root, []string{"schema", "inspect", "-u", "sqlite://dev.db", "--schema", value})

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestAtlasSchemaLeftOut_HappyPath pins that no selection at all is still the
// whole realm: a run that leaves the flag out, or whose env sets no list or an
// empty one, is not refused and describes the database's tables.
func TestAtlasSchemaLeftOut_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		project string
		args    []string
	}{
		{name: "no flag", args: []string{"schema", "inspect", "-u", "sqlite://dev.db"}},
		{
			name:    "an env without schemas",
			project: "env \"local\" {\n  url = \"sqlite://dev.db\"\n}\n",
			args:    []string{"schema", "inspect", "-c", "file://atlas.hcl", "--env", "local"},
		},
		{
			name:    "an env with an empty schemas list",
			project: "env \"local\" {\n  url = \"sqlite://dev.db\"\n  schemas = []\n}\n",
			args:    []string{"schema", "inspect", "-c", "file://atlas.hcl", "--env", "local"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)
			_, err := runSchemaValueCommand(c, root, []string{
				"schema", "apply", "-u", "sqlite://dev.db", "--to", "file://schema.sql",
				"--dev-url", "sqlite://file?mode=memory", "--auto-approve",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(test.project), 0o600), qt.IsNil)

			out, err := runSchemaValueCommand(c, root, test.args)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `table "t" {`)
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
		name    string
		args    []string
		wantErr string
	}{
		{name: `a"b`, args: []string{"--schema", `a"b`}, wantErr: schemaValueRefusal},
		{name: `""`, args: []string{"--schema", `""`}, wantErr: `invalid argument "\"\"" for "-s, --schema" flag: every field is empty, so the value names no schema`},
		{name: "an empty argument", args: []string{"--schema", ""}, wantErr: `invalid argument "" for "-s, --schema" flag: every field is empty, so the value names no schema`},
		{name: "--schema=", args: []string{"--schema="}, wantErr: `invalid argument "" for "-s, --schema" flag: every field is empty, so the value names no schema`},
		{name: `"a,b"`, args: []string{"--schema", `"a,b"`}, wantErr: `schema "a,b" holds a comma, and this command passes its schema names on as one comma-separated list`},
	}

	for _, verb := range verbs {
		for _, value := range values {
			t.Run(verb.name+" "+value.name, func(t *testing.T) {
				c := qt.New(t)
				root := writeSchemaValueFixture(c)

				out, err := runSchemaValueCommand(c, root, slices.Concat(verb.args, value.args))

				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("atlas "+verb.name+" --schema: "+value.wantErr))
				c.Assert(out, qt.Equals, "")
			})
		}
	}
}

// TestAtlasForwardedSchemaEnvironment_FailurePath pins that the variables which
// reach a forwarded --schema take its grammar. On `migrate checkpoint` the
// native flag is --schemas, whose own PTAH_SCHEMAS the native command would
// split on every comma; the adapter reads it in its place, so the value is
// refused as the flag refuses it. An absent PTAH_SCHEMA leaves PTAH_SCHEMAS to
// decide.
func TestAtlasForwardedSchemaEnvironment_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string
	}{
		{
			name:    "migrate checkpoint PTAH_SCHEMAS holding a comma",
			args:    []string{"migrate", "checkpoint", "--dev-url", "sqlite://file?mode=memory"},
			env:     map[string]string{"PTAH_SCHEMAS": `"a,b"`},
			wantErr: `atlas migrate checkpoint --schema: schema "a,b" holds a comma, and this command passes its schema names on as one comma-separated list`,
		},
		{
			name:    "migrate checkpoint PTAH_SCHEMAS naming no schema",
			args:    []string{"migrate", "checkpoint", "--dev-url", "sqlite://file?mode=memory"},
			env:     map[string]string{"PTAH_SCHEMAS": `""`},
			wantErr: `atlas migrate checkpoint --schema: invalid argument "\"\"" for "-s, --schema" flag: every field is empty, so the value names no schema`,
		},
		{
			name:    "migrate checkpoint PTAH_SCHEMA holding a comma",
			args:    []string{"migrate", "checkpoint", "--dev-url", "sqlite://file?mode=memory"},
			env:     map[string]string{"PTAH_SCHEMA": `"a,b"`},
			wantErr: `atlas migrate checkpoint --schema: schema "a,b" holds a comma, and this command passes its schema names on as one comma-separated list`,
		},
		{
			name:    "schema test PTAH_SCHEMA holding a comma",
			args:    []string{"schema", "test", "--dev-url", "sqlite://file?mode=memory"},
			env:     map[string]string{"PTAH_SCHEMA": `"a,b"`},
			wantErr: `atlas schema test --schema: schema "a,b" holds a comma, and this command passes its schema names on as one comma-separated list`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)
			unsetSchemaEnv(c)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			out, err := runSchemaValueCommand(c, root, test.args)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestAtlasSchemaFlagHelpPrintsNoDefault pins the --help line of -s/--schema
// on the verbs that read it as a string slice. The value behind the flag wraps
// the string slice, and pflag recognizes an empty default only on its own type,
// so without an empty default text the line ends in `(default [])`, which the
// pinned binary does not print.
func TestAtlasSchemaFlagHelpPrintsNoDefault(t *testing.T) {
	for _, verb := range [][]string{
		{"schema", "inspect"},
		{"schema", "apply"},
		{"schema", "diff"},
		{"schema", "plan"},
		{"schema", "plan", "new"},
		{"schema", "plan", "validate"},
		{"migrate", "diff"},
	} {
		t.Run(strings.Join(verb, " "), func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)

			out, err := runSchemaValueCommand(c, root, append(verb, "--help"))

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Matches, `(?s).*\n\s+-s, --schema strings\s+[^\n]*\[env: PTAH_SCHEMA\]\n.*`)
		})
	}
}

// TestAtlasMigrateCheckpointTargetReadsNoSchemasVariable pins the other half of
// TestAtlasForwardedSchemaEnvironment_FailurePath: the adapter reads
// PTAH_SCHEMAS, so the native command it forwards to does not bind it, and the
// value is read once. No run shows the difference, because the adapter forwards
// a value whenever the variable carries one; the target's binding does. The
// native command built on its own still binds the variable, which is what gives
// the first assertion its meaning.
func TestAtlasMigrateCheckpointTargetReadsNoSchemasVariable(t *testing.T) {
	c := qt.New(t)
	forwarder, _, err := atlas.NewCompatCommand("atlas").Find([]string{"migrate", "checkpoint"})
	c.Assert(err, qt.IsNil)

	target := cmdforward.Build(forwarder)

	c.Assert(target, qt.IsNotNil)
	schemas := target.Flags().Lookup("schemas")
	c.Assert(schemas, qt.IsNotNil)
	_, bound := cmdflags.EnvBindingName("PTAH", schemas)
	c.Assert(bound, qt.IsFalse)
	name, nativeBound := cmdflags.EnvBindingName("PTAH", migratecheckpoint.NewMigrateCheckpointCommand().Flags().Lookup("schemas"))
	c.Assert(nativeBound, qt.IsTrue)
	c.Assert(name, qt.Equals, "PTAH_SCHEMAS")
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
