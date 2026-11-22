package atlas_test

import (
	"bytes"
	"fmt"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
)

// strictSchemaVerbs are the verbs the strict tree keeps that read -s/--schema.
var strictSchemaVerbs = []struct {
	name string
	args []string
}{
	{name: "schema inspect", args: []string{"schema", "inspect", "-u", "sqlite://dev.db"}},
	{name: "schema apply", args: []string{"schema", "apply", "-u", "sqlite://dev.db", "--to", "file://schema.sql"}},
	{name: "schema diff", args: []string{"schema", "diff", "--from", "sqlite://dev.db", "--to", "file://schema.sql"}},
	{name: "migrate diff", args: []string{"migrate", "diff", "--to", "file://schema.sql", "--dev-url", "sqlite://file?mode=memory"}},
}

// spacedSchemaValues are -s/--schema values holding a name with white space
// around it, each with the name the refusal reports.
var spacedSchemaValues = []struct {
	value string
	name  string
}{
	{value: " main", name: " main"},
	{value: "main ", name: "main "},
	{value: "main, other", name: " other"},
	{value: "\tmain", name: "\tmain"},
}

// TestAtlasStrictSchemaNameWithSpace_FailurePath pins the strict reading of a
// name with white space around it. The community binary keeps the space, so
// ` main` is a schema of its own and `main, other` names ` other`; Ptah trims
// every schema name below the flag and would select the schema without the
// space, which is another one. So the strict tree refuses the value rather than
// read it as a different schema.
func TestAtlasStrictSchemaNameWithSpace_FailurePath(t *testing.T) {
	for _, verb := range strictSchemaVerbs {
		for _, value := range spacedSchemaValues {
			t.Run(verb.name+" "+fmt.Sprintf("%q", value.value), func(t *testing.T) {
				c := qt.New(t)
				root := writeSchemaValueFixture(c)

				out, err := runStrictSchemaValueCommand(c, root, slices.Concat(verb.args, []string{"--schema", value.value}))

				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(fmt.Sprintf(
					`invalid argument %q for "-s, --schema" flag: schema %q begins or ends with white space, which the community binary keeps and Ptah cannot select`,
					value.value, value.name)))
				c.Assert(out, qt.Equals, "")
			})
		}
	}
}

// TestAtlasStrictConfiguredSchemaNameWithSpace_FailurePath is the project-file
// half: an `env { schemas }` element takes the flag's grammar, and the refusal,
// being Ptah's own, names the file and the env.
func TestAtlasStrictConfiguredSchemaNameWithSpace_FailurePath(t *testing.T) {
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
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)
			writeSchemaValueProjectWith(c, root, " main")

			out, err := runStrictSchemaValueCommand(c, root, verb.args)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
				`atlas.hcl: env "local": schemas value " main": schema " main" begins or ends with white space, which the community binary keeps and Ptah cannot select`))
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestAtlasSchemaNameWithSpace_HappyPath pins what the strict reading leaves
// alone. The default tree trims a name, so ` main` is `main` there. The strict
// tree accepts a name with no white space around it, and a field that is blank
// beside a name, which selects nothing on either binary.
func TestAtlasSchemaNameWithSpace_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		policy atlascompatpolicy.Policy
		value  string
	}{
		{name: "default tree, leading space", policy: atlascompatpolicy.Full(), value: " main"},
		{name: "default tree, a list with a space", policy: atlascompatpolicy.Full(), value: "main, other"},
		{name: "strict tree, a plain name", policy: atlascompatpolicy.StrictCE(), value: "main"},
		{name: "strict tree, a blank field beside a name", policy: atlascompatpolicy.StrictCE(), value: "main, "},
		{name: "strict tree, a quoted name", policy: atlascompatpolicy.StrictCE(), value: `"main"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeSchemaValueFixture(c)

			_, err := runSchemaValueCommandWithPolicy(c, root, test.policy, []string{"schema", "inspect", "-u", "sqlite://dev.db", "--schema", test.value})

			c.Assert(err, qt.IsNil)
		})
	}
}

// runStrictSchemaValueCommand runs the strict compatibility tree in root and
// returns what it wrote to stdout.
func runStrictSchemaValueCommand(c *qt.C, root string, args []string) (string, error) {
	c.Helper()
	return runSchemaValueCommandWithPolicy(c, root, atlascompatpolicy.StrictCE(), args)
}

// runSchemaValueCommandWithPolicy runs the compatibility tree built for policy
// in root and returns what it wrote to stdout.
func runSchemaValueCommandWithPolicy(c *qt.C, root string, policy atlascompatpolicy.Policy, args []string) (string, error) {
	c.Helper()
	c.Chdir(root)
	cmd := atlas.NewCompatCommandWithPolicy("atlas", policy)
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}
