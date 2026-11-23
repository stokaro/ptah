package atlas_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
)

// spacedSchemaValues are -s/--schema values holding a name with white space
// around it, none of which names the schema `main` on the strict tree.
var spacedSchemaValues = []string{" main", "main ", "\tmain"}

// writeMainSchemaDocument writes schema.hcl to root, a document declaring the
// schema `main` and a table in it.
func writeMainSchemaDocument(c *qt.C, root string) {
	c.Helper()
	body := "schema \"main\" {}\ntable \"t\" {\n  schema = schema.main\n  column \"id\" {\n    type = integer\n  }\n}\n"
	c.Assert(os.WriteFile(filepath.Join(root, "schema.hcl"), []byte(body), 0o600), qt.IsNil)
}

// mainDocumentInspect is `schema inspect` of the document
// [writeMainSchemaDocument] writes.
var mainDocumentInspect = []string{"schema", "inspect", "-u", "file://schema.hcl", "--dev-url", "sqlite://file?mode=memory"}

// TestAtlasStrictSchemaNameWithSpace_FailurePath pins the strict reading of a
// name with white space around it. The community binary keeps the space, so
// ` main` is a schema of its own, and a document declaring `main` declares a
// schema the run did not request. The strict tree keeps the space too and
// refuses the document in that binary's words, from the flag and from an
// `env { schemas }` element alike.
func TestAtlasStrictSchemaNameWithSpace_FailurePath(t *testing.T) {
	for _, value := range spacedSchemaValues {
		t.Run(fmt.Sprintf("flag %q", value), func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			writeMainSchemaDocument(c, root)

			out, err := runStrictSchemaValueCommand(c, root, slices.Concat(mainDocumentInspect, []string{"--schema", value}))

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
				`schema "main" from paths ["schema.hcl"] is not requested (all schemas in HCL must be requested)`))
			c.Assert(out, qt.Equals, "")
		})
		t.Run(fmt.Sprintf("project file %q", value), func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			writeMainSchemaDocument(c, root)
			writeSchemaValueProjectWith(c, root, value)

			out, err := runStrictSchemaValueCommand(c, root, slices.Concat(mainDocumentInspect, []string{"-c", "file://atlas.hcl", "--env", "local"}))

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
				`schema "main" from paths ["schema.hcl"] is not requested (all schemas in HCL must be requested)`))
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestAtlasSchemaNameWithSpace_HappyPath is the inverse. The default tree trims
// a name, so ` main` is `main` there and the document is described. The strict
// tree describes it for a name with no white space around it, a quoted name,
// and a field that is blank beside a name, which selects nothing on either
// binary.
func TestAtlasSchemaNameWithSpace_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		policy atlascompatpolicy.Policy
		value  string
	}{
		{name: "default tree, leading space", policy: atlascompatpolicy.Full(), value: " main"},
		{name: "default tree, trailing space", policy: atlascompatpolicy.Full(), value: "main "},
		{name: "default tree, a list with a space", policy: atlascompatpolicy.Full(), value: "main, other"},
		{name: "strict tree, a plain name", policy: atlascompatpolicy.StrictCE(), value: "main"},
		{name: "strict tree, a blank field beside a name", policy: atlascompatpolicy.StrictCE(), value: "main, "},
		{name: "strict tree, a quoted name", policy: atlascompatpolicy.StrictCE(), value: `"main"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			writeMainSchemaDocument(c, root)

			out, err := runSchemaValueCommandWithPolicy(c, root, test.policy, slices.Concat(mainDocumentInspect, []string{"--schema", test.value}))

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "schema \"main\" {\n}")
			c.Assert(out, qt.Contains, "schema = schema.main")
		})
	}
}

// The white space before a quoted field is not skipped on either tree: the
// community binary refuses `main, " main"` in the reader's words, and so does
// this flag.
func TestAtlasSchemaSpaceBeforeAQuote_FailurePath(t *testing.T) {
	for _, policy := range []atlascompatpolicy.Policy{atlascompatpolicy.Full(), atlascompatpolicy.StrictCE()} {
		t.Run(fmt.Sprintf("strict %t", policy.IsStrictCE()), func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			writeMainSchemaDocument(c, root)

			out, err := runSchemaValueCommandWithPolicy(c, root, policy, slices.Concat(mainDocumentInspect, []string{"--schema", `main, " main"`}))

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`invalid argument "main, \" main\"" for "-s, --schema" flag: parse error on line 1, column 7: bare " in non-quoted-field`))
			c.Assert(out, qt.Equals, "")
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
