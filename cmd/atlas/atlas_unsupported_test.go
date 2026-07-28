package atlas_test

import (
	"bytes"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func TestNewAtlasCommand_UnsupportedCommunityCommands(t *testing.T) {
	c := qt.New(t)

	tests := unsupportedCommunityCommandTests()

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			cmd := atlas.NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.path)

			err := cmd.Execute()

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(out.String(), qt.Equals, unsupportedCommunityAbortOutput(test.path))
		})
	}
}

func TestNewAtlasCommand_UnsupportedCommunityCommandsHelp(t *testing.T) {
	c := qt.New(t)

	tests := unsupportedCommunityCommandTests()

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			cmd := atlas.NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(append([]string{}, test.path...), "--help"))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, unsupportedCommunityHelpOutput(test.path))
		})
	}
}

func TestNewCompatCommand_UnsupportedCommunityCommands(t *testing.T) {
	c := qt.New(t)

	tests := unsupportedCommunityCommandTests()

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.path)

			err := cmd.Execute()

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(out.String(), qt.Equals, unsupportedCommunityAbortOutput(test.path))
		})
	}
}

func TestNewCompatCommand_UnsupportedCommunityCommandsHelp(t *testing.T) {
	c := qt.New(t)

	tests := unsupportedCommunityCommandTests()

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(append([]string{}, test.path...), "--help"))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, unsupportedCommunityHelpOutput(test.path))
		})
	}
}

type unsupportedCommunityCommandTest struct {
	name string
	path []string
}

// unsupportedCommunityCommandTests lists the Atlas verbs that remain
// deliberate Atlas CE unsupported-boundary stubs. `migrate test`,
// `schema test`, `migrate edit`, `migrate rebase`, `migrate rm`, and
// `schema plan` are no longer here: they forward to or implement native Ptah
// behavior (see migrate_test_forward_test.go, schema_test_forward_test.go,
// migrate_maint_forward_test.go, and schema_plan_test.go). The registry
// sub-verbs under `schema plan` stay stubs: they operate on plans stored in
// the Atlas Registry, which Ptah's local plan-file workflow replaces.
func unsupportedCommunityCommandTests() []unsupportedCommunityCommandTest {
	return []unsupportedCommunityCommandTest{
		{name: "migrate_push", path: []string{"migrate", "push"}},
		{name: "schema_plan_approve", path: []string{"schema", "plan", "approve"}},
		{name: "schema_plan_lint", path: []string{"schema", "plan", "lint"}},
		{name: "schema_plan_list", path: []string{"schema", "plan", "list"}},
		{name: "schema_plan_new", path: []string{"schema", "plan", "new"}},
		{name: "schema_plan_pull", path: []string{"schema", "plan", "pull"}},
		{name: "schema_plan_push", path: []string{"schema", "plan", "push"}},
		{name: "schema_plan_rm", path: []string{"schema", "plan", "rm"}},
		{name: "schema_plan_test", path: []string{"schema", "plan", "test"}},
		{name: "schema_plan_validate", path: []string{"schema", "plan", "validate"}},
		{name: "schema_push", path: []string{"schema", "push"}},
	}
}

func unsupportedCommunityHelpOutput(path []string) string {
	return unsupportedCommunityNoticeOutput(path, "")
}

func unsupportedCommunityAbortOutput(path []string) string {
	return unsupportedCommunityNoticeOutput(path, "Abort: ") + `
You're running the community build of Atlas, which differs from the official version.
If this error persists, try installing the official version as a troubleshooting step:

  curl -sSf https://atlasgo.sh | sh

More installation options: https://atlasgo.io/docs#installation
`
}

func unsupportedCommunityNoticeOutput(path []string, prefix string) string {
	return prefix + unsupportedCommunityMessage(path) + `

To install the non-community version of Atlas, use the following command:

	curl -sSf https://atlasgo.sh | sh

Or, visit the website to see all installation options:

	https://atlasgo.io/docs#installation
`
}

func unsupportedCommunityMessage(path []string) string {
	return "'" + strings.Join(append([]string{"atlas"}, path...), " ") + "' is not supported by the community version."
}
