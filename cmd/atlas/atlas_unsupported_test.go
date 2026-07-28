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
// `schema test`, `migrate edit`, `migrate rebase`, and `migrate rm` are no
// longer here: they forward to the native Ptah commands (see
// migrate_test_forward_test.go, schema_test_forward_test.go, and
// migrate_maint_forward_test.go).
func unsupportedCommunityCommandTests() []unsupportedCommunityCommandTest {
	return []unsupportedCommunityCommandTest{
		{name: "migrate_push", path: []string{"migrate", "push"}},
		{name: "schema_plan", path: []string{"schema", "plan"}},
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
