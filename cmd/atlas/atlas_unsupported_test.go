package atlas_test

import (
	"bytes"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

func TestCompatCommand_UnsupportedCommands(t *testing.T) {
	c := qt.New(t)

	tests := unsupportedCommandTests()

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
			c.Assert(out.String(), qt.Equals, unsupportedCommandErrorOutput(test.path))
		})
	}
}

func TestCompatCommand_UnsupportedCommandsHelp(t *testing.T) {
	c := qt.New(t)

	tests := unsupportedCommandTests()

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(append([]string{}, test.path...), "--help"))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, unsupportedCommandHelpOutput(test.path))
		})
	}
}

type unsupportedCommandTest struct {
	name string
	path []string
}

// unsupportedCommandTests lists the compatibility verbs that remain
// deliberate unsupported-boundary stubs. `migrate test`,
// `schema test`, `migrate edit`, `migrate rebase`, `migrate rm`, and
// `schema plan` are no longer here: they forward to or implement native Ptah
// behavior (see migrate_test_forward_test.go, schema_test_forward_test.go,
// migrate_maint_forward_test.go, and schema_plan_test.go). The registry
// sub-verbs under `schema plan` stay stubs: they operate on plans stored in
// a remote registry, which Ptah's local plan-file workflow replaces.
func unsupportedCommandTests() []unsupportedCommandTest {
	return []unsupportedCommandTest{
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

func unsupportedCommandHelpOutput(path []string) string {
	return compatibilityCommandPath(path) + " is not implemented by Ptah.\n"
}

func unsupportedCommandErrorOutput(path []string) string {
	return "Error: " + compatibilityCommandPath(path) + " is not implemented by Ptah\n"
}

func compatibilityCommandPath(path []string) string {
	return strings.Join(append([]string{"atlas"}, path...), " ")
}
