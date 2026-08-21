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
	tests := unsupportedCommandTests()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
	tests := unsupportedCommandTests()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(append(make([]string, 0), test.path...), "--help"))

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
// deliberate unsupported-boundary stubs. `migrate test`, `schema test`,
// `migrate edit`, `migrate rebase`, `migrate rm`, `schema plan`,
// `schema plan new`, `schema plan validate` and `schema plan lint` are no
// longer here: they forward to or implement native Ptah behavior (see
// migrate_test_forward_test.go, schema_test_forward_test.go,
// migrate_maint_forward_test.go, schema_plan_test.go,
// schema_plan_new_test.go, schema_plan_validate_test.go and
// schema_plan_lint_test.go).
//
// The remaining `schema plan` sub-verbs stay stubs for two different reasons.
// approve, list, pull, push and rm arbitrate plan state in a remote registry,
// which Ptah's local plan-file workflow replaces. test is local by its Atlas
// flag set and stays deferred for a reason of its own: it consumes `.test.hcl`
// case files that nothing in this repository parses yet.
func unsupportedCommandTests() []unsupportedCommandTest {
	return []unsupportedCommandTest{
		{name: "migrate_push", path: []string{"migrate", "push"}},
		{name: "schema_plan_approve", path: []string{"schema", "plan", "approve"}},
		{name: "schema_plan_list", path: []string{"schema", "plan", "list"}},
		{name: "schema_plan_pull", path: []string{"schema", "plan", "pull"}},
		{name: "schema_plan_push", path: []string{"schema", "plan", "push"}},
		{name: "schema_plan_rm", path: []string{"schema", "plan", "rm"}},
		// `schema plan test` left this list in stokaro/ptah#1211: it takes no
		// --url and is entirely local, so nothing about it depended on the
		// registry the rest of these do. It is covered by
		// TestSchemaPlanTest_RunsAPlanCaseEndToEnd.
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
