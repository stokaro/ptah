package atlas_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/envbool/envbooltest"
)

// tableRebuildVar is the variable this surface reads where the native commands
// take --allow-table-rebuild.
const tableRebuildVar = "PTAH_ALLOW_TABLE_REBUILD"

// Each verb that plans resolves the variable before its first early return, so
// a malformed value fails the run before anything else is looked at: each row
// would otherwise fail on a missing flag, as TestCompatPlanningVerbsFailOnTheirOwnFirst
// shows.
func TestCompatRefusesAMalformedRebuildVariableBeforeAnyWork(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema apply", args: []string{"schema", "apply"}},
		{name: "schema diff", args: []string{"schema", "diff"}},
		{name: "schema plan new", args: []string{"schema", "plan", "new"}},
		{name: "migrate diff", args: []string{"migrate", "diff"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(tableRebuildVar, "maybe")(t)

			stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: invalid boolean value \"maybe\" for PTAH_ALLOW_TABLE_REBUILD\n")
		})
	}
}

// The control for the rows above: with the variable unset each verb fails on
// its own first check.
func TestCompatPlanningVerbsFailOnTheirOwnFirst(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "schema apply", args: []string{"schema", "apply"}, want: "Error: required flag(s) \"url\" not set\n"},
		{name: "schema diff", args: []string{"schema", "diff"}, want: "Error: --from is required\n"},
		{name: "schema plan new", args: []string{"schema", "plan", "new"}, want: "Error: --from is required\n"},
		{name: "migrate diff", args: []string{"migrate", "diff"}, want: "Error: --to is required\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(tableRebuildVar)(t)

			stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.want)
		})
	}
}

// A verb that plans nothing does not read the variable, so a malformed value
// leaves it to fail on its own first check.
func TestCompatSchemaInspectDoesNotReadTheRebuildVariable(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(tableRebuildVar, "maybe")(t)

	stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "inspect")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: required flag(s) \"url\" not set\n")
}

// The pinned community binary plans no table rebuild, so the strict profile
// refuses the variable as it refuses every gated PTAH_* toggle, at the process
// boundary and before any command runs; a valid false asks for nothing and is
// kept.
func TestStrictCompatRefusesTheRebuildVariable(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "asked for", value: "1",
			wantErr: "PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ALLOW_TABLE_REBUILD"},
		{name: "malformed", value: "maybe",
			wantErr: `invalid boolean value "maybe" for PTAH_ALLOW_TABLE_REBUILD`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(atlascompatpolicy.StrictCompatEnvVar, "1")(t)
			envbooltest.Set(tableRebuildVar, test.value)(t)

			policy, err := atlascompatpolicy.Resolve()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(policy.IsStrictCE(), qt.IsFalse)
		})
	}
}

// The control for the strict rows: false is kept, and strict mode is selected.
func TestStrictCompatKeepsAFalseRebuildVariable(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(atlascompatpolicy.StrictCompatEnvVar, "1")(t)
	envbooltest.Set(tableRebuildVar, "0")(t)

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsTrue)
}
