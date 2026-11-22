package atlas_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/envbool/envbooltest"
)

// checkScanVar is the variable this surface reads where the native commands
// take --skip-check-scan.
const checkScanVar = "PTAH_SKIP_CHECK_SCAN"

// Each verb that plans resolves the variable before its first early return, so
// a malformed value fails the run before anything else is looked at: each row
// would otherwise fail on a missing flag, as TestCompatPlanningVerbsFailOnTheirOwnFirst
// shows.
func TestCompatRefusesAMalformedCheckScanVariableBeforeAnyWork(t *testing.T) {
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
			envbooltest.Set(checkScanVar, "maybe")(t)

			stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: invalid boolean value \"maybe\" for PTAH_SKIP_CHECK_SCAN\n")
		})
	}
}

// The control for the rows above: a valid value is read and each verb goes on
// to fail on its own first check.
func TestCompatPlanningVerbsTakeAValidCheckScanVariable(t *testing.T) {
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
			envbooltest.Set(checkScanVar, "1")(t)

			stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.want)
		})
	}
}

// A verb that plans nothing does not read the variable, so a malformed value
// leaves it to fail on its own first check.
func TestCompatSchemaInspectDoesNotReadTheCheckScanVariable(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(checkScanVar, "maybe")(t)

	stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "inspect")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: required flag(s) \"url\" not set\n")
}

// The scans are Ptah's own statements, on an engine the pinned community binary
// has no driver for, so the strict profile refuses the variable as it refuses
// every gated PTAH_* toggle, at the process boundary and before any command
// runs.
func TestStrictCompatRefusesTheCheckScanVariable(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "asked for", value: "1", wantErr: "PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_SKIP_CHECK_SCAN"},
		{name: "malformed", value: "maybe", wantErr: `invalid boolean value "maybe" for PTAH_SKIP_CHECK_SCAN`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(atlascompatpolicy.StrictCompatEnvVar, "1")(t)
			envbooltest.Set(checkScanVar, test.value)(t)

			policy, err := atlascompatpolicy.Resolve()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(policy.IsStrictCE(), qt.IsFalse)
		})
	}
}

// The control for the strict rows: false asks for nothing, so it is kept, and
// strict mode is selected.
func TestStrictCompatKeepsAFalseCheckScanVariable(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(atlascompatpolicy.StrictCompatEnvVar, "1")(t)
	envbooltest.Set(checkScanVar, "0")(t)

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsTrue)
}
