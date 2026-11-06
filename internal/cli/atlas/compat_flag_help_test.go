package atlas_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// TestCompatHelpPrintsBooleanFlagsAsAtlasCE holds the help to the pinned
// community binary v1.3.0, which writes a boolean flag as `--dry-run` and its
// help flag as `-h, --help`. pflag v1.1.0 writes each as `--name[=true|false]`.
// The help command row reaches a --help flag cobra adds only for that call.
func TestCompatHelpPrintsBooleanFlagsAsAtlasCE(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantLine string
	}{
		{name: "a verb's own boolean flag", args: []string{"schema", "apply", "--help"}, wantLine: "      --dry-run "},
		{name: "a verb's --help", args: []string{"schema", "apply", "--help"}, wantLine: "  -h, --help "},
		{name: "the help command", args: []string{"help", "schema", "apply"}, wantLine: "  -h, --help "},
		{name: "a forwarded verb", args: []string{"migrate", "checkpoint", "--help"}, wantLine: "      --edit "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, err := atlastest.RunCompat(tt.args...)

			c.Assert(err, qt.IsNil)
			c.Assert(stderr, qt.Equals, "")
			c.Assert(stdout, qt.Contains, tt.wantLine)
			c.Assert(stdout, qt.Not(qt.Contains), "[=true|false]")
		})
	}
}

// TestCompatStrictProfileRefusesFlagValuesAsAtlasCE is the strict profile's
// half of TestCompatLockTimeout_FailurePath: the conformance runs measure this
// profile, so the standard library's wording has to reach it too.
func TestCompatStrictProfileRefusesFlagValuesAsAtlasCE(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{
			name:       "a duration",
			args:       []string{"schema", "apply", "--lock-timeout", "soon"},
			wantStderr: "Error: invalid argument \"soon\" for \"--lock-timeout\" flag: time: invalid duration \"soon\"\n",
		},
		{
			name: "a boolean",
			args: []string{"schema", "apply", "--auto-approve=maybe"},
			wantStderr: "Error: invalid argument \"maybe\" for \"--auto-approve\" flag: " +
				"strconv.ParseBool: parsing \"maybe\": invalid syntax\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout.String(), qt.Equals, "")
			c.Assert(stderr.String(), qt.Equals, tt.wantStderr)
		})
	}
}
