package checkscan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/checkscan"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/envbool/envbooltest"
)

// probe is a command with the flag, its arguments parsed and its environment
// applied, as a command of the tree has them when it runs.
func probe(c *qt.C, args []string) (*cobra.Command, error) {
	c.Helper()
	cmd := &cobra.Command{Use: "probe"}
	checkscan.Register(cmd)
	c.Assert(cmd.Flags().Parse(args), qt.IsNil)
	return cmd, cmdflags.InitializeEnv("PTAH", cmd)
}

// Without the variable the flag alone decides.
func TestRequested_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "the flag", args: []string{"--skip-check-scan"}, want: true},
		{name: "neither", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset("PTAH_SKIP_CHECK_SCAN")(t)
			cmd, err := probe(c, test.args)
			c.Assert(err, qt.IsNil)

			got, err := checkscan.Requested(cmd)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The variable alone asks, a false one keeps the scans, and the flag on the
// command line wins over a false variable.
func TestRequested_HappyPathWithTheVariable(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		variable string
		want     bool
	}{
		{name: "the variable alone", variable: "1", want: true},
		{name: "a false variable", variable: "false", want: false},
		{name: "the flag over a false variable", args: []string{"--skip-check-scan"}, variable: "0", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set("PTAH_SKIP_CHECK_SCAN", test.variable)(t)
			cmd, err := probe(c, test.args)
			c.Assert(err, qt.IsNil)

			got, err := checkscan.Requested(cmd)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A boolean variable is strict: a value that does not parse is refused when
// the command's environment is applied, before the command does any work, and
// the flag stays unset.
func TestRequested_FailurePath(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set("PTAH_SKIP_CHECK_SCAN", "maybe")(t)

	cmd, err := probe(c, nil)
	got, requestErr := checkscan.Requested(cmd)

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for PTAH_SKIP_CHECK_SCAN`)
	c.Assert(requestErr, qt.IsNil)
	c.Assert(got, qt.IsFalse)
}
