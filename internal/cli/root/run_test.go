package root_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/cli/root"
)

// newTestCommand returns a command whose RunE the caller supplies, wired so
// nothing it writes reaches the test binary's own streams.
func newTestCommand(run func(cmd *cobra.Command, args []string) error) (*cobra.Command, *bytes.Buffer) {
	var out bytes.Buffer
	cmd := &cobra.Command{Use: "probe", RunE: run, SilenceUsage: true, SilenceErrors: true}
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	return cmd, &out
}

func TestRunContextReportsTheExitCodeContract(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "success is zero", err: nil, want: 0},
		{name: "a declared code is honored", err: exitcode.New(1, errors.New("drift detected")), want: 1},
		{name: "an undeclared error falls back to two", err: errors.New("boom"), want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd, _ := newTestCommand(func(*cobra.Command, []string) error { return tt.err })

			code := root.RunContext(context.Background(), cmd)

			c.Assert(code, qt.Equals, tt.want)
		})
	}
}

func TestRunContextRecoversAPanicWithoutEndingTheProcess(t *testing.T) {
	c := qt.New(t)
	cmd, out := newTestCommand(func(*cobra.Command, []string) error { panic("deliberate") })

	code := root.RunContext(context.Background(), cmd)

	c.Assert(code, qt.Equals, 2)
	c.Assert(out.String(), qt.Contains, "internal error")
	c.Assert(out.String(), qt.Contains, "deliberate")
}

// TestRunContextHandsTheCommandTheCallersContext is the property a host with no
// signals depends on: cancelation arrives through the context it passed, not
// through SIGINT, which nothing in a browser ever sends.
func TestRunContextHandsTheCommandTheCallersContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var seen error
	cmd, _ := newTestCommand(func(cmd *cobra.Command, _ []string) error {
		seen = cmd.Context().Err()
		return nil
	})

	code := root.RunContext(ctx, cmd)

	c.Assert(code, qt.Equals, 0)
	c.Assert(seen, qt.ErrorIs, context.Canceled)
}

func TestRunContextPassesTheArgumentsItIsGiven(t *testing.T) {
	c := qt.New(t)
	var seen []string
	cmd, _ := newTestCommand(func(_ *cobra.Command, args []string) error {
		seen = args
		return nil
	})
	cmd.Args = cobra.ArbitraryArgs

	code := root.RunContext(context.Background(), cmd, "one", "two")

	c.Assert(code, qt.Equals, 0)
	c.Assert(seen, qt.DeepEquals, []string{"one", "two"})
}
