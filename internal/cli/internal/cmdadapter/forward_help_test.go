package cmdadapter_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdadapter"
)

// helpAdapterLong is the help text a forwarding command prints for itself.
const helpAdapterLong = "Forwards to `ptah target`."

// helpRun is what one invocation of a help adapter did.
type helpRun struct {
	err error
	out string
	// mapped holds the arguments the mapper received, nil when it never ran.
	mapped []string
	// ran reports whether the target's RunE ran.
	ran bool
}

// newHelpAdapter returns a forwarding command that answers help itself, with
// flags of the kinds pflag reads differently: a value flag with a shorthand, a
// boolean with a shorthand, an unsigned number, and a persistent flag of a
// parent, which the adapter inherits.
func newHelpAdapter(run *helpRun) *cobra.Command {
	target := &cobra.Command{
		Use:          "target",
		SilenceUsage: true,
		RunE: func(*cobra.Command, []string) error {
			run.ran = true
			return nil
		},
	}
	target.Flags().StringP("dir", "d", "file://migrations", "")
	target.Flags().BoolP("verbose", "v", false, "")
	target.Flags().Uint("count", 0, "")
	target.Flags().StringP("config", "c", "file://atlas.hcl", "")
	mapper := func(_ *cobra.Command, args []string, _ *cmdadapter.CleanupScope) ([]string, context.Context, error) {
		run.mapped = append(make([]string, 0, len(args)), args...)
		return args, nil, nil
	}
	adapter := cmdadapter.NewForwardCommandWithArgsMapper(
		"adapter", "Atlas adapter command", "target",
		func() *cobra.Command { return target },
		mapper,
	)
	adapter.Flags().StringP("dir", "d", "file://migrations", "")
	adapter.Flags().BoolP("verbose", "v", false, "")
	adapter.Flags().Uint("count", 0, "")
	root := &cobra.Command{Use: "atlas", SilenceUsage: true, SilenceErrors: true}
	root.PersistentFlags().StringP("config", "c", "file://atlas.hcl", "")
	root.AddCommand(adapter)
	return root
}

func runHelpAdapter(args ...string) helpRun {
	var run helpRun
	root := newHelpAdapter(&run)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(append([]string{"adapter"}, args...))
	run.err = root.Execute()
	run.out = out.String()
	return run
}

// TestForwardCommandHelp_HappyPath covers arguments that ask for help as pflag
// reads them and carry nothing pflag refuses: the adapter prints its own help
// and neither maps nor forwards anything.
func TestForwardCommandHelp_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "the shorthand", args: []string{"-h"}},
		{name: "the long form", args: []string{"--help"}},
		{name: "an explicit true", args: []string{"--help=true"}},
		{name: "a group of booleans", args: []string{"-vh"}},
		{name: "help with a positional", args: []string{"-h", "name"}},
		{name: "help after a value flag and its value", args: []string{"--dir", "file://x", "-h"}},
		{name: "help with an attached shorthand value", args: []string{"-h", "-cfoo"}},
		{name: "help with an inherited flag", args: []string{"--config", "file://other.hcl", "--help"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			run := runHelpAdapter(tt.args...)

			c.Assert(run.err, qt.IsNil)
			c.Assert(run.out, qt.Contains, helpAdapterLong)
			c.Assert(run.mapped, qt.IsNil)
			c.Assert(run.ran, qt.IsFalse)
		})
	}
}

// TestForwardCommandHelp_FailurePath covers arguments that ask for help and
// carry something pflag refuses. cobra parses every flag before it reads
// --help, so the refusal wins wherever it stands, and nothing is printed,
// mapped or forwarded. Each message is pflag's own.
func TestForwardCommandHelp_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "an unknown shorthand after help",
			args:    []string{"-h", "-x"},
			wantErr: `unknown shorthand flag: 'x' in -x`,
		},
		{
			name:    "an unknown shorthand before help",
			args:    []string{"-x", "-h"},
			wantErr: `unknown shorthand flag: 'x' in -x`,
		},
		{
			name:    "an unknown shorthand grouped with help",
			args:    []string{"-hx"},
			wantErr: `unknown shorthand flag: 'x' in -x`,
		},
		{
			name:    "an unknown long flag after help",
			args:    []string{"--help", "--nonexistent"},
			wantErr: `unknown flag: --nonexistent`,
		},
		{
			name:    "an unknown long flag and its value before help",
			args:    []string{"--nonexistent", "value", "--help"},
			wantErr: `unknown flag: --nonexistent`,
		},
		{
			name:    "a value flag with no value",
			args:    []string{"-h", "--dir"},
			wantErr: `flag needs an argument: --dir`,
		},
		{
			name:    "an inherited shorthand with no value",
			args:    []string{"-h", "-c"},
			wantErr: `flag needs an argument: 'c' in -c`,
		},
		{
			name:    "a malformed number after help",
			args:    []string{"-h", "--count=abc"},
			wantErr: `invalid argument "abc" for "--count" flag: strconv.ParseUint: parsing "abc": invalid syntax`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			run := runHelpAdapter(tt.args...)

			c.Assert(run.err, qt.ErrorMatches, regexp.QuoteMeta(tt.wantErr))
			c.Assert(run.out, qt.Equals, "")
			c.Assert(run.mapped, qt.IsNil)
			c.Assert(run.ran, qt.IsFalse)
		})
	}
}

// TestForwardCommandHelp_ForwardsWhatAsksForNoHelp covers arguments that hold
// a help spelling pflag does not read as a request: a value flag's value, a
// positional after `--`, and an explicit false. They reach the mapper as typed
// and the target runs.
func TestForwardCommandHelp_ForwardsWhatAsksForNoHelp(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "the value of a value flag", args: []string{"--dir", "-h"}},
		{name: "the attached value of a shorthand", args: []string{"-d-h"}},
		{name: "the value of an inherited value flag", args: []string{"--config", "-h"}},
		{name: "the value of an inherited shorthand", args: []string{"-c", "-h"}},
		{name: "an explicit false", args: []string{"--help=false"}},
		{name: "an explicit false on the shorthand", args: []string{"-h=false"}},
		{name: "a positional after the terminator", args: []string{"--", "-h"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			run := runHelpAdapter(tt.args...)

			c.Assert(run.err, qt.IsNil)
			c.Assert(run.out, qt.Not(qt.Contains), helpAdapterLong)
			c.Assert(run.mapped, qt.DeepEquals, tt.args)
			c.Assert(run.ran, qt.IsTrue)
		})
	}
}

// TestForwardCommandHelp_TargetRefusesAMalformedHelpValue covers
// `--help=maybe`, which asks for nothing because pflag refuses the value. It
// is forwarded, and the target refuses it with the standard library's error,
// as every other boolean on the target is refused.
func TestForwardCommandHelp_TargetRefusesAMalformedHelpValue(t *testing.T) {
	c := qt.New(t)

	run := runHelpAdapter("--help=maybe")

	c.Assert(run.err, qt.ErrorMatches, regexp.QuoteMeta(
		`invalid argument "maybe" for "-h, --help" flag: strconv.ParseBool: parsing "maybe": invalid syntax`,
	))
	c.Assert(run.mapped, qt.DeepEquals, []string{"--help=maybe"})
	c.Assert(run.ran, qt.IsFalse)
}

// TestForwardCommandHelp_RefusesThroughTheFlagErrorFunction covers where a
// refusal goes. cobra hands a parse error to the command's flag error
// function, which a tree uses to set its exit code and reset its flags, so the
// adapter hands its refusal to the same function.
func TestForwardCommandHelp_RefusesThroughTheFlagErrorFunction(t *testing.T) {
	c := qt.New(t)
	var run helpRun
	root := newHelpAdapter(&run)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return fmt.Errorf("flag error: %w", err)
	})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"adapter", "-h", "-x"})

	err := root.Execute()

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`flag error: unknown shorthand flag: 'x' in -x`))
	c.Assert(run.mapped, qt.IsNil)
	c.Assert(run.ran, qt.IsFalse)
}

// TestAnswersHelp_ReportsWhatTheAdapterAnswers covers the predicate a wrapper
// asks before doing work of its own: it agrees with the adapter on every row
// above, the refusals included, since the adapter answers those too.
func TestAnswersHelp_ReportsWhatTheAdapterAnswers(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "help", args: []string{"-h"}, want: true},
		{name: "help in a group", args: []string{"-vh"}, want: true},
		{name: "help with an unknown flag", args: []string{"-x", "-h"}, want: true},
		{name: "help with a value flag missing its value", args: []string{"-h", "--dir"}, want: true},
		{name: "no help at all", args: []string{"--dir", "file://x"}, want: false},
		{name: "help as a value", args: []string{"--dir", "-h"}, want: false},
		{name: "help as the value of an inherited flag", args: []string{"-c", "-h"}, want: false},
		{name: "help after the terminator", args: []string{"--", "-h"}, want: false},
		{name: "an explicit false", args: []string{"--help=false"}, want: false},
		{name: "a malformed help value", args: []string{"--help=maybe"}, want: false},
		{name: "an unknown flag alone", args: []string{"-x"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := newHelpAdapter(&helpRun{})
			adapter, _, err := root.Find([]string{"adapter"})
			c.Assert(err, qt.IsNil)

			c.Assert(cmdadapter.AnswersHelp(adapter, tt.args), qt.Equals, tt.want)
		})
	}
}
