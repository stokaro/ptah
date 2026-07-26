package cmdutil_test

import (
	"bytes"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func TestWrapRunEMapsOrdinaryErrorsToExit2(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "boom"}
	cmd.SetErr(&stderr)
	run := cmdutil.WrapRunE(func(_ *cobra.Command, _ []string) error {
		return errors.New("boom")
	})

	err := run(cmd, nil)

	c.Assert(err, qt.ErrorMatches, "boom")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr.String(), qt.Equals, "error: boom\n")
}

func TestWrapRunEPreservesExplicitExitCodes(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "diff"}
	cmd.SetErr(&stderr)
	run := cmdutil.WrapRunE(func(_ *cobra.Command, _ []string) error {
		return exitcode.New(1, errors.New("diff found"))
	})

	err := run(cmd, nil)

	c.Assert(err, qt.ErrorMatches, "diff found")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "")
}

func TestNormalizeCommandError_MapsCobraFlagGroupErrors(t *testing.T) {
	c := qt.New(t)

	cmd := &cobra.Command{
		Use:  "root",
		RunE: func(_ *cobra.Command, _ []string) error { return nil },
	}
	cmd.Flags().Bool("first", false, "First choice")
	cmd.Flags().Bool("second", false, "Second choice")
	cmd.MarkFlagsMutuallyExclusive("first", "second")
	cmdutil.SetErrorCodePolicy(cmd, 1)
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--first", "--second"})

	executed, err := cmd.ExecuteC()
	err = cmdutil.NormalizeCommandError(executed, err, 2)

	c.Assert(err, qt.ErrorMatches, `if any flags in the group \[first second\] are set none of the others can be; \[first second\] were all set`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "error: if any flags in the group [first second] are set none of the others can be; [first second] were all set\n")
}

func TestNormalizeCommandError_MapsLateDescendantErrors(t *testing.T) {
	c := qt.New(t)

	root := &cobra.Command{Use: "root"}
	cmdutil.SetErrorCodePolicy(root, 1)
	root.AddCommand(&cobra.Command{
		Use:  "late",
		RunE: commandError("late failure"),
	})
	var stderr bytes.Buffer
	root.SetErr(&stderr)
	root.SetArgs([]string{"late"})

	executed, err := root.ExecuteC()
	err = cmdutil.NormalizeCommandError(executed, err, 2)

	c.Assert(err, qt.ErrorMatches, "late failure")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "error: late failure\n")
}

func TestNormalizeCommandError_PreservesNativeExplicitExitCode(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "native"}
	cmd.SetErr(&stderr)
	err := cmdutil.NormalizeCommandError(cmd, exitcode.New(1, errors.New("drift")), 2)

	c.Assert(err, qt.ErrorMatches, "drift")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "")
}

func TestNormalizeCommandError_RemapExplicitErrorWithoutDuplicateOutput(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "atlas"}
	cmd.SetErr(&stderr)
	cmdutil.SetErrorCodePolicy(cmd, 1)
	_, err := stderr.WriteString("error: boom\n")
	c.Assert(err, qt.IsNil)

	err = cmdutil.NormalizeCommandError(cmd, exitcode.New(2, errors.New("boom")), 2)

	c.Assert(err, qt.ErrorMatches, "boom")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "error: boom\n")
}

func commandError(message string) func(*cobra.Command, []string) error {
	return func(_ *cobra.Command, _ []string) error {
		return errors.New(message)
	}
}
