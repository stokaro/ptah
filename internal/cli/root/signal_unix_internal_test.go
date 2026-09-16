//go:build unix

package root

// White-box testing required: runCommand is the seam where the process exit
// code is decided, and observing an interrupted command through ExecuteCommand
// would end the test binary. Unix only: sending a signal to this process needs
// syscall.Kill, which Windows does not have.

import (
	"bytes"
	"io"
	"syscall"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
)

func TestAnInterruptedCommandExitsWithTheSignalStatus(t *testing.T) {
	c := qt.New(t)

	stderr := &bytes.Buffer{}
	cmd := &cobra.Command{Use: "blocks", SilenceUsage: true, SilenceErrors: true}
	cmd.SetOut(io.Discard)
	cmd.SetErr(stderr)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		// runCommand has registered signal delivery by the time RunE runs, so
		// this interrupt cannot reach the default handler and kill the test
		// binary. It stands in for the Ctrl-C an operator sends while a verb is
		// waiting on a dev database.
		c.Assert(syscall.Kill(syscall.Getpid(), syscall.SIGINT), qt.IsNil)
		awaitCancel(c, cmd.Context(), "the running command was not canceled by the interrupt")
		return cmd.Context().Err()
	}

	c.Assert(runCommand(cmd), qt.Equals, 130)
	c.Assert(stderr.String(), qt.Contains, interruptNotice)
}

// TestACommandThatFinishedItsWorkKeepsItsOwnStatus covers the window a signal
// cannot do anything about: the verb has committed everything the invocation
// asked for, and the interrupt arrives while the process is on its way out.
// Telling the operator the run was interrupted, against a database it fully
// migrated, is the defect (stokaro/ptah#3314).
func TestACommandThatFinishedItsWorkKeepsItsOwnStatus(t *testing.T) {
	c := qt.New(t)

	stderr := &bytes.Buffer{}
	cmd := &cobra.Command{Use: "finishes", SilenceUsage: true, SilenceErrors: true}
	cmd.SetOut(io.Discard)
	cmd.SetErr(stderr)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		cmdutil.ReportWorkFinished(cmd.Context())
		c.Assert(syscall.Kill(syscall.Getpid(), syscall.SIGINT), qt.IsNil)
		// Waiting for the cancelation is what makes this the narrow window
		// rather than a race: the signal is delivered and recorded before the
		// command returns.
		awaitCancel(c, cmd.Context(), "the interrupt did not reach the running command")
		return nil
	}

	c.Assert(runCommand(cmd), qt.Equals, 0)
	c.Assert(stderr.String(), qt.Contains, interruptNotice)
}

// TestACommandThatEndsBecauseOfTheSignalReportsTheSignal is the control, and
// the shape of `ptah schema serve`: a command that returns nil from its own
// shutdown looks exactly like one that finished, so the status stays the
// signal's until a verb says otherwise. docs/exit_codes.md promises 130 here.
func TestACommandThatEndsBecauseOfTheSignalReportsTheSignal(t *testing.T) {
	c := qt.New(t)

	stderr := &bytes.Buffer{}
	cmd := &cobra.Command{Use: "serves", SilenceUsage: true, SilenceErrors: true}
	cmd.SetOut(io.Discard)
	cmd.SetErr(stderr)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		c.Assert(syscall.Kill(syscall.Getpid(), syscall.SIGINT), qt.IsNil)
		awaitCancel(c, cmd.Context(), "the interrupt did not reach the running command")
		return nil
	}

	c.Assert(runCommand(cmd), qt.Equals, 130)
	c.Assert(stderr.String(), qt.Contains, interruptNotice)
}
