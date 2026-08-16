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
