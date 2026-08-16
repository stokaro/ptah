package root

// White-box testing required: watchInterrupts is the seam that lets an
// interrupt be delivered without sending a real signal to the test binary, and
// the exit-code mapping it feeds is a process-exit boundary that cannot be
// observed through NewRootCommand without invoking os.Exit.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
)

func TestInterruptExitCodeReproducesTheDefaultHandler(t *testing.T) {
	tests := []struct {
		name string
		sig  os.Signal
		want int
	}{
		{name: "SIGINT is what Ctrl-C sends", sig: syscall.SIGINT, want: 130},
		{name: "SIGTERM is what kill sends", sig: syscall.SIGTERM, want: 143},
		{name: "SIGHUP is a closed terminal", sig: syscall.SIGHUP, want: 129},
		{name: "a signal that is not a syscall.Signal falls back", sig: fakeSignal{}, want: 130},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(interruptExitCode(&cobra.Command{Use: "native"}, tt.sig), qt.Equals, tt.want)
		})
	}
}

func TestASurfaceWithANarrowerContractKeepsItOnInterrupt(t *testing.T) {
	tests := []struct {
		name string
		sig  os.Signal
	}{
		{name: "SIGINT", sig: syscall.SIGINT},
		{name: "SIGTERM", sig: syscall.SIGTERM},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// The Atlas-compatible tree promises 0 or 1 and nothing else, and
			// the pinned community binary exits 1 when interrupted. Reporting
			// a signal status there would be a divergence this handler
			// invented. See docs/exit_codes.md.
			surface := &cobra.Command{Use: "compat"}
			cmdutil.SetErrorCodePolicy(surface, 1)
			leaf := &cobra.Command{Use: "leaf"}
			surface.AddCommand(leaf)

			c.Assert(interruptExitCode(surface, tt.sig), qt.Equals, 1)
			c.Assert(interruptExitCode(leaf, tt.sig), qt.Equals, 1)
		})
	}
}

func TestInterruptCancelsTheContextAndReportsTheSignal(t *testing.T) {
	c := qt.New(t)

	notifications := make(chan os.Signal, 1)
	stops := &atomic.Int64{}
	announce := &bytes.Buffer{}

	ctx, received, release := watchInterrupts(
		context.Background(),
		notifications,
		func() { stops.Add(1) },
		announce,
	)
	defer release()

	c.Assert(received(), qt.IsNil)
	c.Assert(ctx.Err(), qt.IsNil)

	notifications <- syscall.SIGTERM
	awaitCancel(c, ctx, "the context was not canceled by the interrupt")

	c.Assert(ctx.Err(), qt.Equals, context.Canceled)
	c.Assert(received(), qt.Equals, os.Signal(syscall.SIGTERM))
	c.Assert(announce.String(), qt.Equals, interruptNotice+"\n")

	// Delivery is detached on the first signal, so the second interrupt reaches
	// the default handler and kills the process the way it always did. The
	// detach is ordered before the cancelation awaited above, so counting it is
	// not a race.
	c.Assert(stops.Load(), qt.Equals, int64(1))
}

func TestReceivedReportsTheSameSignalOnEveryCall(t *testing.T) {
	c := qt.New(t)

	notifications := make(chan os.Signal, 1)
	ctx, received, release := watchInterrupts(context.Background(), notifications, func() {}, io.Discard)
	defer release()

	notifications <- syscall.SIGINT
	awaitCancel(c, ctx, "the context was not canceled by the interrupt")

	// ExecuteCommand asks once, but a reader that drained the answer would make
	// the exit code depend on how many times it was consulted.
	c.Assert(received(), qt.Equals, os.Signal(syscall.SIGINT))
	c.Assert(received(), qt.Equals, os.Signal(syscall.SIGINT))
}

func TestReleaseWithoutAnInterruptReportsNoSignal(t *testing.T) {
	c := qt.New(t)

	notifications := make(chan os.Signal, 1)
	stops := &atomic.Int64{}
	announce := &bytes.Buffer{}

	ctx, received, release := watchInterrupts(
		context.Background(),
		notifications,
		func() { stops.Add(1) },
		announce,
	)

	release()
	awaitCancel(c, ctx, "release did not cancel the context")

	c.Assert(received(), qt.IsNil)
	c.Assert(announce.String(), qt.Equals, "")
	c.Assert(stops.Load(), qt.Equals, int64(1))
}

func TestACanceledParentDoesNotLookLikeAnInterrupt(t *testing.T) {
	c := qt.New(t)

	parent, cancelParent := context.WithCancel(context.Background())
	notifications := make(chan os.Signal, 1)
	ctx, received, release := watchInterrupts(parent, notifications, func() {}, io.Discard)
	defer release()

	cancelParent()
	awaitCancel(c, ctx, "canceling the parent did not cancel the derived context")

	// A command that ends for its own reasons must keep its own exit code.
	c.Assert(received(), qt.IsNil)
}

func TestAnUninterruptedCommandKeepsItsOwnExitCode(t *testing.T) {
	tests := []struct {
		name string
		run  func(*cobra.Command, []string) error
		want int
	}{
		{name: "success is zero", run: func(*cobra.Command, []string) error { return nil }, want: 0},
		{
			name: "an ordinary failure is the mapped code",
			run:  func(*cobra.Command, []string) error { return errors.New("boom") },
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			cmd := &cobra.Command{Use: "quiet", SilenceUsage: true, SilenceErrors: true, RunE: tt.run}
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			c.Assert(runCommand(cmd), qt.Equals, tt.want)
		})
	}
}

// waitForCancel bounds every wait in this file. A mutant that drops the cancel
// would otherwise hang the package until the whole test binary times out, which
// reads as infrastructure trouble rather than as the assertion it is.
const waitForCancel = 5 * time.Second

func awaitCancel(c *qt.C, ctx context.Context, complaint string) {
	c.Helper()

	select {
	case <-ctx.Done():
	case <-time.After(waitForCancel):
		c.Fatal(complaint)
	}
}

// fakeSignal is an os.Signal that is not a syscall.Signal, which is what a
// non-Unix port delivers.
type fakeSignal struct{}

func (fakeSignal) String() string { return "fake" }

func (fakeSignal) Signal() {}
