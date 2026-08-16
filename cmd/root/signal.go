package root

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
)

// interruptSignals are the two ways a person or a supervisor asks a CLI to
// stop: Ctrl-C and the default `kill`.
var interruptSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

// interruptNotice is what an operator reads after the first interrupt. It has a
// job: the command no longer dies on the spot, and without a word explaining
// why, a shell that does not return for another second looks hung. It also says
// how to get the old behavior back, which is the second interrupt.
const interruptNotice = "interrupt received, releasing resources; interrupt again to stop immediately"

// interruptExitCode is the status the process exits with after sig interrupted
// cmd.
//
// On the native surface it is the shell convention, 128 plus the signal number
// -- 130 for SIGINT, 143 for SIGTERM -- which reproduces what the default
// handler produced before this package took the signal over, so the contract a
// caller scripts against does not move.
//
// A surface that declares an error-code policy gets that code instead. The
// Atlas-compatible tree declares 1 because it promises the narrower contract
// documented in docs/exit_codes.md, and because the pinned community binary
// exits 1 when interrupted. A signal status there would be a divergence
// invented by the signal handler.
func interruptExitCode(cmd *cobra.Command, sig os.Signal) int {
	if code, ok := cmdutil.ErrorCodePolicy(cmd); ok {
		return code
	}
	if signum, ok := sig.(syscall.Signal); ok {
		return 128 + int(signum)
	}
	return 130
}

// withInterruptCancel returns a context canceled by the first SIGINT or
// SIGTERM, and a function reporting which signal arrived, or nil if none did.
//
// Before this, nothing in the tree called signal.Notify at all, so the default
// handler killed the process where it stood and every deferred release was
// skipped. Measured: Ctrl-C during a `docker://` readiness wait left the dev
// container running, holding a copy of the operator's schema on a published
// port (stokaro/ptah#1565).
//
// Cancelation rather than an immediate exit is what makes the cleanup run: the
// verbs already thread cmd.Context() into the work, and the releases they defer
// remove through a context detached from it, so a canceled command can still
// finish tidying. Exiting from the handler would be the same defect wearing a
// handler.
func withInterruptCancel(parent context.Context, announce io.Writer) (ctx context.Context, received func() os.Signal, release func()) {
	notifications := make(chan os.Signal, 1)
	signal.Notify(notifications, interruptSignals...)
	return watchInterrupts(parent, notifications, func() { signal.Stop(notifications) }, announce)
}

// watchInterrupts is withInterruptCancel with the process-wide signal plumbing
// handed in, so that a test can deliver an interrupt without sending one to the
// test binary. stop detaches the delivery channel.
//
// Delivery is detached as soon as the first signal is seen, which restores the
// default handler. A second Ctrl-C from an operator who has waited long enough
// therefore kills the process immediately, as it should -- the graceful path is
// offered once, not enforced.
func watchInterrupts(parent context.Context, notifications <-chan os.Signal, stop func(), announce io.Writer) (ctx context.Context, received func() os.Signal, release func()) {
	ctx, cancel := context.WithCancel(parent)

	// Read into a local before the goroutine starts: capturing the named result
	// itself would make any later reassignment of ctx a data race.
	done := ctx.Done()

	// Stored rather than sent down a channel so that asking twice answers twice.
	// The exit code must not depend on how many times it was consulted.
	var seen atomic.Pointer[os.Signal]
	go func() {
		select {
		case sig := <-notifications:
			stop()
			seen.Store(&sig)
			fmt.Fprintln(announce, interruptNotice)
			cancel()
		case <-done:
		}
	}()

	received = func() os.Signal {
		if sig := seen.Load(); sig != nil {
			return *sig
		}
		return nil
	}
	return ctx, received, func() {
		stop()
		cancel()
	}
}
