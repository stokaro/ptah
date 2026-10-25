package root

import (
	"context"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/cliobs"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/rundiag"
)

// RunContext runs cmd under ctx and reports the status a process would exit
// with, without exiting one.
//
// Execute owns the process: it installs signal delivery and it calls os.Exit.
// A caller that is not a process owns neither, and needs both halves of that
// separated -- a test, which os.Exit would end before it could assert, and a
// build with no process to exit, js/wasm above all.
//
// Cancelation is a parameter here rather than a signal handler for the same
// reason. Nothing in a browser ever sends SIGINT, so a handler there would
// watch for something that cannot arrive; a host that wants to stop a command
// cancels the context it passed in. On the native surface runCommand still
// derives that context from the signal, so the CLI's behavior is unchanged.
//
// The run collects the warnings that concern many objects and writes one line
// per kind to cmd's error stream when the command ends, whether it succeeded
// or not, in the words cmd's surface declares for listing them, and as JSON
// records when PTAH_LOG_FORMAT makes the log JSON; see [rundiag],
// [cmdutil.SetDiagnosticsListing] and [cliobs.JSONLog].
func RunContext(ctx context.Context, cmd *cobra.Command, args ...string) int {
	report := rundiag.New(rundiag.Options{
		Stream: cmd.ErrOrStderr(), Listing: cmdutil.DiagnosticsListing(cmd), Records: cliobs.JSONLog(),
	})
	defer report.Flush()
	cmd.SetArgs(args)
	cmd.SetContext(rundiag.With(ctx, report))

	if err := executeWithRecovery(cmd); err != nil {
		return exitcode.Code(err, 2)
	}
	return 0
}
