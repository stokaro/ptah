// Package cmdutil holds small helpers shared by CLI subcommands: consistent
// usage-error reporting (exit code 2 with a printed message) and directory
// validation.
package cmdutil

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

const configuredAnnotation = "ptah.exitcode_configured"

// ConfigureCommand installs Ptah's common CLI error contract on cmd. It is
// idempotent because many command constructors return package-level singletons.
func ConfigureCommand(cmd *cobra.Command) {
	ConfigureCommandArgs(cmd, NoPositionalArgs)
}

// ConfigureCommandArgs installs Ptah's common CLI error contract on cmd while
// preserving a command-specific Args validator.
func ConfigureCommandArgs(cmd *cobra.Command, args cobra.PositionalArgs) {
	if cmd.Annotations != nil && cmd.Annotations[configuredAnnotation] == "true" {
		return
	}
	if cmd.Annotations == nil {
		cmd.Annotations = make(map[string]string)
	}
	cmd.Annotations[configuredAnnotation] = "true"

	cmd.Args = args
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetFlagErrorFunc(FlagErrorFunc)
	if cmd.RunE != nil {
		cmd.RunE = WrapRunE(cmd.RunE)
	}
}

// WrapRunE maps ordinary command failures to exit code 2 while preserving
// expected-negative results that already carry an explicit exit code.
func WrapRunE(run func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := run(cmd, args)
		if err == nil || exitcode.Code(err, -1) != -1 {
			return err
		}
		fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n", err)
		return exitcode.New(2, err)
	}
}

// Fail prints err to the command's stderr and returns it as an exit-2 usage
// error. Commands that set SilenceErrors must route their usage failures
// through this so the message still reaches the user.
func Fail(cmd *cobra.Command, err error) error {
	fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n", err)
	return exitcode.New(2, err)
}

// FlagErrorFunc reports a cobra flag-parse error (unknown flag, bad value)
// with a printed message and exit code 2, matching every other usage error.
// Install it with cmd.SetFlagErrorFunc.
func FlagErrorFunc(cmd *cobra.Command, err error) error {
	return Fail(cmd, err)
}

// NoPositionalArgs is a cobra Args validator that rejects any positional
// argument with a printed message and exit code 2. Unlike cobra.NoArgs, whose
// error is swallowed under SilenceErrors and degrades to a bare exit 1, this
// routes through Fail so the failure is visible and carries the usage exit
// code, so a stray positional value does not masquerade as success/drift.
func NoPositionalArgs(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return Fail(cmd, fmt.Errorf("unexpected positional arguments %q", args))
	}
	return nil
}

// StatDir validates that dir exists and is a directory, returning an
// actionable error (wrapping the underlying os.Stat error, and distinguishing
// a path that exists but is a file) otherwise.
func StatDir(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("migrations directory %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("migrations directory %s: not a directory", dir)
	}
	return nil
}
