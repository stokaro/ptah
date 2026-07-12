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
