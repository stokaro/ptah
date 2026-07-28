// Package cmdutil holds small helpers shared by CLI subcommands: consistent
// usage-error reporting, command-tree error policies, and directory validation.
package cmdutil

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

const (
	configuredAnnotation      = "ptah.exitcode_configured"
	errorCodePolicyAnnotation = "ptah.error_code_policy"
	unconfiguredErrorCode     = -1
	nativeCommandErrorCode    = 2
)

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

// SetErrorCodePolicy marks cmd and its future descendants with an inherited
// process exit code. [NormalizeCommandError] applies the policy after Cobra
// finishes command execution and validation.
func SetErrorCodePolicy(cmd *cobra.Command, code int) {
	if cmd.Annotations == nil {
		cmd.Annotations = make(map[string]string)
	}
	cmd.Annotations[errorCodePolicyAnnotation] = strconv.Itoa(code)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
}

// NormalizeCommandError applies the nearest configured ancestor error policy
// to err. Commands without a configured policy preserve explicit exit codes
// and map an ordinary error to fallback.
func NormalizeCommandError(cmd *cobra.Command, err error, fallback int) error {
	if err == nil {
		return nil
	}
	if code, ok := commandErrorCode(cmd); ok {
		currentCode := exitcode.Code(err, unconfiguredErrorCode)
		switch currentCode {
		case code:
			return err
		case unconfiguredErrorCode:
			fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n", err)
		}
		return exitcode.New(code, err)
	}
	if exitcode.Code(err, unconfiguredErrorCode) != unconfiguredErrorCode {
		return err
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n", err)
	return exitcode.New(fallback, err)
}

func commandErrorCode(cmd *cobra.Command) (int, bool) {
	for current := cmd; current != nil; current = current.Parent() {
		policy, ok := current.Annotations[errorCodePolicyAnnotation]
		if !ok {
			continue
		}
		code, err := strconv.Atoi(policy)
		if err == nil {
			return code, true
		}
	}
	return 0, false
}

// WrapRunE maps ordinary command failures to exit code 2 while preserving
// expected-negative results that already carry an explicit exit code.
func WrapRunE(run func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := run(cmd, args)
		if err == nil || exitcode.Code(err, unconfiguredErrorCode) != unconfiguredErrorCode {
			return err
		}
		fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n", err)
		return exitcode.New(nativeCommandErrorCode, err)
	}
}

// Fail prints err to the command's stderr and returns it as an exit-2 usage
// error. Commands that set SilenceErrors must route their usage failures
// through this so the message still reaches the user.
func Fail(cmd *cobra.Command, err error) error {
	fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n", err)
	return exitcode.New(nativeCommandErrorCode, err)
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

// ExactArgs returns a Cobra validator that requires exactly count positional
// arguments while preserving Ptah's printed exit-2 usage-error contract.
func ExactArgs(count int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != count {
			return Fail(cmd, fmt.Errorf("expected exactly %d positional argument(s), got %d", count, len(args)))
		}
		return nil
	}
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
