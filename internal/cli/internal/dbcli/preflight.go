package dbcli

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/execpolicy"
	"ptah.run/internal/preflight"
	"ptah.run/migration/migrator"
)

// CombineMigrationHooks returns one hook that runs each non-nil hook in order
// and stops at the first error. It returns nil when no hooks are configured.
func CombineMigrationHooks(hooks ...migrator.PreMigrationHook) migrator.PreMigrationHook {
	hooks = slices.DeleteFunc(slices.Clone(hooks), func(hook migrator.PreMigrationHook) bool {
		return hook == nil
	})
	if len(hooks) == 0 {
		return nil
	}
	return func(ctx context.Context, plan migrator.MigrationPlan) error {
		for _, hook := range hooks {
			if err := hook(ctx, plan); err != nil {
				return err
			}
		}
		return nil
	}
}

// MigrationPreflightReporter writes pre-flight progress messages.
type MigrationPreflightReporter interface {
	Println(args ...any)
	Printf(format string, args ...any)
}

// MigrationPreflightRequest contains the CLI context for one migration
// pre-flight run.
type MigrationPreflightRequest struct {
	DryRun   bool
	Hooks    preflight.Options
	Reporter MigrationPreflightReporter
	Stdout   io.Writer
}

// RunMigrationPreflight runs configured migration pre-flight hooks, or reports
// that they were skipped for dry-run commands because backup hooks have side
// effects.
func RunMigrationPreflight(ctx context.Context, req MigrationPreflightRequest) error {
	if !req.Hooks.Enabled() {
		return nil
	}
	reporter := req.Reporter
	if reporter == nil {
		reporter = stdoutReporter{}
	}
	stdout := req.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	if req.DryRun {
		reporter.Println("Pre-flight hooks configured; skipped in dry-run mode")
		return nil
	}

	reporter.Println("Running pre-flight hooks...")
	results, err := preflight.Runner{Stdout: stdout}.Execute(ctx, req.Hooks)
	if flusher, ok := stdout.(interface{ Flush() }); ok {
		flusher.Flush()
	}
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Artifact != "" {
			reporter.Printf("Pre-flight %s completed: %s\n", result.Name, result.Artifact)
			continue
		}
		reporter.Printf("Pre-flight %s completed\n", result.Name)
	}
	return nil
}

// LockedMigrationPreflightHook adapts configured CLI pre-flight hooks into a
// migrator hook that receives the migration plan selected under the migration
// lock.
func LockedMigrationPreflightHook(dryRun bool, opts preflight.Options, reporter MigrationPreflightReporter, stdout io.Writer) migrator.PreMigrationHook {
	if reporter == nil {
		reporter = stdoutReporter{}
	}
	return func(ctx context.Context, plan migrator.MigrationPlan) error {
		ReportMigrationInputs(reporter.Printf, plan)
		if !opts.Enabled() {
			return nil
		}
		opts.CurrentVersion = plan.CurrentVersion
		opts.TargetVersion = plan.TargetVersion
		if err := RunMigrationPreflight(ctx, MigrationPreflightRequest{
			DryRun:   dryRun,
			Hooks:    opts,
			Reporter: reporter,
			Stdout:   stdout,
		}); err != nil {
			return err
		}
		reporter.Println()
		return nil
	}
}

// ReportMigrationInputs prints the names the selected statement bodies require
// before approval hooks or execution can resolve their values.
func ReportMigrationInputs(write func(string, ...any), plan migrator.MigrationPlan) {
	for _, statement := range plan.Inputs {
		version := statement.VersionKey
		if version == "" {
			version = fmt.Sprint(statement.Version)
		}
		for _, input := range statement.Inputs {
			write("Execution input for migration %s statement %d: %s %s\n", version, statement.StatementIndex, input.Source, input.Name)
		}
	}
}

type stdoutReporter struct{}

func (stdoutReporter) Println(args ...any) {
	fmt.Println(args...)
}

func (stdoutReporter) Printf(format string, args ...any) {
	fmt.Printf(format, args...)
}

// ConfiguredHookOrigin names the project file construct a pre-flight command
// hook came from, or is empty when the operator passed it with flagName or the
// flag's environment twin. A hook from ptah.yaml is repository-controlled
// shell, so the binary's program policy decides whether it runs; one the
// operator passed needs no second permission.
func ConfiguredHookOrigin(cmd *cobra.Command, flagName, construct string, value projectconfig.Value[string]) string {
	if flag := cmd.Flags().Lookup(flagName); flag != nil && flag.Changed {
		return ""
	}
	if !value.Present || strings.TrimSpace(value.Value) == "" {
		return ""
	}
	return construct
}

// AuthorizeConfiguredHook refuses a command hook from a project file that the
// binary's program policy does not allow, before the command connects to a
// database. The hook asks the same policy again where it starts. A dry run,
// which runs no hook, does not ask.
func AuthorizeConfiguredHook(ctx context.Context, origin string) error {
	return execpolicy.Authorize(ctx, origin, []string{preflight.Shell()})
}
