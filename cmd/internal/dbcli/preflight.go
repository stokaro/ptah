package dbcli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/stokaro/ptah/internal/preflight"
	"github.com/stokaro/ptah/migration/migrator"
)

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
	if !opts.Enabled() {
		return nil
	}
	return func(ctx context.Context, plan migrator.MigrationPlan) error {
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
		if reporter == nil {
			reporter = stdoutReporter{}
		}
		reporter.Println()
		return nil
	}
}

type stdoutReporter struct{}

func (stdoutReporter) Println(args ...any) {
	fmt.Println(args...)
}

func (stdoutReporter) Printf(format string, args ...any) {
	fmt.Printf(format, args...)
}
