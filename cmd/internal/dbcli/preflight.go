package dbcli

import (
	"context"
	"fmt"
	"os"

	"github.com/stokaro/ptah/internal/preflight"
	"github.com/stokaro/ptah/migration/migrator"
)

// MigrationPreflightRequest contains the CLI context for one migration
// pre-flight run.
type MigrationPreflightRequest struct {
	DryRun bool
	Hooks  preflight.Options
}

// RunMigrationPreflight runs configured migration pre-flight hooks, or reports
// that they were skipped for dry-run commands because backup hooks have side
// effects.
func RunMigrationPreflight(ctx context.Context, req MigrationPreflightRequest) error {
	if !req.Hooks.Enabled() {
		return nil
	}
	if req.DryRun {
		fmt.Println("Pre-flight hooks configured; skipped in dry-run mode")
		return nil
	}

	fmt.Println("Running pre-flight hooks...")
	results, err := preflight.Runner{Stdout: os.Stdout}.Execute(ctx, req.Hooks)
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Artifact != "" {
			fmt.Printf("Pre-flight %s completed: %s\n", result.Name, result.Artifact)
			continue
		}
		fmt.Printf("Pre-flight %s completed\n", result.Name)
	}
	return nil
}

// LockedMigrationPreflightHook adapts configured CLI pre-flight hooks into a
// migrator hook that receives the migration plan selected under the migration
// lock.
func LockedMigrationPreflightHook(dryRun bool, opts preflight.Options) migrator.PreMigrationHook {
	if !opts.Enabled() {
		return nil
	}
	return func(ctx context.Context, plan migrator.MigrationPlan) error {
		opts.CurrentVersion = plan.CurrentVersion
		opts.TargetVersion = plan.TargetVersion
		if err := RunMigrationPreflight(ctx, MigrationPreflightRequest{
			DryRun: dryRun,
			Hooks:  opts,
		}); err != nil {
			return err
		}
		fmt.Println()
		return nil
	}
}
