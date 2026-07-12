package seed

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/seeder"
)

// NewSeedCommand returns the seed command.
func NewSeedCommand() *cobra.Command {
	var dbURL string
	var seedsDir string
	var env string
	var protectedEnvs []string
	var protectedTables []string
	var force bool
	var idempotent bool
	var allowProd bool
	var verbose bool
	var connectTimeoutRaw string

	cmd := &cobra.Command{
		Use:          "seed",
		Short:        "Apply environment-scoped SQL seed files",
		SilenceUsage: true,
		Long: `Apply SQL seed files from a seeds directory.

Seed files use the NNN_description.env.sql naming convention. Files matching
the requested --env and files ending in .all.sql are applied in version order.
Successful seeds are recorded in schema_seeds, so re-running the command is a
no-op unless --force is set.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSeed(cmd.Context(), runOptions{
				dbURL:             dbURL,
				seedsDir:          seedsDir,
				env:               env,
				protectedEnvs:     protectedEnvs,
				protectedTables:   protectedTables,
				force:             force,
				idempotent:        idempotent,
				allowProd:         allowProd,
				verbose:           verbose,
				connectTimeoutRaw: connectTimeoutRaw,
			})
		},
	}

	cmd.Flags().StringVar(&dbURL, "db-url", "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	cmd.Flags().StringVar(&seedsDir, "seeds-dir", "seeds", "Directory containing seed files")
	cmd.Flags().StringVar(&env, "env", "", "Seed environment to apply (required), for example dev, test, or prod")
	cmd.Flags().StringArrayVar(&protectedEnvs, "protected-env", seeder.DefaultProtectedEnvs(), "Environment name that requires --allow-prod; repeat to add more")
	cmd.Flags().StringArrayVar(&protectedTables, "protected-table", nil, "Existing target table name that requires --allow-prod; repeat to add more")
	cmd.Flags().BoolVar(&force, "force", false, "Re-run seeds even when they are already recorded in schema_seeds")
	cmd.Flags().BoolVar(&idempotent, "idempotent", false, "Treat duplicate-key conflicts as already-applied seed data using a per-file savepoint")
	cmd.Flags().BoolVar(&allowProd, "allow-prod", false, "Allow seeding a protected production-like environment")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Enable verbose output")
	cmd.Flags().StringVar(
		&connectTimeoutRaw,
		dbcli.ConnectTimeoutFlagName,
		dbcli.DefaultConnectTimeout.String(),
		"Maximum time to wait when establishing the initial database connection (for example 5s or 1m). Use 0 to disable the timeout.",
	)

	return cmd
}

type runOptions struct {
	dbURL             string
	seedsDir          string
	env               string
	protectedEnvs     []string
	protectedTables   []string
	force             bool
	idempotent        bool
	allowProd         bool
	verbose           bool
	connectTimeoutRaw string
}

func runSeed(ctx context.Context, opts runOptions) error {
	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.seedsDir == "" {
		return fmt.Errorf("seeds directory is required")
	}

	seedOpts := seeder.Options{
		Env:             opts.env,
		ProtectedEnvs:   opts.protectedEnvs,
		ProtectedTables: opts.protectedTables,
		Force:           opts.force,
		Idempotent:      opts.idempotent,
		AllowProd:       opts.allowProd,
	}
	if err := seeder.ValidateOptions(seedOpts); err != nil {
		return err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeoutRaw)
	if err != nil {
		return err
	}

	if opts.verbose {
		fmt.Printf("Connecting to database: %s\n", dbschema.FormatDatabaseURL(opts.dbURL))
	}
	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	fmt.Println("=== SEED ===")
	fmt.Printf("Database: %s\n", dbschema.FormatDatabaseURL(opts.dbURL))
	fmt.Printf("Dialect: %s\n", conn.Info().Dialect)
	fmt.Printf("Seeds directory: %s\n", opts.seedsDir)
	fmt.Printf("Environment: %s\n", strings.TrimSpace(opts.env))
	if opts.force {
		fmt.Println("Force: true")
	}
	if opts.idempotent {
		fmt.Println("Idempotent: true")
	}
	fmt.Println()

	result, err := seeder.Apply(ctx, conn, os.DirFS(opts.seedsDir), seedOpts)
	if err != nil {
		return fmt.Errorf("error applying seeds: %w", err)
	}

	fmt.Printf("Matching seeds: %d\n", result.Total)
	fmt.Printf("Applied seeds: %d\n", len(result.Applied))
	fmt.Printf("Skipped seeds: %d\n", len(result.Skipped))
	if opts.verbose {
		for _, seed := range result.Applied {
			fmt.Printf("Applied: %s\n", seed.Path)
		}
		for _, seed := range result.Skipped {
			fmt.Printf("Skipped: %s\n", seed.Path)
		}
	}
	if len(result.Applied) == 0 {
		fmt.Println("Database seed data is already up to date.")
	} else {
		fmt.Println("Seeds completed successfully.")
	}
	return nil
}
