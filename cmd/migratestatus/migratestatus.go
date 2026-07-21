package migratestatus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	dbURLFlag      = "db-url"
	migrationsFlag = "migrations-dir"
	dirFormatFlag  = "dir-format"
	atlasEnvFlag   = "atlas-env"
	verboseFlag    = "verbose"
	jsonFlag       = "json"
	exitCodeFlag   = "exit-code"
)

type options struct {
	dbURL               string
	migrationsDir       string
	dirFormat           string
	atlasEnv            string
	verbose             bool
	jsonOutput          bool
	exitOnPending       bool
	connectTimeout      string
	configPath          string
	envName             string
	migrationsSchema    string
	migrationsTable     string
	revisionTableFormat string
}

func NewMigrateStatusCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show current migration status",
		Long: `Show the current migration status of the database.

This command displays information about:
- Current database schema version
- Total number of available migrations
- Number of pending migrations
- List of pending migration versions

This is useful for checking the state of your database before running
migrations or for debugging migration issues.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return migrateStatusCommand(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.StringVar(&opts.migrationsDir, migrationsFlag, "", "Directory containing migration files (required)")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.BoolVar(&opts.verbose, verboseFlag, false, "Enable verbose output with detailed migration information")
	flags.BoolVar(&opts.jsonOutput, jsonFlag, false, "Output status in JSON format")
	flags.BoolVar(&opts.exitOnPending, exitCodeFlag, false, "Exit with 1 when pending migrations are available")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
}

func migrateStatusCommand(cmd *cobra.Command, opts *options) error {
	dbURL := opts.dbURL
	migrationsDir := opts.migrationsDir
	dirFormatValue := opts.dirFormat
	atlasEnv := opts.atlasEnv
	migrationsSchema := opts.migrationsSchema
	migrationsTable := opts.migrationsTable
	revisionFormatValue := opts.revisionTableFormat

	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return err
	}
	dbURL = dbcli.EffectiveString(cmd, dbURLFlag, dbURL, projectCfg.DatabaseURL)
	migrationsDir = dbcli.EffectiveString(cmd, migrationsFlag, migrationsDir, projectCfg.Migration.Dir)
	dirFormatValue = dbcli.EffectiveString(cmd, dirFormatFlag, dirFormatValue, projectCfg.Migration.Format)
	atlasEnv = dbcli.EffectiveString(cmd, atlasEnvFlag, atlasEnv, projectCfg.EnvName)
	migrationsSchema = dbcli.EffectiveString(cmd, dbcli.MigrationsSchemaFlagName, migrationsSchema, projectCfg.Migration.RevisionsSchema)
	migrationsTable = dbcli.EffectiveString(cmd, dbcli.MigrationsTableFlagName, migrationsTable, projectCfg.Migration.RevisionsTable)
	revisionFormatValue = dbcli.EffectiveString(cmd, dbcli.RevisionTableFormatFlagName, revisionFormatValue, projectCfg.Migration.RevisionFormat)
	connectTimeoutValue := dbcli.EffectiveString(cmd, dbcli.ConnectTimeoutFlagName, opts.connectTimeout, projectCfg.Migration.ConnectTimeout)

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(revisionFormatValue)
	if err != nil {
		return err
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(connectTimeoutValue)
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	// Create filesystem from migrations directory
	migrationsFS := os.DirFS(migrationsDir)

	mig, err := migrator.NewFSMigrator(
		conn,
		migrationsFS,
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: atlasEnv}),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(migrationsSchema, migrationsTable).
		WithRevisionTableFormat(revisionFormat)

	// Get migration status
	status, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting migration status: %w", err)
	}

	if opts.jsonOutput {
		if err := outputJSON(status); err != nil {
			return err
		}
	} else if err := outputHuman(status, conn, opts.verbose); err != nil {
		return err
	}

	if opts.exitOnPending {
		return pendingMigrationsExitCode(status)
	}
	return nil
}

func pendingMigrationsExitCode(status *migrator.MigrationStatus) error {
	if status.HasPendingChanges {
		return exitcode.New(1, errors.New("pending migrations available"))
	}
	return nil
}

func outputJSON(status *migrator.MigrationStatus) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(status)
}

func outputHuman(status *migrator.MigrationStatus, conn *dbschema.DatabaseConnection, verbose bool) error { //revive:disable-line:flag-parameter // it's ok here
	fmt.Println("=== MIGRATION STATUS ===")
	fmt.Printf("Database: %s\n", dbschema.FormatDatabaseURL("***"))
	fmt.Printf("Dialect: %s\n", conn.Info().Dialect)
	fmt.Printf("Schema: %s\n", conn.Info().Schema)
	fmt.Println()

	fmt.Printf("Current Version: %d\n", status.CurrentVersion)
	fmt.Printf("Total Migrations: %d\n", status.TotalMigrations)
	fmt.Printf("Applied Migrations: %d\n", len(status.AppliedMigrations))
	fmt.Printf("Pending Migrations: %d\n", len(status.PendingMigrations))
	fmt.Printf("Out-of-order Migrations: %d\n", len(status.OutOfOrderMigrations))

	if status.DirtyRevision != nil {
		fmt.Println("Status: ❌ Dirty migration state detected")
		fmt.Printf(
			"Dirty Migration: version=%d state=%s applied=%d/%d\n",
			status.DirtyRevision.Version,
			status.DirtyRevision.State,
			status.DirtyRevision.Applied,
			status.DirtyRevision.Total,
		)
		if status.DirtyRevision.Error != "" {
			fmt.Printf("Error: %s\n", status.DirtyRevision.Error)
		}
		if status.DirtyRevision.ErrorStatement != "" {
			fmt.Printf("Error Statement: %s\n", status.DirtyRevision.ErrorStatement)
		}
		fmt.Println("\nRun 'ptah migrations repair --version <version>' after fixing the database state.")
		return nil
	}

	if status.HasPendingChanges {
		fmt.Println("Status: ⚠️  Pending migrations available")

		if verbose && len(status.PendingMigrations) > 0 {
			fmt.Println("\nPending migration versions:")
			for _, version := range status.PendingMigrations {
				fmt.Printf("  - %d\n", version)
			}
		}
		if verbose && len(status.OutOfOrderMigrations) > 0 {
			fmt.Println("\nOut-of-order migration versions:")
			for _, version := range status.OutOfOrderMigrations {
				fmt.Printf("  - %d\n", version)
			}
		}

		fmt.Println("\nRun 'ptah migrations up' to apply pending migrations.")
	} else {
		fmt.Println("Status: ✅ Database is up to date")
	}

	if verbose {
		fmt.Println("\n=== DETAILED INFORMATION ===")

		if status.TotalMigrations == 0 {
			fmt.Println("No migrations found in the migrations directory.")
		} else {
			fmt.Printf("Applied migrations: %d\n", len(status.AppliedMigrations))

			if len(status.PendingMigrations) > 0 {
				fmt.Printf("Next migration to apply: %d\n", status.PendingMigrations[0])
			}
		}
	}

	return nil
}
