package migratestatus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

var migrateStatusCmd = &cobra.Command{
	Use:   "migrate-status",
	Short: "Show current migration status",
	Long: `Show the current migration status of the database.

This command displays information about:
- Current database schema version
- Total number of available migrations
- Number of pending migrations
- List of pending migration versions

This is useful for checking the state of your database before running
migrations or for debugging migration issues.`,
	RunE: migrateStatusCommand,
}

const (
	dbURLFlag      = "db-url"
	migrationsFlag = "migrations-dir"
	dirFormatFlag  = "dir-format"
	atlasEnvFlag   = "atlas-env"
	verboseFlag    = "verbose"
	jsonFlag       = "json"
)

var migrateStatusFlags = map[string]cobraflags.Flag{
	dbURLFlag: &cobraflags.StringFlag{
		Name:  dbURLFlag,
		Value: "",
		Usage: "Database URL (required). Example: postgres://localhost:5432/dbname",
	},
	migrationsFlag: &cobraflags.StringFlag{
		Name:  migrationsFlag,
		Value: "",
		Usage: "Directory containing migration files (required)",
	},
	dirFormatFlag: &cobraflags.StringFlag{
		Name:  dirFormatFlag,
		Value: string(migrator.MigrationDirFormatAuto),
		Usage: "Migration directory format: auto, ptah, or atlas",
	},
	atlasEnvFlag: &cobraflags.StringFlag{
		Name:  atlasEnvFlag,
		Value: "",
		Usage: "Value exposed as .Env when rendering Atlas SQL template migrations",
	},
	verboseFlag: &cobraflags.BoolFlag{
		Name:  verboseFlag,
		Value: false,
		Usage: "Enable verbose output with detailed migration information",
	},
	jsonFlag: &cobraflags.BoolFlag{
		Name:  jsonFlag,
		Value: false,
		Usage: "Output status in JSON format",
	},
	dbcli.ConnectTimeoutFlagName:   dbcli.NewConnectTimeoutFlag(),
	dbcli.MigrationsSchemaFlagName: dbcli.NewMigrationsSchemaFlag(),
	dbcli.MigrationsTableFlagName:  dbcli.NewMigrationsTableFlag(),
}

func NewMigrateStatusCommand() *cobra.Command {
	cobraflags.RegisterMap(migrateStatusCmd, migrateStatusFlags)
	return migrateStatusCmd
}

func migrateStatusCommand(_ *cobra.Command, _ []string) error {
	dbURL := migrateStatusFlags[dbURLFlag].GetString()
	migrationsDir := migrateStatusFlags[migrationsFlag].GetString()
	dirFormatValue := migrateStatusFlags[dirFormatFlag].GetString()
	atlasEnv := migrateStatusFlags[atlasEnvFlag].GetString()
	verbose := migrateStatusFlags[verboseFlag].GetBool()
	jsonOutput := migrateStatusFlags[jsonFlag].GetBool()
	migrationsSchema := migrateStatusFlags[dbcli.MigrationsSchemaFlagName].GetString()
	migrationsTable := migrateStatusFlags[dbcli.MigrationsTableFlagName].GetString()

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

	connectTimeout, err := dbcli.ParseConnectTimeout(migrateStatusFlags[dbcli.ConnectTimeoutFlagName].GetString())
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
	mig = mig.WithMigrationsTable(migrationsSchema, migrationsTable)

	// Get migration status
	status, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting migration status: %w", err)
	}

	if jsonOutput {
		return outputJSON(status)
	}

	return outputHuman(status, conn, verbose)
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

		fmt.Println("\nRun 'migrate-up' to apply pending migrations.")
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
