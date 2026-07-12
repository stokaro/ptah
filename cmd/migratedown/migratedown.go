package migratedown

import (
	"context"
	"fmt"
	"os"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/onlineddl"
)

var migrateDownCmd = &cobra.Command{
	Use:   "migrate-down",
	Short: "Roll back migrations to a specific version",
	Long: `Roll back database migrations to a specific target version.

This command applies down migrations to revert the database schema to an earlier
version. All migrations with versions higher than the target version will be
rolled back in reverse order.

Each migration rollback is run in a transaction, so if any rollback fails, it will
be rolled back and the migration process will stop.

⚠️  WARNING: This operation can result in data loss! Make sure you have backups
before running down migrations in production.`,
	RunE: migrateDownCommand,
}

const (
	dbURLFlag            = "db-url"
	migrationsFlag       = "migrations-dir"
	targetFlag           = "target"
	dryRunFlag           = "dry-run"
	verboseFlag          = "verbose"
	confirmFlag          = "confirm"
	lockTimeoutFlag      = "lock-timeout"
	statementTimeoutFlag = "statement-timeout"
)

var migrateDownFlags = map[string]cobraflags.Flag{
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
	targetFlag: &cobraflags.IntFlag{
		Name:  targetFlag,
		Value: 0,
		Usage: "Target version to migrate down to (required)",
	},
	dryRunFlag: &cobraflags.BoolFlag{
		Name:  dryRunFlag,
		Value: false,
		Usage: "Show what migrations would be rolled back without actually running them",
	},
	verboseFlag: &cobraflags.BoolFlag{
		Name:  verboseFlag,
		Value: false,
		Usage: "Enable verbose output",
	},
	confirmFlag: &cobraflags.BoolFlag{
		Name:  confirmFlag,
		Value: false,
		Usage: "Skip confirmation prompt (use with caution!)",
	},
	lockTimeoutFlag: &cobraflags.StringFlag{
		Name:  lockTimeoutFlag,
		Value: "",
		Usage: "Default per-migration lock timeout, such as 3s or 500ms",
	},
	statementTimeoutFlag: &cobraflags.StringFlag{
		Name:  statementTimeoutFlag,
		Value: "",
		Usage: "Default per-migration statement timeout, such as 30s or 2m",
	},
	dbcli.ConnectTimeoutFlagName: dbcli.NewConnectTimeoutFlag(),
	dbcli.ConfigFlagName:         dbcli.NewConfigFlag(),
}

func NewMigrateDownCommand() *cobra.Command {
	cobraflags.RegisterMap(migrateDownCmd, migrateDownFlags)
	return migrateDownCmd
}

func migrateDownCommand(_ *cobra.Command, _ []string) error {
	dbURL := migrateDownFlags[dbURLFlag].GetString()
	migrationsDir := migrateDownFlags[migrationsFlag].GetString()
	targetVersion := migrateDownFlags[targetFlag].GetInt()
	dryRun := migrateDownFlags[dryRunFlag].GetBool()
	verbose := migrateDownFlags[verboseFlag].GetBool()
	skipConfirm := migrateDownFlags[confirmFlag].GetBool()
	lockTimeout := migrateDownFlags[lockTimeoutFlag].GetString()
	statementTimeout := migrateDownFlags[statementTimeoutFlag].GetString()

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	if targetVersion < 0 {
		return fmt.Errorf("target version must be >= 0")
	}

	if verbose {
		fmt.Printf("Connecting to database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	}

	timeouts, err := migrator.ParseMigrationTimeouts(lockTimeout, statementTimeout)
	if err != nil {
		return err
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(migrateDownFlags[dbcli.ConnectTimeoutFlagName].GetString())
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

	// Set dry run mode if requested
	conn.Writer().SetDryRun(dryRun)

	if dryRun {
		fmt.Println("=== DRY RUN MODE ===")
		fmt.Println("No actual changes will be made to the database")
		fmt.Println()
	}

	fmt.Println("=== MIGRATE DOWN ===")
	fmt.Printf("Database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	fmt.Printf("Dialect: %s\n", conn.Info().Dialect)
	fmt.Printf("Migrations directory: %s\n", migrationsDir)
	fmt.Printf("Target version: %d\n", targetVersion)
	fmt.Println()

	// Create filesystem from migrations directory
	migrationsFS := os.DirFS(migrationsDir)

	// Online-DDL routing works for down migrations too: a rollback ALTER on
	// a large table is just as lock-heavy as the forward one.
	onlineCfg, err := dbcli.LoadOnlineDDLConfig(migrateDownFlags[dbcli.ConfigFlagName].GetString())
	if err != nil {
		return err
	}
	if onlineCfg.Enabled() {
		fmt.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
	}
	interceptor := onlineddl.New(*onlineCfg).WithDryRun(dryRun)

	// Create migrator to access applied migrations
	mig, err := migrator.NewFSMigrator(conn, migrationsFS, migrator.WithStatementInterceptor(interceptor))
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithDefaultTimeouts(timeouts)

	// Get migration status before running
	status, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting migration status: %w", err)
	}

	fmt.Printf("Current version: %d\n", status.CurrentVersion)
	fmt.Printf("Total migrations: %d\n", status.TotalMigrations)

	if status.CurrentVersion <= targetVersion {
		fmt.Printf("✅ Database is already at or below target version %d!\n", targetVersion)
		return nil
	}

	// Get applied migrations from the database
	appliedMigrations, err := mig.GetAppliedMigrations(context.Background())
	if err != nil {
		return fmt.Errorf("error getting applied migrations: %w", err)
	}

	// Calculate which migrations will be rolled back
	var migrationsToRollback []int
	for _, version := range appliedMigrations {
		if version > targetVersion {
			migrationsToRollback = append(migrationsToRollback, version)
		}
	}

	fmt.Printf("Migrations to roll back: %d\n", len(migrationsToRollback))

	if verbose {
		fmt.Printf("Will roll back from version %d to %d\n", status.CurrentVersion, targetVersion)
		if len(migrationsToRollback) > 0 {
			fmt.Printf("Specific migrations to rollback: %v\n", migrationsToRollback)
		}
	}

	fmt.Println()

	// Safety confirmation (unless skipped or dry run)
	if !dryRun && !skipConfirm {
		fmt.Println("⚠️  WARNING: Rolling back migrations can result in data loss!")
		fmt.Printf("This will roll back the database from version %d to version %d.\n", status.CurrentVersion, targetVersion)
		if len(migrationsToRollback) > 0 {
			fmt.Printf("The following %d migration(s) will be rolled back: %v\n", len(migrationsToRollback), migrationsToRollback)
		}
		fmt.Print("Are you sure you want to continue? Type 'YES' to confirm: ")

		var confirmation string
		fmt.Scanln(&confirmation)

		if confirmation != "YES" {
			fmt.Println("Migration rollback cancelled.")
			return nil
		}
		fmt.Println()
	}

	// Run down migrations
	err = mig.MigrateDownTo(context.Background(), targetVersion)
	if err != nil {
		return fmt.Errorf("error running down migrations: %w", err)
	}

	// Get final status
	finalStatus, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting final migration status: %w", err)
	}

	fmt.Println()
	if dryRun {
		fmt.Println("✅ Dry run completed successfully!")
		fmt.Printf("Would have rolled back to version: %d\n", targetVersion)
		if len(migrationsToRollback) > 0 {
			fmt.Printf("Would have rolled back these migrations: %v\n", migrationsToRollback)
		}
	} else {
		fmt.Println("✅ Migration rollback completed successfully!")
		fmt.Printf("Database is now at version: %d\n", finalStatus.CurrentVersion)
	}

	return nil
}
