package migrateup

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

var migrateUpCmd = &cobra.Command{
	Use:   "migrate-up",
	Short: "Run pending migrations up to the latest version",
	Long: `Run all pending database migrations up to the latest version.

This command applies all migrations that haven't been applied yet, bringing
the database schema up to the latest version defined in the migration files.

Each migration is run in a transaction, so if any migration fails, it will
be rolled back and the migration process will stop.`,
	RunE: migrateUpCommand,
}

const (
	dbURLFlag            = "db-url"
	migrationsFlag       = "migrations-dir"
	dryRunFlag           = "dry-run"
	verboseFlag          = "verbose"
	lockTimeoutFlag      = "lock-timeout"
	statementTimeoutFlag = "statement-timeout"
)

var migrateUpFlags = map[string]cobraflags.Flag{
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
	dryRunFlag: &cobraflags.BoolFlag{
		Name:  dryRunFlag,
		Value: false,
		Usage: "Show what migrations would be applied without actually running them",
	},
	verboseFlag: &cobraflags.BoolFlag{
		Name:  verboseFlag,
		Value: false,
		Usage: "Enable verbose output",
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

func NewMigrateUpCommand() *cobra.Command {
	cobraflags.RegisterMap(migrateUpCmd, migrateUpFlags)
	return migrateUpCmd
}

func migrateUpCommand(_ *cobra.Command, _ []string) error {
	dbURL := migrateUpFlags[dbURLFlag].GetString()
	migrationsDir := migrateUpFlags[migrationsFlag].GetString()
	dryRun := migrateUpFlags[dryRunFlag].GetBool()
	verbose := migrateUpFlags[verboseFlag].GetBool()
	lockTimeout := migrateUpFlags[lockTimeoutFlag].GetString()
	statementTimeout := migrateUpFlags[statementTimeoutFlag].GetString()

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	if verbose {
		fmt.Printf("Connecting to database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	}

	timeouts, err := migrator.ParseMigrationTimeouts(lockTimeout, statementTimeout)
	if err != nil {
		return err
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(migrateUpFlags[dbcli.ConnectTimeoutFlagName].GetString())
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

	fmt.Println("=== MIGRATE UP ===")
	fmt.Printf("Database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	fmt.Printf("Dialect: %s\n", conn.Info().Dialect)
	fmt.Printf("Migrations directory: %s\n", migrationsDir)
	fmt.Println()

	// Create filesystem from migrations directory
	migrationsFS := os.DirFS(migrationsDir)

	// Online-DDL routing: `-- +ptah online_ddl_tool=...` directives always
	// work; the ptah.yaml online_ddl section adds automatic routing of
	// ALTERs on tables above the configured row threshold.
	onlineCfg, err := dbcli.LoadOnlineDDLConfig(migrateUpFlags[dbcli.ConfigFlagName].GetString())
	if err != nil {
		return err
	}
	if onlineCfg.Enabled() {
		fmt.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
	}
	interceptor := onlineddl.New(*onlineCfg).WithDryRun(dryRun)

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
	fmt.Printf("Pending migrations: %d\n", len(status.PendingMigrations))

	if !status.HasPendingChanges {
		fmt.Println("✅ Database is already up to date!")
		return nil
	}

	if verbose {
		fmt.Printf("Pending migration versions: %v\n", status.PendingMigrations)
	}

	fmt.Println()

	// Run migrations
	err = mig.MigrateUp(context.Background())
	if err != nil {
		return fmt.Errorf("error running migrations: %w", err)
	}

	// Get final status
	finalStatus, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting final migration status: %w", err)
	}

	fmt.Println()
	if dryRun {
		fmt.Println("✅ Dry run completed successfully!")
		fmt.Printf("Would have applied %d migrations\n", len(status.PendingMigrations))
	} else {
		fmt.Println("✅ Migrations completed successfully!")
		fmt.Printf("Database is now at version: %d\n", finalStatus.CurrentVersion)
	}

	return nil
}
