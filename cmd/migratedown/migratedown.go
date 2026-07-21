package migratedown

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/onlineddl"
	"github.com/stokaro/ptah/internal/preflight"
	"github.com/stokaro/ptah/migration/migrator"
)

var migrateDownCmd = &cobra.Command{
	Use:   "down",
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
	dbURLFlag                = "db-url"
	migrationsFlag           = "migrations-dir"
	targetFlag               = "target"
	dirFormatFlag            = "dir-format"
	atlasEnvFlag             = "atlas-env"
	dryRunFlag               = "dry-run"
	verboseFlag              = "verbose"
	confirmFlag              = "confirm"
	execOrderFlag            = "exec-order"
	migrationLockTimeoutFlag = "migration-lock-timeout"
	lockTimeoutFlag          = "lock-timeout"
	statementTimeoutFlag     = "statement-timeout"
	preDownHookFlag          = "pre-down-hook"
	pgDumpToFlag             = "pg-dump-to"
	mySQLDumpToFlag          = "mysqldump-to"
	webhookFlag              = "webhook"
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
	targetFlag: &cobraflags.StringFlag{
		Name:  targetFlag,
		Value: "0",
		Usage: "Target version to migrate down to (required)",
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
	execOrderFlag: &cobraflags.StringFlag{
		Name:  execOrderFlag,
		Value: string(migrator.ExecOrderLinear),
		Usage: "Execution order policy for pending migrations below the current version: linear, linear-skip, or non-linear",
	},
	migrationLockTimeoutFlag: &cobraflags.StringFlag{
		Name:  migrationLockTimeoutFlag,
		Value: "",
		Usage: "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m",
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
	preDownHookFlag: &cobraflags.StringFlag{
		Name:  preDownHookFlag,
		Value: "",
		Usage: "Shell command to run before rolling back migrations; aborts unless it exits 0",
	},
	pgDumpToFlag: &cobraflags.StringFlag{
		Name:  pgDumpToFlag,
		Value: "",
		Usage: "Directory where pg_dump writes a custom-format backup before rolling back migrations",
	},
	mySQLDumpToFlag: &cobraflags.StringFlag{
		Name:  mySQLDumpToFlag,
		Value: "",
		Usage: "Directory where mysqldump writes a SQL backup before rolling back migrations",
	},
	webhookFlag: &cobraflags.StringFlag{
		Name:  webhookFlag,
		Value: "",
		Usage: "Webhook URL to POST migration metadata before rolling back migrations; must return HTTP 200",
	},
	dbcli.ConnectTimeoutFlagName:      dbcli.NewConnectTimeoutFlag(),
	dbcli.ConfigFlagName:              dbcli.NewConfigFlag(),
	dbcli.EnvFlagName:                 dbcli.NewEnvFlag(),
	dbcli.MigrationsSchemaFlagName:    dbcli.NewMigrationsSchemaFlag(),
	dbcli.MigrationsTableFlagName:     dbcli.NewMigrationsTableFlag(),
	dbcli.RevisionTableFormatFlagName: dbcli.NewRevisionTableFormatFlag(),
}

var migrateDownFlagsRegistered bool

func NewMigrateDownCommand() *cobra.Command {
	if !migrateDownFlagsRegistered {
		cobraflags.RegisterMap(migrateDownCmd, migrateDownFlags)
		migrateDownFlagsRegistered = true
	}
	cmdutil.ConfigureCommand(migrateDownCmd)
	return migrateDownCmd
}

func migrateDownCommand(cmd *cobra.Command, _ []string) error {
	dbURL := migrateDownFlags[dbURLFlag].GetString()
	migrationsDir := migrateDownFlags[migrationsFlag].GetString()
	targetVersionValue := migrateDownFlags[targetFlag].GetString()
	dirFormatValue := migrateDownFlags[dirFormatFlag].GetString()
	atlasEnv := migrateDownFlags[atlasEnvFlag].GetString()
	dryRun := migrateDownFlags[dryRunFlag].GetBool()
	verbose := migrateDownFlags[verboseFlag].GetBool()
	skipConfirm := migrateDownFlags[confirmFlag].GetBool()
	execOrderValue := migrateDownFlags[execOrderFlag].GetString()
	migrationLockTimeoutValue := migrateDownFlags[migrationLockTimeoutFlag].GetString()
	lockTimeout := migrateDownFlags[lockTimeoutFlag].GetString()
	statementTimeout := migrateDownFlags[statementTimeoutFlag].GetString()
	preDownHook := migrateDownFlags[preDownHookFlag].GetString()
	pgDumpTo := migrateDownFlags[pgDumpToFlag].GetString()
	mySQLDumpTo := migrateDownFlags[mySQLDumpToFlag].GetString()
	webhook := migrateDownFlags[webhookFlag].GetString()
	migrationsSchema := migrateDownFlags[dbcli.MigrationsSchemaFlagName].GetString()
	migrationsTable := migrateDownFlags[dbcli.MigrationsTableFlagName].GetString()
	revisionFormatValue := migrateDownFlags[dbcli.RevisionTableFormatFlagName].GetString()
	configPath := migrateDownFlags[dbcli.ConfigFlagName].GetString()

	projectCfg, err := dbcli.LoadProjectConfig(cmd, configPath)
	if err != nil {
		return err
	}
	dbURL = dbcli.EffectiveString(cmd, dbURLFlag, dbURL, projectCfg.DatabaseURL)
	migrationsDir = dbcli.EffectiveString(cmd, migrationsFlag, migrationsDir, projectCfg.Migration.Dir)
	dirFormatValue = dbcli.EffectiveString(cmd, dirFormatFlag, dirFormatValue, projectCfg.Migration.Format)
	atlasEnv = dbcli.EffectiveString(cmd, atlasEnvFlag, atlasEnv, projectCfg.EnvName)
	execOrderValue = dbcli.EffectiveString(cmd, execOrderFlag, execOrderValue, projectCfg.Migration.ExecOrder)
	migrationLockTimeoutValue = dbcli.EffectiveString(cmd, migrationLockTimeoutFlag, migrationLockTimeoutValue, projectCfg.Migration.MigrationLockTimeout)
	lockTimeout = dbcli.EffectiveString(cmd, lockTimeoutFlag, lockTimeout, projectCfg.Migration.LockTimeout)
	statementTimeout = dbcli.EffectiveString(cmd, statementTimeoutFlag, statementTimeout, projectCfg.Migration.StatementTimeout)
	preDownHook = dbcli.EffectiveString(cmd, preDownHookFlag, preDownHook, projectCfg.Migration.PreDownHook)
	pgDumpTo = dbcli.EffectiveString(cmd, pgDumpToFlag, pgDumpTo, projectCfg.Migration.PostgresDumpTo)
	mySQLDumpTo = dbcli.EffectiveString(cmd, mySQLDumpToFlag, mySQLDumpTo, projectCfg.Migration.MySQLDumpTo)
	webhook = dbcli.EffectiveString(cmd, webhookFlag, webhook, projectCfg.Migration.Webhook)
	migrationsSchema = dbcli.EffectiveString(cmd, dbcli.MigrationsSchemaFlagName, migrationsSchema, projectCfg.Migration.RevisionsSchema)
	migrationsTable = dbcli.EffectiveString(cmd, dbcli.MigrationsTableFlagName, migrationsTable, projectCfg.Migration.RevisionsTable)
	revisionFormatValue = dbcli.EffectiveString(cmd, dbcli.RevisionTableFormatFlagName, revisionFormatValue, projectCfg.Migration.RevisionFormat)
	connectTimeoutValue := dbcli.EffectiveString(cmd, dbcli.ConnectTimeoutFlagName, migrateDownFlags[dbcli.ConnectTimeoutFlagName].GetString(), projectCfg.Migration.ConnectTimeout)

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	targetVersion, err := strconv.ParseInt(targetVersionValue, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid target version %q: %w", targetVersionValue, err)
	}
	if targetVersion < 0 {
		return fmt.Errorf("target version must be >= 0")
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(revisionFormatValue)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Printf("Connecting to database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	}

	timeouts, err := migrator.ParseMigrationTimeouts(lockTimeout, statementTimeout)
	if err != nil {
		return err
	}
	execOrder, err := migrator.ParseExecOrder(execOrderValue)
	if err != nil {
		return err
	}
	migrationLockTimeout, err := migrator.ParseMigrationLockTimeout(migrationLockTimeoutValue)
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

	// Set dry run mode if requested
	conn.SchemaWriter().SetDryRun(dryRun)

	if dryRun {
		fmt.Println("=== DRY RUN MODE ===")
		fmt.Println("No actual changes will be made to the database")
		fmt.Println()
	}

	fmt.Println("=== MIGRATE DOWN ===")
	fmt.Printf("Database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	fmt.Printf("Dialect: %s\n", conn.Info().Dialect)
	fmt.Printf("Migrations directory: %s\n", migrationsDir)
	fmt.Printf("Migration directory format: %s\n", dirFormat)
	fmt.Printf("Target version: %d\n", targetVersion)
	fmt.Println()

	// Create filesystem from migrations directory
	migrationsFS := os.DirFS(migrationsDir)

	// Online-DDL routing works for down migrations too: a rollback ALTER on
	// a large table is just as lock-heavy as the forward one.
	onlineCfg, err := dbcli.LoadOnlineDDLConfigForEnv(configPath, projectCfg.EnvName)
	if err != nil {
		return err
	}
	if onlineCfg.Enabled() {
		fmt.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
	}
	interceptor := onlineddl.New(*onlineCfg).WithDryRun(dryRun)

	// Create migrator to access applied migrations
	mig, err := migrator.NewFSMigrator(
		conn,
		migrationsFS,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: atlasEnv}),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(migrationsSchema, migrationsTable).
		WithRevisionTableFormat(revisionFormat).
		WithDefaultTimeouts(timeouts).
		WithExecOrder(execOrder).
		WithMigrationLockTimeout(migrationLockTimeout)

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
	var migrationsToRollback []int64
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

	preflightHook := dbcli.LockedMigrationPreflightHook(dryRun, preflight.Options{
		Direction:          preflight.DirectionDown,
		DatabaseURL:        dbURL,
		DisplayDatabaseURL: dbschema.FormatDatabaseURL(dbURL),
		Dialect:            conn.Info().Dialect,
		Command:            preDownHook,
		PostgresDumpDir:    pgDumpTo,
		MySQLDumpDir:       mySQLDumpTo,
		WebhookURL:         webhook,
	})

	// Run down migrations
	err = mig.MigrateDownToWithPreflight(context.Background(), targetVersion, preflightHook)
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
