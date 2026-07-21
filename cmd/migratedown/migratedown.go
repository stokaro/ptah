package migratedown

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cliobs"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/onlineddl"
	"github.com/stokaro/ptah/internal/preflight"
	"github.com/stokaro/ptah/migration/migrator"
)

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

type options struct {
	dbURL                string
	migrationsDir        string
	target               string
	dirFormat            string
	atlasEnv             string
	dryRun               bool
	verbose              bool
	skipConfirm          bool
	execOrder            string
	migrationLockTimeout string
	lockTimeout          string
	statementTimeout     string
	preDownHook          string
	pgDumpTo             string
	mySQLDumpTo          string
	webhook              string
	connectTimeout       string
	configPath           string
	envName              string
	migrationsSchema     string
	migrationsTable      string
	revisionTableFormat  string
	logFormat            string
	logLevel             string
	metricsAddr          string
}

func NewMigrateDownCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
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
		RunE: func(cmd *cobra.Command, _ []string) error {
			return migrateDownCommand(cmd, &opts)
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
	flags.StringVar(&opts.target, targetFlag, "0", "Target version to migrate down to (required)")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Show what migrations would be rolled back without actually running them")
	flags.BoolVar(&opts.verbose, verboseFlag, false, "Enable verbose output")
	flags.BoolVar(&opts.skipConfirm, confirmFlag, false, "Skip confirmation prompt (use with caution!)")
	flags.StringVar(&opts.execOrder, execOrderFlag, string(migrator.ExecOrderLinear), "Execution order policy for pending migrations below the current version: linear, linear-skip, or non-linear")
	flags.StringVar(&opts.migrationLockTimeout, migrationLockTimeoutFlag, "", "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m")
	flags.StringVar(&opts.lockTimeout, lockTimeoutFlag, "", "Default per-migration lock timeout, such as 3s or 500ms")
	flags.StringVar(&opts.statementTimeout, statementTimeoutFlag, "", "Default per-migration statement timeout, such as 30s or 2m")
	flags.StringVar(&opts.preDownHook, preDownHookFlag, "", "Shell command to run before rolling back migrations; aborts unless it exits 0")
	flags.StringVar(&opts.pgDumpTo, pgDumpToFlag, "", "Directory where pg_dump writes a custom-format backup before rolling back migrations")
	flags.StringVar(&opts.mySQLDumpTo, mySQLDumpToFlag, "", "Directory where mysqldump writes a SQL backup before rolling back migrations")
	flags.StringVar(&opts.webhook, webhookFlag, "", "Webhook URL to POST migration metadata before rolling back migrations; must return HTTP 200")
	flags.StringVar(&opts.logFormat, cliobs.LogFormatFlagName, "text", "Log format: text or json")
	flags.StringVar(&opts.logLevel, cliobs.LogLevelFlagName, "info", "Log level: debug, info, warn, or error")
	flags.StringVar(&opts.metricsAddr, cliobs.MetricsAddrFlagName, "", "Address for the Prometheus /metrics endpoint, such as :9090")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
}

func migrateDownCommand(cmd *cobra.Command, opts *options) error {
	dbURL := opts.dbURL
	migrationsDir := opts.migrationsDir
	targetVersionValue := opts.target
	dirFormatValue := opts.dirFormat
	atlasEnv := opts.atlasEnv
	execOrderValue := opts.execOrder
	migrationLockTimeoutValue := opts.migrationLockTimeout
	lockTimeout := opts.lockTimeout
	statementTimeout := opts.statementTimeout
	preDownHook := opts.preDownHook
	pgDumpTo := opts.pgDumpTo
	mySQLDumpTo := opts.mySQLDumpTo
	webhook := opts.webhook
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
	connectTimeoutValue := dbcli.EffectiveString(cmd, dbcli.ConnectTimeoutFlagName, opts.connectTimeout, projectCfg.Migration.ConnectTimeout)

	runtime, err := startObservability(cmd, opts)
	if err != nil {
		return err
	}
	defer shutdownObservability(runtime)
	emit := cliobs.NewEmitter(cmd.OutOrStdout(), runtime)

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

	if opts.verbose {
		emit.Printf("Connecting to database: %s\n", dbschema.FormatDatabaseURL(dbURL))
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
	conn.SchemaWriter().SetDryRun(opts.dryRun)

	if opts.dryRun {
		emit.Println("=== DRY RUN MODE ===")
		emit.Println("No actual changes will be made to the database")
		emit.Println()
	}

	emit.Println("=== MIGRATE DOWN ===")
	emit.Printf("Database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	emit.Printf("Dialect: %s\n", conn.Info().Dialect)
	emit.Printf("Migrations directory: %s\n", migrationsDir)
	emit.Printf("Migration directory format: %s\n", dirFormat)
	emit.Printf("Target version: %d\n", targetVersion)
	emit.Println()

	// Create filesystem from migrations directory
	migrationsFS := os.DirFS(migrationsDir)

	// Online-DDL routing works for down migrations too: a rollback ALTER on
	// a large table is just as lock-heavy as the forward one.
	onlineCfg, err := dbcli.LoadOnlineDDLConfigForEnv(opts.configPath, projectCfg.EnvName)
	if err != nil {
		return err
	}
	if onlineCfg.Enabled() {
		emit.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
	}
	interceptor := onlineddl.New(*onlineCfg).WithDryRun(opts.dryRun)

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
		WithMigrationLockTimeout(migrationLockTimeout).
		WithLogger(runtime.Logger()).
		WithObserver(runtime.Observer())

	// Get migration status before running
	status, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting migration status: %w", err)
	}

	emit.Printf("Current version: %d\n", status.CurrentVersion)
	emit.Printf("Total migrations: %d\n", status.TotalMigrations)

	if status.CurrentVersion <= targetVersion {
		cliobs.ObserveNoopMigration(context.Background(), runtime.Observer(), "ptah.migrate.down",
			migrator.ObservationAttribute{Key: "db.system", Value: conn.Info().Dialect},
			migrator.ObservationAttribute{Key: "migration.direction", Value: "down"},
			migrator.ObservationAttribute{Key: "migration.current_version", Value: status.CurrentVersion},
			migrator.ObservationAttribute{Key: "migration.target_version", Value: status.CurrentVersion},
			migrator.ObservationAttribute{Key: "migration.requested_target_version", Value: targetVersion},
			migrator.ObservationAttribute{Key: "migration.pending_count", Value: 0},
		)
		emit.Printf("✅ Database is already at or below target version %d!\n", targetVersion)
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

	emit.Printf("Migrations to roll back: %d\n", len(migrationsToRollback))

	if opts.verbose {
		emit.Printf("Will roll back from version %d to %d\n", status.CurrentVersion, targetVersion)
		if len(migrationsToRollback) > 0 {
			emit.Printf("Specific migrations to rollback: %v\n", migrationsToRollback)
		}
	}

	emit.Println()

	// Safety confirmation (unless skipped or dry run)
	promptWriter := cmd.OutOrStdout()
	if opts.logFormat == "json" {
		promptWriter = cmd.ErrOrStderr()
	}
	confirmed, err := confirmRollback(opts, promptWriter, cmd.InOrStdin(), status.CurrentVersion, targetVersion, migrationsToRollback)
	if err != nil {
		return err
	}
	if !confirmed {
		return nil
	}

	preflightHook := dbcli.LockedMigrationPreflightHook(opts.dryRun, preflight.Options{
		Direction:          preflight.DirectionDown,
		DatabaseURL:        dbURL,
		DisplayDatabaseURL: dbschema.FormatDatabaseURL(dbURL),
		Dialect:            conn.Info().Dialect,
		Command:            preDownHook,
		PostgresDumpDir:    pgDumpTo,
		MySQLDumpDir:       mySQLDumpTo,
		WebhookURL:         webhook,
	}, emit, cliobs.NewOutputWriter(cmd.OutOrStdout(), runtime, "pre-flight output"))

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

	emit.Println()
	if opts.dryRun {
		emit.Println("✅ Dry run completed successfully!")
		emit.Printf("Would have rolled back to version: %d\n", targetVersion)
		if len(migrationsToRollback) > 0 {
			emit.Printf("Would have rolled back these migrations: %v\n", migrationsToRollback)
		}
	} else {
		emit.Println("✅ Migration rollback completed successfully!")
		emit.Printf("Database is now at version: %d\n", finalStatus.CurrentVersion)
	}

	return nil
}

func startObservability(cmd *cobra.Command, opts *options) (*cliobs.Runtime, error) {
	logWriter := cmd.ErrOrStderr()
	if opts.logFormat == "json" {
		logWriter = cmd.OutOrStdout()
	}
	return cliobs.Start(context.Background(), cliobs.Options{
		Command:     "migrations.down",
		LogFormat:   opts.logFormat,
		LogLevel:    opts.logLevel,
		MetricsAddr: opts.metricsAddr,
		LogWriter:   logWriter,
	})
}

func confirmRollback(opts *options, prompt io.Writer, input io.Reader, currentVersion, targetVersion int64, migrationsToRollback []int64) (bool, error) {
	if opts.dryRun || opts.skipConfirm {
		return true, nil
	}

	fmt.Fprintln(prompt, "⚠️  WARNING: Rolling back migrations can result in data loss!")
	fmt.Fprintf(prompt, "This will roll back the database from version %d to version %d.\n", currentVersion, targetVersion)
	if len(migrationsToRollback) > 0 {
		fmt.Fprintf(prompt, "The following %d migration(s) will be rolled back: %v\n", len(migrationsToRollback), migrationsToRollback)
	}
	fmt.Fprint(prompt, "Are you sure you want to continue? Type 'YES' to confirm: ")

	var confirmation string
	if _, err := fmt.Fscan(input, &confirmation); err != nil {
		return false, fmt.Errorf("read rollback confirmation: %w", err)
	}

	if confirmation != "YES" {
		fmt.Fprintln(prompt, "Migration rollback canceled.")
		return false, nil
	}
	fmt.Fprintln(prompt)
	return true, nil
}

func shutdownObservability(runtime *cliobs.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		runtime.Logger().Warn("failed to shut down observability", "error", err)
	}
}
