// Package migratedown implements "ptah migrations down", which rolls back
// applied migrations to a target version against a live database.
package migratedown

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cliobs"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/devdocker"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/internal/onlineddl"
	"go.5x5.cz/ptah/internal/preflight"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	dbURLFlag                = "db-url"
	migrationsFlag           = "migrations-dir"
	targetFlag               = "target"
	shadowDBFlag             = "shadow-db"
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
	plainHTTPFlag            = "plain-http"
	verifySumFlag            = "verify-sum"
)

type options struct {
	dbURL                string
	migrationsDir        string
	target               string
	shadowDB             string
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
	plainHTTP            bool
	verifySum            bool
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
	flags.StringVar(&opts.migrationsDir, migrationsFlag, "", "Local directory or oci:// reference containing migration files (required)")
	flags.StringVar(&opts.target, targetFlag, "0", "Target version to migrate down to (required)")
	flags.StringVar(&opts.shadowDB, shadowDBFlag, "", "Ephemeral shadow database URL where the rollback plan is replayed and verified before touching the target")
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
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	flags.BoolVar(
		&opts.verifySum,
		verifySumFlag,
		false,
		migrationsource.VerifySumUsage(
			"Require a sum file: a missing ptah.sum or atlas.sum is an error "+
				"(hashed directories always verify before rolling back)",
		),
	)
	flags.StringVar(&opts.logFormat, cliobs.LogFormatFlagName, "text", "Log format: text or json")
	flags.StringVar(&opts.logLevel, cliobs.LogLevelFlagName, "info", "Log level: debug, info, warn, or error")
	flags.StringVar(&opts.metricsAddr, cliobs.MetricsAddrFlagName, "", "Address for the Prometheus /metrics endpoint, such as :9090")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterAtlasProjectInternalFlags(flags)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
}

func resolveProjectOptions(cmd *cobra.Command, opts options, projectCfg projectconfig.Config) options {
	effectiveString := func(flagName, flagValue string, field projectconfig.StringField) string {
		return dbcli.EffectiveString(cmd, flagName, flagValue, projectCfg.StringValue(field))
	}
	opts.dbURL = effectiveString(dbURLFlag, opts.dbURL, projectconfig.StringDatabaseURL)
	opts.migrationsDir = effectiveString(migrationsFlag, opts.migrationsDir, projectconfig.StringMigrationDir)
	opts.shadowDB = effectiveString(shadowDBFlag, opts.shadowDB, projectconfig.StringDevURL)
	opts.dirFormat = effectiveString(dirFormatFlag, opts.dirFormat, projectconfig.StringMigrationFormat)
	opts.atlasEnv = effectiveString(atlasEnvFlag, opts.atlasEnv, projectconfig.StringEnvName)
	opts.execOrder = effectiveString(execOrderFlag, opts.execOrder, projectconfig.StringMigrationExecOrder)
	opts.migrationLockTimeout = effectiveString(
		migrationLockTimeoutFlag,
		opts.migrationLockTimeout,
		projectconfig.StringMigrationMigrationLockTimeout,
	)
	opts.lockTimeout = effectiveString(lockTimeoutFlag, opts.lockTimeout, projectconfig.StringMigrationLockTimeout)
	opts.statementTimeout = effectiveString(
		statementTimeoutFlag,
		opts.statementTimeout,
		projectconfig.StringMigrationStatementTimeout,
	)
	opts.preDownHook = effectiveString(preDownHookFlag, opts.preDownHook, projectconfig.StringMigrationPreDownHook)
	opts.pgDumpTo = effectiveString(pgDumpToFlag, opts.pgDumpTo, projectconfig.StringMigrationPostgresDumpTo)
	opts.mySQLDumpTo = effectiveString(mySQLDumpToFlag, opts.mySQLDumpTo, projectconfig.StringMigrationMySQLDumpTo)
	opts.webhook = effectiveString(webhookFlag, opts.webhook, projectconfig.StringMigrationWebhook)
	opts.migrationsSchema = effectiveString(
		dbcli.MigrationsSchemaFlagName,
		opts.migrationsSchema,
		projectconfig.StringMigrationRevisionsSchema,
	)
	opts.migrationsTable = effectiveString(
		dbcli.MigrationsTableFlagName,
		opts.migrationsTable,
		projectconfig.StringMigrationRevisionsTable,
	)
	opts.revisionTableFormat = effectiveString(
		dbcli.RevisionTableFormatFlagName,
		opts.revisionTableFormat,
		projectconfig.StringMigrationRevisionFormat,
	)
	opts.connectTimeout = effectiveString(
		dbcli.ConnectTimeoutFlagName,
		opts.connectTimeout,
		projectconfig.StringMigrationConnectTimeout,
	)
	return opts
}

func migrateDownCommand(cmd *cobra.Command, opts *options) error {
	integrityPolicy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return err
	}
	resolvedOpts := resolveProjectOptions(cmd, *opts, projectCfg)
	dbURL := resolvedOpts.dbURL
	migrationsDir := resolvedOpts.migrationsDir
	targetVersionValue := resolvedOpts.target
	dirFormatValue := resolvedOpts.dirFormat
	atlasEnv := resolvedOpts.atlasEnv
	execOrderValue := resolvedOpts.execOrder
	migrationLockTimeoutValue := resolvedOpts.migrationLockTimeout
	lockTimeout := resolvedOpts.lockTimeout
	statementTimeout := resolvedOpts.statementTimeout
	preDownHook := resolvedOpts.preDownHook
	pgDumpTo := resolvedOpts.pgDumpTo
	mySQLDumpTo := resolvedOpts.mySQLDumpTo
	webhook := resolvedOpts.webhook
	migrationsSchema := resolvedOpts.migrationsSchema
	migrationsTable := resolvedOpts.migrationsTable
	revisionFormatValue := resolvedOpts.revisionTableFormat
	connectTimeoutValue := resolvedOpts.connectTimeout

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
	source, err := migrationsource.Resolve(cmd.Context(), migrationsDir, migrationsource.Options{
		DirFormat: dirFormat,
		PlainHTTP: opts.plainHTTP,
	})
	if err != nil {
		return err
	}
	migrationsFS := source.FileSystem
	migrationsDir = source.Display
	dirFormat = source.DirFormat

	// The integrity gate runs before the database connection, before the
	// confirmation prompt, and before any rollback SQL is read: a run that is
	// going to refuse must not first ask the operator to type YES, and must not
	// hold a connection while it decides. `migrations up` has verified here
	// since stokaro/ptah#955; `down` did not, so a rewritten `_init.down.sql`
	// under a stale ptah.sum executed at exit 0 while `up` on the same
	// directory exited 2.
	//
	// --verify-sum adds the half the always-on gate deliberately does not
	// cover: a directory carrying NO sum passes the gate, because there is no
	// recorded intent to compare it against, and until stokaro/ptah#928 item 4
	// `down` had no spelling that could demand one. It also qualifies what a
	// verification through a movable OCI tag actually established, which is the
	// same sentence `up` prints — shared, not copied, so the destructive verb
	// cannot end up saying less than the constructive one again.
	//
	// integrityPolicy was resolved at the command boundary, above, so the
	// escape hatch is decided once for the whole invocation rather than read
	// again here.
	if err := migrationsource.Verify(
		cmd.ErrOrStderr(), emit, runtime, source, dirFormat, integrityPolicy,
		migrationsource.VerifyOptions{RequireSum: opts.verifySum, Verbose: opts.verbose},
	); err != nil {
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

	// Online-DDL routing works for down migrations too: a rollback ALTER on
	// a large table is just as lock-heavy as the forward one.
	onlineCfg := projectCfg.OnlineDDL
	if onlineCfg.Enabled() {
		emit.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
	}
	interceptor := onlineddl.New(onlineCfg).WithDryRun(opts.dryRun)

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
		observeNoopDown(runtime, conn.Info().Dialect, status.CurrentVersion, targetVersion)
		emit.Printf("✅ Database is already at or below target version %d!\n", targetVersion)
		return nil
	}

	// Get applied migrations from the database
	appliedMigrations, err := mig.GetAppliedMigrations(context.Background())
	if err != nil {
		return fmt.Errorf("error getting applied migrations: %w", err)
	}

	// Calculate which migrations will be rolled back
	migrationsToRollback := versionsAboveTarget(appliedMigrations, targetVersion)

	emit.Printf("Migrations to roll back: %d\n", len(migrationsToRollback))

	if opts.verbose {
		emit.Printf("Will roll back from version %d to %d\n", status.CurrentVersion, targetVersion)
		if len(migrationsToRollback) > 0 {
			emit.Printf("Specific migrations to rollback: %v\n", migrationsToRollback)
		}
	}

	emit.Println()

	if err := verifyRollbackOnShadow(cmd.Context(), shadowVerification{
		targetConnection: conn,
		shadowDB:         resolvedOpts.shadowDB,
		migrationsFS:     migrationsFS,
		currentVersion:   status.CurrentVersion,
		targetVersion:    targetVersion,
		dirFormat:        dirFormat,
		atlasEnv:         atlasEnv,
		connectTimeout:   connectTimeout,
	}, emit); err != nil {
		return err
	}

	// Safety confirmation (unless skipped or dry run)
	confirmed, err := confirmRollbackPrompt(cmd, opts, status.CurrentVersion, targetVersion, migrationsToRollback)
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

func versionsAboveTarget(appliedMigrations []int64, targetVersion int64) []int64 {
	var versions []int64
	for _, version := range appliedMigrations {
		if version > targetVersion {
			versions = append(versions, version)
		}
	}
	return versions
}

func observeNoopDown(runtime *cliobs.Runtime, dialect string, currentVersion, targetVersion int64) {
	cliobs.ObserveNoopMigration(context.Background(), runtime.Observer(), "ptah.migrate.down",
		migrator.ObservationAttribute{Key: "db.system", Value: dialect},
		migrator.ObservationAttribute{Key: "migration.direction", Value: "down"},
		migrator.ObservationAttribute{Key: "migration.current_version", Value: currentVersion},
		migrator.ObservationAttribute{Key: "migration.target_version", Value: currentVersion},
		migrator.ObservationAttribute{Key: "migration.requested_target_version", Value: targetVersion},
		migrator.ObservationAttribute{Key: "migration.pending_count", Value: 0},
	)
}

type shadowVerification struct {
	targetConnection *dbschema.DatabaseConnection
	shadowDB         string
	migrationsFS     fs.FS
	currentVersion   int64
	targetVersion    int64
	dirFormat        migrator.MigrationDirFormat
	atlasEnv         string
	connectTimeout   time.Duration
}

// verifyRollbackOnShadow replays the rollback plan on a disposable shadow
// database first, so a down file that fails or a missing down migration aborts
// before the target is touched (and before the operator is asked to confirm).
// Without --shadow-db it is a no-op, keeping the default output unchanged.
func verifyRollbackOnShadow(
	ctx context.Context,
	v shadowVerification,
	emit cliobs.Emitter,
) error {
	if v.shadowDB == "" {
		return nil
	}
	// A docker:// shadow database is provisioned here, after the no-op check
	// above, so a run with no verification to do never starts a container.
	shadowDB, releaseShadow, err := devdocker.Resolve(ctx, v.shadowDB, devdocker.Options{})
	if err != nil {
		return err
	}
	defer releaseShadow()

	err = generator.VerifyRollbackFromShadow(ctx, generator.RollbackFromShadowOptions{
		TargetConnection:  v.targetConnection,
		ShadowDatabaseURL: shadowDB,
		FS:                v.migrationsFS,
		CurrentVersion:    v.currentVersion,
		TargetVersion:     v.targetVersion,
		ProviderOptions: []migrator.FSProviderOption{
			migrator.WithMigrationDirFormat(v.dirFormat),
			migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: v.atlasEnv}),
		},
		ConnectTimeout: v.connectTimeout,
	})
	if err != nil {
		return err
	}
	emit.Println("✅ Rollback plan verified on shadow database")
	emit.Println()
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

// confirmRollbackPrompt routes the confirmation prompt to stdout, or to stderr
// under JSON log format where stdout carries structured output.
func confirmRollbackPrompt(cmd *cobra.Command, opts *options, currentVersion, targetVersion int64, migrationsToRollback []int64) (bool, error) {
	promptWriter := cmd.OutOrStdout()
	if opts.logFormat == "json" {
		promptWriter = cmd.ErrOrStderr()
	}
	return confirmRollback(opts, promptWriter, cmd.InOrStdin(), currentVersion, targetVersion, migrationsToRollback)
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
