// Package migrateup implements "ptah migrations up", which applies pending
// migrations to a live database with optional lint checks, apply limits, and
// online-DDL configuration.
package migrateup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cliobs"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/migrationsource"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/deploymentreport"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/onlineddl"
	"github.com/stokaro/ptah/internal/preflight"
	"github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/risk"
)

const (
	dbURLFlag                = "db-url"
	migrationsFlag           = "migrations-dir"
	dryRunFlag               = "dry-run"
	verboseFlag              = "verbose"
	verifySumFlag            = "verify-sum"
	dirFormatFlag            = "dir-format"
	atlasEnvFlag             = "atlas-env"
	execOrderFlag            = "exec-order"
	txModeFlag               = "tx-mode"
	migrationLockTimeoutFlag = "migration-lock-timeout"
	lockTimeoutFlag          = "lock-timeout"
	statementTimeoutFlag     = "statement-timeout"
	allowDestructiveFlag     = "allow-destructive"
	allowDirtyFlag           = "allow-dirty"
	limitFlag                = "limit"
	skipChecksFlag           = "skip-checks"
	preUpHookFlag            = "pre-up-hook"
	pgDumpToFlag             = "pg-dump-to"
	mySQLDumpToFlag          = "mysqldump-to"
	webhookFlag              = "webhook"
	plainHTTPFlag            = "plain-http"
	skipReportFlag           = "skip-report"
)

type options struct {
	dbURL                string
	migrationsDir        string
	dryRun               bool
	verbose              bool
	verifySum            bool
	dirFormat            string
	atlasEnv             string
	execOrder            string
	txMode               string
	migrationLockTimeout string
	lockTimeout          string
	statementTimeout     string
	allowDestructive     bool
	allowDirty           bool
	limit                uint64
	skipChecks           bool
	preUpHook            string
	pgDumpTo             string
	mySQLDumpTo          string
	webhook              string
	plainHTTP            bool
	skipReport           bool
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

type parsedMigrationSettings struct {
	dirFormat            migrator.MigrationDirFormat
	revisionFormat       migrator.RevisionTableFormat
	execOrder            migrator.ExecOrder
	txMode               migrator.MigrationTxMode
	migrationLockTimeout time.Duration
	connectTimeout       time.Duration
}

type deploymentReportPublication struct {
	source     *migrationsource.OCI
	dialect    string
	before     *migrator.MigrationStatus
	after      *migrator.MigrationStatus
	startedAt  time.Time
	finishedAt time.Time
	dryRun     bool
	skip       bool
}

func NewMigrateUpCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Run pending migrations up to the latest version",
		Long: `Run all pending database migrations up to the latest version.

This command applies all migrations that haven't been applied yet, bringing
the database schema up to the latest version defined in the migration files.

By default, each migration file is run in its own transaction unless the file
explicitly opts out with -- +ptah no_transaction. Use --tx-mode=all to wrap the
whole pending up batch in one transaction on supported dialects, or
--tx-mode=none to run without migration transaction wrapping.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return migrateUpCommand(cmd, &opts)
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
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Show what migrations would be applied without actually running them")
	flags.BoolVar(&opts.verbose, verboseFlag, false, "Enable verbose output")
	flags.BoolVar(&opts.verifySum, verifySumFlag, false, "Verify the migrations directory against its committed ptah.sum before applying; abort on drift")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.StringVar(&opts.execOrder, execOrderFlag, string(migrator.ExecOrderLinear), "Execution order policy for pending migrations below the current version: linear, linear-skip, or non-linear")
	flags.StringVar(&opts.txMode, txModeFlag, string(migrator.MigrationTxModeFile), "Transaction mode for pending migrations: file, all, or none")
	flags.StringVar(&opts.migrationLockTimeout, migrationLockTimeoutFlag, "", "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m")
	flags.StringVar(&opts.lockTimeout, lockTimeoutFlag, "", "Default per-migration lock timeout, such as 3s or 500ms")
	flags.StringVar(&opts.statementTimeout, statementTimeoutFlag, "", "Default per-migration statement timeout, such as 30s or 2m")
	flags.BoolVar(&opts.allowDestructive, allowDestructiveFlag, false, "Allow pending migrations that contain destructive statements")
	flags.BoolVar(&opts.allowDirty, allowDirtyFlag, false, "Recovery escape hatch: run pending migrations even when the revision table records a dirty (partially applied) migration")
	flags.Uint64Var(&opts.limit, limitFlag, 0, "Apply only the first N pending migrations (0 applies all)")
	flags.BoolVar(&opts.skipChecks, skipChecksFlag, false, "Emergency bypass: skip pre-migration +ptah check assertion checks")
	flags.StringVar(&opts.preUpHook, preUpHookFlag, "", "Shell command to run before applying pending migrations; aborts unless it exits 0")
	flags.StringVar(&opts.pgDumpTo, pgDumpToFlag, "", "Directory where pg_dump writes a custom-format backup before applying migrations")
	flags.StringVar(&opts.mySQLDumpTo, mySQLDumpToFlag, "", "Directory where mysqldump writes a SQL backup before applying migrations")
	flags.StringVar(&opts.webhook, webhookFlag, "", "Webhook URL to POST migration metadata before applying migrations; must return HTTP 200")
	flags.BoolVar(&opts.plainHTTP, plainHTTPFlag, false, "Use plain HTTP for an explicitly trusted local OCI registry")
	flags.BoolVar(&opts.skipReport, skipReportFlag, false, "Do not attach a deployment report after applying an OCI migration artifact")
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

func parseMigrationSettings(
	dirFormatValue string,
	revisionFormatValue string,
	execOrderValue string,
	txModeValue string,
	migrationLockTimeoutValue string,
	connectTimeoutValue string,
) (parsedMigrationSettings, error) {
	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(revisionFormatValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	execOrder, err := migrator.ParseExecOrder(execOrderValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	txMode, err := migrator.ParseMigrationTxMode(txModeValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	migrationLockTimeout, err := migrator.ParseMigrationLockTimeout(migrationLockTimeoutValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(connectTimeoutValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	return parsedMigrationSettings{
		dirFormat:            dirFormat,
		revisionFormat:       revisionFormat,
		execOrder:            execOrder,
		txMode:               txMode,
		migrationLockTimeout: migrationLockTimeout,
		connectTimeout:       connectTimeout,
	}, nil
}

func resolveProjectOptions(cmd *cobra.Command, opts options, projectCfg projectconfig.Config) options {
	effectiveString := func(flagName, flagValue string, field projectconfig.StringField) string {
		return dbcli.EffectiveString(cmd, flagName, flagValue, projectCfg.StringValue(field))
	}
	opts.dbURL = effectiveString(dbURLFlag, opts.dbURL, projectconfig.StringDatabaseURL)
	opts.migrationsDir = effectiveString(migrationsFlag, opts.migrationsDir, projectconfig.StringMigrationDir)
	opts.dirFormat = effectiveString(dirFormatFlag, opts.dirFormat, projectconfig.StringMigrationFormat)
	opts.atlasEnv = effectiveString(atlasEnvFlag, opts.atlasEnv, projectconfig.StringEnvName)
	opts.execOrder = effectiveString(execOrderFlag, opts.execOrder, projectconfig.StringMigrationExecOrder)
	opts.txMode = effectiveString(txModeFlag, opts.txMode, projectconfig.StringMigrationTxMode)
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
	opts.preUpHook = effectiveString(preUpHookFlag, opts.preUpHook, projectconfig.StringMigrationPreUpHook)
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

func migrateUpCommand(cmd *cobra.Command, opts *options) error {
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return err
	}
	resolvedOpts := resolveProjectOptions(cmd, *opts, projectCfg)
	dbURL := resolvedOpts.dbURL
	migrationsDir := resolvedOpts.migrationsDir
	dirFormatValue := resolvedOpts.dirFormat
	atlasEnv := resolvedOpts.atlasEnv
	execOrderValue := resolvedOpts.execOrder
	txModeValue := resolvedOpts.txMode
	migrationLockTimeoutValue := resolvedOpts.migrationLockTimeout
	lockTimeout := resolvedOpts.lockTimeout
	statementTimeout := resolvedOpts.statementTimeout
	preUpHook := resolvedOpts.preUpHook
	pgDumpTo := resolvedOpts.pgDumpTo
	mySQLDumpTo := resolvedOpts.mySQLDumpTo
	webhook := resolvedOpts.webhook
	migrationsSchema := resolvedOpts.migrationsSchema
	migrationsTable := resolvedOpts.migrationsTable
	revisionFormatValue := resolvedOpts.revisionTableFormat
	connectTimeoutValue := resolvedOpts.connectTimeout

	logWriter := cmd.ErrOrStderr()
	if opts.logFormat == "json" {
		logWriter = cmd.OutOrStdout()
	}
	runtime, err := cliobs.Start(context.Background(), cliobs.Options{
		Command:     "migrations.up",
		LogFormat:   opts.logFormat,
		LogLevel:    opts.logLevel,
		MetricsAddr: opts.metricsAddr,
		LogWriter:   logWriter,
	})
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
	settings, err := parseMigrationSettings(
		dirFormatValue,
		revisionFormatValue,
		execOrderValue,
		txModeValue,
		migrationLockTimeoutValue,
		connectTimeoutValue,
	)
	if err != nil {
		return err
	}
	source, err := migrationsource.Resolve(cmd.Context(), migrationsDir, migrationsource.Options{
		DirFormat: settings.dirFormat,
		PlainHTTP: opts.plainHTTP,
	})
	if err != nil {
		return err
	}
	migrationsFS := source.FileSystem
	migrationsDir = source.Display
	settings.dirFormat = source.DirFormat

	if opts.verifySum {
		result, err := verifyMigrationIntegrity(migrationsFS, settings.dirFormat)
		if err != nil {
			return err
		}
		if opts.verbose {
			emit.Printf("%s verified: migrations directory is intact\n", result.SumFileName)
		}
	}

	if opts.verbose {
		emit.Printf("Connecting to database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	}

	timeouts, err := migrator.ParseMigrationTimeouts(lockTimeout, statementTimeout)
	if err != nil {
		return err
	}
	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), settings.connectTimeout)
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

	emit.Println("=== MIGRATE UP ===")
	emit.Printf("Database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	emit.Printf("Dialect: %s\n", conn.Info().Dialect)
	emit.Printf("Migrations directory: %s\n", migrationsDir)
	emit.Printf("Migration directory format: %s\n", settings.dirFormat)
	emit.Printf("Transaction mode: %s\n", settings.txMode)
	emit.Println()

	// Online-DDL routing: `-- +ptah online_ddl_tool=...` directives always
	// work; the ptah.yaml online_ddl section adds automatic routing of
	// ALTERs on tables above the configured row threshold.
	onlineCfg := projectCfg.OnlineDDL
	if onlineCfg.Enabled() {
		emit.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
	}
	interceptor := onlineddl.New(onlineCfg).WithDryRun(opts.dryRun)

	mig, err := migrator.NewFSMigrator(
		conn,
		migrationsFS,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithMigrationDirFormat(settings.dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: atlasEnv}),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(migrationsSchema, migrationsTable).
		WithRevisionTableFormat(settings.revisionFormat).
		WithDefaultTimeouts(timeouts).
		WithExecOrder(settings.execOrder).
		WithTransactionMode(settings.txMode).
		WithMigrationLockTimeout(settings.migrationLockTimeout).
		WithSkipChecks(opts.skipChecks).
		WithLogger(runtime.Logger()).
		WithObserver(runtime.Observer())

	// Get migration status before running
	status, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting migration status: %w", err)
	}

	emit.Printf("Current version: %d\n", status.CurrentVersion)
	emit.Printf("Total migrations: %d\n", status.TotalMigrations)
	emit.Printf("Pending migrations: %d\n", len(status.PendingMigrations))
	if len(status.OutOfOrderMigrations) > 0 {
		emit.Printf("Out-of-order migrations: %v\n", status.OutOfOrderMigrations)
	}

	if !status.HasPendingChanges {
		cliobs.ObserveNoopMigration(context.Background(), runtime.Observer(), "ptah.migrate.up",
			migrator.ObservationAttribute{Key: "db.system", Value: conn.Info().Dialect},
			migrator.ObservationAttribute{Key: "migration.direction", Value: "up"},
			migrator.ObservationAttribute{Key: "migration.current_version", Value: status.CurrentVersion},
			migrator.ObservationAttribute{Key: "migration.target_version", Value: status.CurrentVersion},
			migrator.ObservationAttribute{Key: "migration.pending_count", Value: 0},
		)
		emit.Println("✅ Database is already up to date!")
		return nil
	}

	if opts.verbose {
		emit.Printf("Pending migration versions: %v\n", status.PendingMigrations)
		if len(status.OutOfOrderMigrations) > 0 {
			emit.Printf("Out-of-order migration versions: %v\n", status.OutOfOrderMigrations)
		}
	}
	if settings.execOrder == migrator.ExecOrderLinear && len(status.OutOfOrderMigrations) > 0 {
		return migrator.NewOutOfOrderError(status.CurrentVersion, status.OutOfOrderMigrations)
	}

	emit.Println()
	preflightHook := dbcli.LockedMigrationPreflightHook(opts.dryRun, preflight.Options{
		Direction:          preflight.DirectionUp,
		DatabaseURL:        dbURL,
		DisplayDatabaseURL: dbschema.FormatDatabaseURL(dbURL),
		Dialect:            conn.Info().Dialect,
		Command:            preUpHook,
		PostgresDumpDir:    pgDumpTo,
		MySQLDumpDir:       mySQLDumpTo,
		WebhookURL:         webhook,
	}, emit, cliobs.NewOutputWriter(cmd.OutOrStdout(), runtime, "pre-flight output"))
	if !opts.allowDestructive {
		preflightHook = dbcli.CombineMigrationHooks(
			lockedDestructiveLintHook(migrationsFS, conn.Info().Dialect),
			preflightHook,
		)
	}

	// Run migrations
	startedAt := time.Now()
	err = mig.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{
		Amount:     opts.limit,
		AllowDirty: opts.allowDirty,
		Preflight:  preflightHook,
	})
	if err != nil {
		var checkErr *migrator.CheckFailedError
		if errors.As(err, &checkErr) {
			return fmt.Errorf("%w\nrerun with --skip-checks to bypass this pre-migration check after review", checkErr)
		}
		return fmt.Errorf("error running migrations: %w", err)
	}

	// Get final status
	finalStatus, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting final migration status: %w", err)
	}
	publishDeploymentReportIfNeeded(cmd.Context(), runtime, emit, deploymentReportPublication{
		source:     source.OCI,
		dialect:    conn.Info().Dialect,
		before:     status,
		after:      finalStatus,
		startedAt:  startedAt,
		finishedAt: time.Now(),
		dryRun:     opts.dryRun,
		skip:       opts.skipReport,
	})

	emitMigrateUpSummary(emit, opts, status, finalStatus)
	return nil
}

// emitMigrateUpSummary prints the closing run summary. Dry runs report how
// many migrations would have been applied, bounded by --limit when set.
func emitMigrateUpSummary(
	emit cliobs.Emitter,
	opts *options,
	status *migrator.MigrationStatus,
	finalStatus *migrator.MigrationStatus,
) {
	emit.Println()
	if opts.dryRun {
		emit.Println("✅ Dry run completed successfully!")
		wouldApply := uint64(len(status.PendingMigrations))
		if opts.limit > 0 {
			wouldApply = min(wouldApply, opts.limit)
		}
		emit.Printf("Would have applied %d migrations\n", wouldApply)
		return
	}
	emit.Println("✅ Migrations completed successfully!")
	emit.Printf("Database is now at version: %d\n", finalStatus.CurrentVersion)
}

func verifyMigrationIntegrity(
	fsys fs.FS,
	format migrator.MigrationDirFormat,
) (*migratesum.Result, error) {
	result, err := migratesum.VerifyWithFormat(fsys, format)
	if err != nil {
		return nil, fmt.Errorf("migration sum verification failed: %w", err)
	}
	if !result.OK() {
		return nil, fmt.Errorf("migration sum verification failed:\n%s", result.Describe())
	}
	return result, nil
}

func publishDeploymentReportIfNeeded(
	ctx context.Context,
	runtime *cliobs.Runtime,
	emit cliobs.Emitter,
	publication deploymentReportPublication,
) {
	if publication.dryRun ||
		publication.skip ||
		publication.source == nil ||
		!deploymentreport.HasAppliedChanges(publication.before, publication.after) {
		return
	}
	publishDeploymentReportBestEffort(
		ctx,
		runtime,
		emit,
		publication.source,
		deploymentreport.SuccessfulOptions{
			Subject:    publication.source.Descriptor,
			Dialect:    publication.dialect,
			Before:     publication.before,
			After:      publication.after,
			StartedAt:  publication.startedAt,
			FinishedAt: publication.finishedAt,
		},
	)
}

func publishDeploymentReportBestEffort(
	ctx context.Context,
	runtime *cliobs.Runtime,
	emit cliobs.Emitter,
	source *migrationsource.OCI,
	opts deploymentreport.SuccessfulOptions,
) {
	_, reportErr := deploymentreport.PublishSuccessful(
		ctx,
		source.Client,
		source.Reference,
		opts,
	)
	if reportErr != nil {
		runtime.Logger().Warn("failed to attach OCI deployment report", "error", reportErr)
		emit.Printf("Warning: migrations succeeded, but the OCI deployment report could not be attached: %s\n", reportErr)
	}
}

func shutdownObservability(runtime *cliobs.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		runtime.Logger().Warn("failed to shut down observability", "error", err)
	}
}

func lintPendingDestructive(fsys fs.FS, pending []int64, dialect string) ([]lint.Finding, error) {
	cfg, err := lint.LoadConfigFS(fsys, lint.ConfigFileName)
	if err != nil {
		return nil, err
	}
	findings, err := lint.LintFS(fsys, lint.Options{
		Dialect:  dialect,
		Disabled: append([]string{"MF", "BC", "PG", "MY"}, cfg.DisabledRules...),
		Selection: lint.VersionSelection{
			Versions:   pending,
			Restricted: true,
		},
		RuleConfigs: cfg.Rules,
	})
	if err != nil {
		return nil, err
	}
	var destructive []lint.Finding
	for _, finding := range findings {
		if strings.HasPrefix(finding.Rule, "DS") && risk.IsBlocking(finding.Severity) {
			destructive = append(destructive, finding)
		}
	}
	return destructive, nil
}

func lockedDestructiveLintHook(fsys fs.FS, dialect string) migrator.PreMigrationHook {
	return func(_ context.Context, plan migrator.MigrationPlan) error {
		findings, err := lintPendingDestructive(fsys, plan.Versions, dialect)
		if err != nil {
			return fmt.Errorf("error checking pending migration safety: %w", err)
		}
		if len(findings) > 0 {
			return fmt.Errorf(
				"pending migrations contain destructive statements; rerun with --allow-destructive after review:\n%s",
				formatDestructiveFindings(findings),
			)
		}
		return nil
	}
}

func formatDestructiveFindings(findings []lint.Finding) string {
	var b strings.Builder
	for _, finding := range findings {
		if finding.Line > 0 {
			fmt.Fprintf(&b, "- %s:%d %s %s: %s\n", finding.File, finding.Line, finding.Rule, finding.Severity, finding.Message)
			continue
		}
		fmt.Fprintf(&b, "- %s %s %s: %s\n", finding.File, finding.Rule, finding.Severity, finding.Message)
	}
	return strings.TrimRight(b.String(), "\n")
}
