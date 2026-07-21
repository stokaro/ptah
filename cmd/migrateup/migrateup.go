package migrateup

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cliobs"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/onlineddl"
	"github.com/stokaro/ptah/internal/pathguard"
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
	migrationLockTimeoutFlag = "migration-lock-timeout"
	lockTimeoutFlag          = "lock-timeout"
	statementTimeoutFlag     = "statement-timeout"
	allowDestructiveFlag     = "allow-destructive"
	preUpHookFlag            = "pre-up-hook"
	pgDumpToFlag             = "pg-dump-to"
	mySQLDumpToFlag          = "mysqldump-to"
	webhookFlag              = "webhook"
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
	migrationLockTimeout string
	lockTimeout          string
	statementTimeout     string
	allowDestructive     bool
	preUpHook            string
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

func NewMigrateUpCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Run pending migrations up to the latest version",
		Long: `Run all pending database migrations up to the latest version.

This command applies all migrations that haven't been applied yet, bringing
the database schema up to the latest version defined in the migration files.

Each migration is run in a transaction unless its file explicitly opts out with
-- +ptah no_transaction, so ordinary migration failures are rolled back and the
migration process stops.`,
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
	flags.StringVar(&opts.migrationsDir, migrationsFlag, "", "Directory containing migration files (required)")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Show what migrations would be applied without actually running them")
	flags.BoolVar(&opts.verbose, verboseFlag, false, "Enable verbose output")
	flags.BoolVar(&opts.verifySum, verifySumFlag, false, "Verify the migrations directory against its committed ptah.sum before applying; abort on drift")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.StringVar(&opts.execOrder, execOrderFlag, string(migrator.ExecOrderLinear), "Execution order policy for pending migrations below the current version: linear, linear-skip, or non-linear")
	flags.StringVar(&opts.migrationLockTimeout, migrationLockTimeoutFlag, "", "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m")
	flags.StringVar(&opts.lockTimeout, lockTimeoutFlag, "", "Default per-migration lock timeout, such as 3s or 500ms")
	flags.StringVar(&opts.statementTimeout, statementTimeoutFlag, "", "Default per-migration statement timeout, such as 30s or 2m")
	flags.BoolVar(&opts.allowDestructive, allowDestructiveFlag, false, "Allow pending migrations that contain destructive statements")
	flags.StringVar(&opts.preUpHook, preUpHookFlag, "", "Shell command to run before applying pending migrations; aborts unless it exits 0")
	flags.StringVar(&opts.pgDumpTo, pgDumpToFlag, "", "Directory where pg_dump writes a custom-format backup before applying migrations")
	flags.StringVar(&opts.mySQLDumpTo, mySQLDumpToFlag, "", "Directory where mysqldump writes a SQL backup before applying migrations")
	flags.StringVar(&opts.webhook, webhookFlag, "", "Webhook URL to POST migration metadata before applying migrations; must return HTTP 200")
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

func migrateUpCommand(cmd *cobra.Command, opts *options) error {
	dbURL := opts.dbURL
	migrationsDir := opts.migrationsDir
	dirFormatValue := opts.dirFormat
	atlasEnv := opts.atlasEnv
	execOrderValue := opts.execOrder
	migrationLockTimeoutValue := opts.migrationLockTimeout
	lockTimeout := opts.lockTimeout
	statementTimeout := opts.statementTimeout
	preUpHook := opts.preUpHook
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
	preUpHook = dbcli.EffectiveString(cmd, preUpHookFlag, preUpHook, projectCfg.Migration.PreUpHook)
	pgDumpTo = dbcli.EffectiveString(cmd, pgDumpToFlag, pgDumpTo, projectCfg.Migration.PostgresDumpTo)
	mySQLDumpTo = dbcli.EffectiveString(cmd, mySQLDumpToFlag, mySQLDumpTo, projectCfg.Migration.MySQLDumpTo)
	webhook = dbcli.EffectiveString(cmd, webhookFlag, webhook, projectCfg.Migration.Webhook)
	migrationsSchema = dbcli.EffectiveString(cmd, dbcli.MigrationsSchemaFlagName, migrationsSchema, projectCfg.Migration.RevisionsSchema)
	migrationsTable = dbcli.EffectiveString(cmd, dbcli.MigrationsTableFlagName, migrationsTable, projectCfg.Migration.RevisionsTable)
	revisionFormatValue = dbcli.EffectiveString(cmd, dbcli.RevisionTableFormatFlagName, revisionFormatValue, projectCfg.Migration.RevisionFormat)
	connectTimeoutValue := dbcli.EffectiveString(cmd, dbcli.ConnectTimeoutFlagName, opts.connectTimeout, projectCfg.Migration.ConnectTimeout)

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
	migrationsDir, err = pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return fmt.Errorf("invalid migrations directory: %w", err)
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(revisionFormatValue)
	if err != nil {
		return err
	}

	// Integrity gate: refuse to apply if a committed migration was edited
	// out of band. Runs before connecting so a tampered directory fails fast.
	if opts.verifySum {
		result, err := migratesum.VerifyDirWithFormat(migrationsDir, dirFormat)
		if err != nil {
			return fmt.Errorf("migration sum verification failed: %w", err)
		}
		if !result.OK() {
			return fmt.Errorf("migration sum verification failed:\n%s", result.Describe())
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

	emit.Println("=== MIGRATE UP ===")
	emit.Printf("Database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	emit.Printf("Dialect: %s\n", conn.Info().Dialect)
	emit.Printf("Migrations directory: %s\n", migrationsDir)
	emit.Printf("Migration directory format: %s\n", dirFormat)
	emit.Println()

	// Create filesystem from migrations directory
	migrationsFS := os.DirFS(migrationsDir)

	// Online-DDL routing: `-- +ptah online_ddl_tool=...` directives always
	// work; the ptah.yaml online_ddl section adds automatic routing of
	// ALTERs on tables above the configured row threshold.
	onlineCfg, err := dbcli.LoadOnlineDDLConfigForEnv(opts.configPath, projectCfg.EnvName)
	if err != nil {
		return err
	}
	if onlineCfg.Enabled() {
		emit.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
	}
	interceptor := onlineddl.New(*onlineCfg).WithDryRun(opts.dryRun)

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
	if execOrder == migrator.ExecOrderLinear && len(status.OutOfOrderMigrations) > 0 {
		return migrator.NewOutOfOrderError(status.CurrentVersion, status.OutOfOrderMigrations)
	}

	if !opts.allowDestructive {
		findings, err := lintPendingDestructive(migrationsFS, pendingMigrationsForRun(status, execOrder), conn.Info().Dialect)
		if err != nil {
			return fmt.Errorf("error checking pending migration safety: %w", err)
		}
		if len(findings) > 0 {
			return fmt.Errorf("pending migrations contain destructive statements; rerun with --allow-destructive after review:\n%s", formatDestructiveFindings(findings))
		}
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

	// Run migrations
	err = mig.MigrateUpWithPreflight(context.Background(), preflightHook)
	if err != nil {
		return fmt.Errorf("error running migrations: %w", err)
	}

	// Get final status
	finalStatus, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting final migration status: %w", err)
	}

	emit.Println()
	if opts.dryRun {
		emit.Println("✅ Dry run completed successfully!")
		emit.Printf("Would have applied %d migrations\n", len(status.PendingMigrations))
	} else {
		emit.Println("✅ Migrations completed successfully!")
		emit.Printf("Database is now at version: %d\n", finalStatus.CurrentVersion)
	}

	return nil
}

func shutdownObservability(runtime *cliobs.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		runtime.Logger().Warn("failed to shut down observability", "error", err)
	}
}

func pendingMigrationsForRun(status *migrator.MigrationStatus, execOrder migrator.ExecOrder) []int64 {
	if execOrder != migrator.ExecOrderLinearSkip {
		return status.PendingMigrations
	}

	outOfOrder := make(map[int64]struct{}, len(status.OutOfOrderMigrations))
	for _, version := range status.OutOfOrderMigrations {
		outOfOrder[version] = struct{}{}
	}

	pending := make([]int64, 0, len(status.PendingMigrations))
	for _, version := range status.PendingMigrations {
		if _, ok := outOfOrder[version]; ok {
			continue
		}
		pending = append(pending, version)
	}
	return pending
}

func lintPendingDestructive(fsys fs.FS, pending []int64, dialect string) ([]lint.Finding, error) {
	cfg, err := lint.LoadConfigFS(fsys, lint.ConfigFileName)
	if err != nil {
		return nil, err
	}
	findings, err := lint.LintFS(fsys, lint.Options{
		Dialect:     dialect,
		Disabled:    append([]string{"MF", "BC", "PG", "MY"}, cfg.DisabledRules...),
		Versions:    pending,
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
