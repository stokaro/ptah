// Package migrateup implements "ptah migrations up", which applies pending
// migrations to a live database with optional lint checks, apply limits, and
// online-DDL configuration.
package migrateup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/dbschema"
	"ptah.run/internal/cli/cliobs"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/migrateflags"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/deploymentreport"
	"ptah.run/internal/migrationintegrity"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/internal/onlineddl"
	"ptah.run/internal/preflight"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
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
	toVersionFlag            = "to-version"
	skipChecksFlag           = "skip-checks"
	preUpHookFlag            = "pre-up-hook"
	pgDumpToFlag             = "pg-dump-to"
	mySQLDumpToFlag          = "mysqldump-to"
	webhookFlag              = "webhook"
	plainHTTPFlag            = "plain-http"
	skipReportFlag           = "skip-report"
	jsonFlag                 = "json"
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
	toVersion            string
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
	migrationsEngine     string
	revisionTableFormat  string
	logFormat            string
	logLevel             string
	metricsAddr          string
	jsonOutput           bool
}

type parsedMigrationSettings struct {
	dirFormat            migrationfile.DirFormat
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

// This command has no adoption gate, and that is a decision rather than an
// omission. stokaro/ptah#1253 asked whether it should refuse a database that
// already holds objects this history never created -- the refusal
// `ptah-compat migrate apply` grew in stokaro/ptah#1252 -- and the answer
// recorded here is no.
//
// Three reasons, in the order they bind:
//
//   - There is no oracle. Every operand of the compatibility gate was pinned by
//     running the pinned community binary. A native gate would be pinned by
//     taste, and its blast radius is every existing user deploying into a
//     pre-existing schema. Pre-v1 permits the break; permission is not a
//     reason to take it.
//   - The opt-out has no name left. --allow-dirty already means something else
//     here (see registerFlags), and a second dirty-shaped flag on the surface
//     that already has one is the confusion the gate was meant to prevent.
//   - Native already has the better answer, and it is a verb rather than a
//     flag: `ptah migrations baseline --version/--force/--shadow-db` adopts an
//     existing database and can verify the adoption against a shadow. A gate
//     would exist mostly to point at it.
//
// What would reverse this: evidence about what native users actually do today.
// If deploying into a pre-existing schema turns out to be rare and accidental
// rather than a deliberate workflow, the refusal becomes cheap and the argument
// above loses its first and strongest leg. That measurement is not something
// this repository can take from inside itself.
//
// Until then the failure stands as it is: the first object that already exists
// stops the run, and `ptah migrations repair` or `ptah migrations baseline`
// takes it from there.
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
	flags.BoolVar(
		&opts.verifySum,
		verifySumFlag,
		false,
		migrationsource.VerifySumUsage(
			"Require a sum file: a missing ptah.sum or atlas.sum is an error "+
				"(hashed directories always verify before applying)",
		),
	)
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrationfile.DirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.StringVar(&opts.execOrder, execOrderFlag, string(migrator.ExecOrderLinear), "Execution order policy for pending migrations below the current version: linear, linear-skip, or non-linear")
	flags.StringVar(&opts.txMode, txModeFlag, string(migrator.MigrationTxModeFile), "Transaction mode for pending migrations: file, all, or none")
	flags.StringVar(&opts.migrationLockTimeout, migrationLockTimeoutFlag, "", "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m")
	flags.StringVar(&opts.lockTimeout, lockTimeoutFlag, "", "Default per-migration lock timeout, such as 3s or 500ms")
	flags.StringVar(&opts.statementTimeout, statementTimeoutFlag, "", "Default per-migration statement timeout, such as 30s or 2m")
	flags.BoolVar(&opts.allowDestructive, allowDestructiveFlag, false, "Allow pending migrations that contain destructive statements")
	// --allow-dirty is one spelling with two meanings across Ptah's two
	// surfaces, and the collision is permanent: `ptah-compat migrate apply`
	// registers the flag Atlas registers, and this command registers the one
	// Ptah has always had.
	//
	//	native  (here)                  a REVISION ROW is dirty -- a migration
	//	                                body failed part-way. This asks for a
	//	                                verified retry of that body.
	//	compat  (internal/cli/atlas/migrate_apply.go)
	//	                                the SCHEMA is not empty -- the database
	//	                                already holds objects this history did
	//	                                not create. This says "adopt it anyway".
	//
	// Measured against the pinned community binary: its --allow-dirty releases
	// no dirty-revision guard at all. So the two flags share a spelling and
	// share no meaning, and neither can be expressed in terms of the other. An
	// operator reaching here for the documented recovery is not also opting out
	// of anything about adoption -- see stokaro/ptah#1253 for why this surface
	// grew no adoption gate that would have made them one flag.
	flags.BoolVar(
		&opts.allowDirty,
		allowDirtyFlag,
		false,
		"Request a verified retry of a dirty migration; only an unchanged committed source prefix is skipped",
	)
	flags.Uint64Var(&opts.limit, limitFlag, 0, "Apply only the first N pending migrations (0 applies all)")
	// A string rather than an int64: a migration version is written the way the
	// file name writes it, and a zero-padded 0000000002 has to reach the same
	// migration as 2. A separate empty state is what keeps the bound apart from
	// the migrator's zero, which means latest.
	flags.StringVar(
		&opts.toVersion,
		toVersionFlag,
		"",
		"Apply pending migrations up to and including this version; a version the directory does not "+
			"carry, or one the database has already passed, is refused (empty applies all)",
	)
	flags.BoolVar(&opts.skipChecks, skipChecksFlag, false, "Emergency bypass: skip pre-migration assertion checks (+ptah check directives and Atlas txtar checks.sql)")
	flags.StringVar(&opts.preUpHook, preUpHookFlag, "", "Shell command to run before applying pending migrations; aborts unless it exits 0")
	flags.StringVar(&opts.pgDumpTo, pgDumpToFlag, "", "Directory where pg_dump writes a custom-format backup before applying migrations")
	flags.StringVar(&opts.mySQLDumpTo, mySQLDumpToFlag, "", "Directory where mysqldump writes a SQL backup before applying migrations")
	flags.StringVar(&opts.webhook, webhookFlag, "", "Webhook URL to POST migration metadata before applying migrations; must return HTTP 200")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	flags.BoolVar(&opts.skipReport, skipReportFlag, false, "Do not attach a deployment report after applying an OCI migration artifact")
	flags.BoolVar(&opts.jsonOutput, jsonFlag, false, "Print the run's evidence as one JSON document, on success and on failure")
	flags.StringVar(&opts.logFormat, cliobs.LogFormatFlagName, "text", "Log format: text or json")
	flags.StringVar(&opts.logLevel, cliobs.LogLevelFlagName, "info", "Log level: debug, info, warn, or error")
	flags.StringVar(&opts.metricsAddr, cliobs.MetricsAddrFlagName, "", "Address for the Prometheus /metrics endpoint, such as :9090")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterMigrationsEngineFlag(flags, &opts.migrationsEngine)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
}

func parseMigrationSettings(
	dirFormatValue,
	revisionFormatValue,
	execOrderValue,
	txModeValue,
	migrationLockTimeoutValue,
	connectTimeoutValue string,
) (parsedMigrationSettings, error) {
	dirFormat, err := migrationfile.ParseDirFormat(dirFormatValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	revisionFormat, err := migrateflags.ParseRevisionTableFormat(revisionFormatValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	execOrder, err := migrateflags.ParseExecOrder(execOrderValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	txMode, err := migrateflags.ParseMigrationTxMode(txModeValue)
	if err != nil {
		return parsedMigrationSettings{}, err
	}
	migrationLockTimeout, err := migrateflags.ParseMigrationLockTimeout(migrationLockTimeoutValue)
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

// parseToVersion reads the --to-version bound into the migrator's operand.
//
// An unset flag answers zero, which the migrator reads as "latest". A typed
// zero is refused instead of being folded into that: `--to-version 0` names no
// migration, and reading it as "apply everything" is the guess the bound exists
// to remove.
func parseToVersion(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	version, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s %q: a migration version is a number", toVersionFlag, value)
	}
	if version <= 0 {
		return 0, fmt.Errorf("invalid --%s %q: a migration version is greater than zero", toVersionFlag, value)
	}
	return version, nil
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

// commandLogWriter is the stream structured logs go to.
//
// Human output and JSON output do not share a stream: a JSON run's logs belong
// on stdout with the records a consumer parses, while a human run keeps them on
// stderr so the report on stdout stays pipeable.
func commandLogWriter(cmd *cobra.Command, logFormat string) io.Writer {
	if logFormat == "json" {
		return cmd.OutOrStdout()
	}
	return cmd.ErrOrStderr()
}

func migrateUpCommand(cmd *cobra.Command, opts *options) error {
	// --limit and --to-version select different prefixes of the pending list and
	// neither outranks the other, so the pair is refused here rather than deep
	// in the migrator, whose own refusal names an operand ("amount") that no
	// flag on this surface spells. Both are environment-bound, so the group is
	// resolved on what the operator typed: cobra's own ValidateFlagGroups reads
	// Changed, which an exported PTAH_LIMIT sets, and would refuse a command
	// line carrying one flag while naming a second one nobody wrote.
	if err := cmdflags.ExclusiveOnCommandLine(cmd.Flags(), limitFlag, toVersionFlag); err != nil {
		return err
	}
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

	logWriter := commandLogWriter(cmd, opts.logFormat)
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
	// Under --json the document is the output, so everything written for a
	// person goes to standard error. A `=== MIGRATE UP ===` banner in front of
	// it would reach the caller as a parse error rather than as a help.
	emit := cliobs.NewEmitter(humanOutput(cmd, opts), runtime)

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	requestedMigrationsDir := migrationsDir
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
	toVersion, err := parseToVersion(resolvedOpts.toVersion)
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
	lintPathPrefix := lintPathPrefixForSource(requestedMigrationsDir, source)
	migrationsFS := source.FileSystem
	migrationsDir = source.Display
	settings.dirFormat = source.DirFormat

	if err := runIntegrityGate(cmd.ErrOrStderr(), emit, runtime, source, settings.dirFormat, integrityPolicy, opts); err != nil {
		return err
	}

	if opts.verbose {
		emit.Printf("Connecting to database: %s\n", dburldisplay.Format(dbURL))
	}

	timeouts, err := migrateflags.ParseMigrationTimeouts(lockTimeout, statementTimeout)
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
	lintPolicy, err := migrationlintgate.LoadPolicy(migrationsFS, conn.Info().Dialect)
	if err != nil {
		return fmt.Errorf("error loading migration lint policy: %w", err)
	}

	// Set dry run mode if requested
	conn.SchemaWriter().SetDryRun(opts.dryRun)

	// Online-DDL routing: `-- +ptah online_ddl_tool=...` directives always
	// work; the ptah.yaml online_ddl section adds automatic routing of
	// ALTERs on tables above the configured row threshold.
	onlineCfg := projectCfg.OnlineDDL
	interceptor := onlineddl.New(onlineCfg).WithDryRun(opts.dryRun)

	mig, err := migrator.NewFSMigrator(
		conn,
		migrationsFS,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithMigrationDirFormat(settings.dirFormat),
		migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: atlasEnv}),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(migrationsSchema, migrationsTable).
		WithMigrationsEngine(opts.migrationsEngine).
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

	emitPlanOutput := func() {
		if opts.dryRun {
			emit.Println("=== DRY RUN MODE ===")
			emit.Println("No actual changes will be made to the database")
			emit.Println()
		}

		emit.Println("=== MIGRATE UP ===")
		emit.Printf("Database: %s\n", dburldisplay.Format(dbURL))
		emit.Printf("Dialect: %s\n", conn.Info().Dialect)
		emit.Printf("Migrations directory: %s\n", migrationsDir)
		emit.Printf("Migration directory format: %s\n", settings.dirFormat)
		emit.Printf("Transaction mode: %s\n", settings.txMode)
		emit.Println()

		if onlineCfg.Enabled() {
			emit.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
		}
		emit.Printf("Current version: %d\n", status.CurrentVersion)
		emit.Printf("Total migrations: %d\n", status.TotalMigrations)
		emit.Printf("Pending migrations: %d\n", len(status.PendingMigrations))
		if len(status.OutOfOrderMigrations) > 0 {
			emit.Printf("Out-of-order migrations: %v\n", status.OutOfOrderMigrations)
		}
		if opts.verbose {
			emit.Printf("Pending migration versions: %v\n", status.PendingMigrations)
			if len(status.OutOfOrderMigrations) > 0 {
				emit.Printf("Out-of-order migration versions: %v\n", status.OutOfOrderMigrations)
			}
		}
		emit.Println()
	}

	// A bounded run does not take this shortcut. Whether the target is still
	// reachable is decided under the migration lock, where the recorded history
	// cannot move underneath the answer, and the status read above happens
	// before that lock exists. Returning here would report success for a run
	// that never reached the version the operator named.
	if !status.HasPendingChanges && toVersion == 0 {
		emitPlanOutput()
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

	if settings.execOrder == migrator.ExecOrderLinear && len(status.OutOfOrderMigrations) > 0 {
		return &migrator.OutOfOrderError{
			CurrentVersion: status.CurrentVersion,
			Versions:       slices.Clone(status.OutOfOrderMigrations),
		}
	}
	preflightHook := dbcli.LockedMigrationPreflightHook(opts.dryRun, preflight.Options{
		Direction:          preflight.DirectionUp,
		DatabaseURL:        dbURL,
		DisplayDatabaseURL: dburldisplay.Format(dbURL),
		Dialect:            conn.Info().Dialect,
		Command:            preUpHook,
		PostgresDumpDir:    pgDumpTo,
		MySQLDumpDir:       mySQLDumpTo,
		WebhookURL:         webhook,
	}, emit, cliobs.NewOutputWriter(cmd.OutOrStdout(), runtime, "pre-flight output"))
	if !opts.allowDestructive {
		preflightHook = dbcli.CombineMigrationHooks(
			lockedDestructiveLintHook(migrationsFS, lintPolicy, lintPathPrefix),
			preflightHook,
		)
	}
	preflightHook = dbcli.CombineMigrationHooks(
		func(context.Context, migrator.MigrationPlan) error {
			emitPlanOutput()
			return nil
		},
		preflightHook,
	)

	// Run migrations
	startedAt := time.Now()
	outcome := applyPendingMigrations(cmd, mig, opts, toVersion, preflightHook)
	finalStatus := outcome.status
	if err := outcome.err(); err != nil {
		return err
	}
	checksDeferred := outcome.checksDeferred
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

	if !opts.jsonOutput {
		emitMigrateUpSummary(emit, opts, outcome)
		emitMigrateUpDeferredChecks(emit, checksDeferred)
	}
	return nil
}

// migrateUpOutcome is one run and everything the command has to say about it.
type migrateUpOutcome struct {
	status         *migrator.MigrationStatus
	plan           *migrator.MigrationPlan
	checksDeferred []int64
	runErr         error
	statusErr      error
}

// selectedCount is how many migrations this run set out to apply. A run whose
// plan was never selected counts nothing rather than falling back to the
// pending list, which is the number the bound was asked to narrow.
func (o migrateUpOutcome) selectedCount() int {
	if o.plan == nil {
		return 0
	}
	return len(o.plan.Versions)
}

// err is the failure the command returns, in the order the caller can act on:
// the run's own error first, and the unreadable status only when the run
// itself succeeded.
func (o migrateUpOutcome) err() error {
	if o.runErr != nil {
		if checkErr, ok := errors.AsType[*migrator.CheckFailedError](o.runErr); ok {
			return fmt.Errorf("%w\nrerun with --skip-checks to bypass this pre-migration check after review", checkErr)
		}
		return fmt.Errorf("error running migrations: %w", o.runErr)
	}
	if o.statusErr != nil {
		return fmt.Errorf("error getting final migration status: %w", o.statusErr)
	}
	return nil
}

// applyPendingMigrations runs the migrations and gathers the run's evidence.
//
// The status is read on both paths, and the failing one is why: a run that
// stopped is exactly the run whose caller has to be told what the database now
// holds. A status that cannot be read leaves the outcome unknown rather than
// replacing it with a guess, and the document is written before either failure
// is returned.
func applyPendingMigrations(
	cmd *cobra.Command,
	mig *migrator.Migrator,
	opts *options,
	toVersion int64,
	preflightHook migrator.PreMigrationHook,
) migrateUpOutcome {
	outcome := migrateUpOutcome{}
	// The selected plan is captured rather than derived from the pending list
	// afterwards: a limit, a target version or a checkpoint narrows what the
	// migrator selected under its own lock, and a document that reported the
	// pending list would name work this run never intended to do.
	outcome.runErr = mig.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{
		Amount:        opts.limit,
		TargetVersion: toVersion,
		// The native surface hands the migrator an operator's exact version, so
		// a run that cannot reach it is an error here. `ptah-compat migrate
		// apply --to-version` keeps the Atlas answer, which is an empty run and
		// exit 0.
		RefuseTargetVersionAlreadyPassed: true,
		AllowDirty:                       opts.allowDirty,
		Preflight:                        preflightHook,
		PlanObserver: func(_ context.Context, plan migrator.MigrationPlan) {
			outcome.plan = &plan
		},
		ChecksDeferredObserver: func(_ context.Context, versions []int64) {
			outcome.checksDeferred = versions
		},
	})
	status, statusErr := mig.GetMigrationStatus(context.Background())
	if statusErr != nil {
		outcome.statusErr = statusErr
		status = nil
	}
	outcome.status = status
	if !opts.jsonOutput {
		return outcome
	}
	if err := emitRunResult(cmd.OutOrStdout(), migrator.RunEvidence{
		Direction: migrator.MigrationDirectionUp,
		Plan:      outcome.plan,
		After:     status,
		Err:       outcome.runErr,
		DryRun:    opts.dryRun,
	}); err != nil && outcome.runErr == nil {
		outcome.runErr = err
	}
	return outcome
}

// humanOutput selects where this command's human-facing output goes.
func humanOutput(cmd *cobra.Command, opts *options) io.Writer {
	if opts.jsonOutput {
		return cmd.ErrOrStderr()
	}
	return cmd.OutOrStdout()
}

// emitRunResult writes the run's evidence as one JSON document.
//
// It goes to standard output on both the successful and the failing path,
// because the caller that most needs the evidence is the one whose run stopped.
func emitRunResult(w io.Writer, evidence migrator.RunEvidence) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(migrator.NewRunResult(evidence)); err != nil {
		return fmt.Errorf("error writing migration run result: %w", err)
	}
	return nil
}

// emitMigrateUpDeferredChecks names the pre-migration checks a dry run
// validated but did not evaluate. A preview that quietly answers fewer
// questions than it was asked is worse than one that says so.
func emitMigrateUpDeferredChecks(emit cliobs.Emitter, versions []int64) {
	if len(versions) == 0 {
		return
	}
	labels := make([]string, 0, len(versions))
	for _, version := range versions {
		labels = append(labels, strconv.FormatInt(version, 10))
	}
	noun := "migrations"
	if len(versions) == 1 {
		noun = "migration"
	}
	emit.Printf(
		"Deferred pre-migration checks for %d %s (%s): a dry run does not create the state they assert on, so they are evaluated on apply.\n",
		len(versions),
		noun,
		strings.Join(labels, ", "),
	)
}

// emitMigrateUpSummary prints the closing run summary.
//
// A dry run reports the plan the migrator selected while holding its own lock,
// so --limit, --to-version, the execution order and a checkpoint each narrow
// the count the same way they narrow the work. Recomputing it from the pending
// list read before the lock would name migrations the run had already decided
// to leave alone.
func emitMigrateUpSummary(emit cliobs.Emitter, opts *options, outcome migrateUpOutcome) {
	emit.Println()
	if opts.dryRun {
		emit.Println("✅ Dry run completed successfully!")
		emit.Printf("Would have applied %d migrations\n", outcome.selectedCount())
		return
	}
	emit.Println("✅ Migrations completed successfully!")
	emit.Printf("Database is now at version: %d\n", outcome.status.CurrentVersion)
}

func lintPathPrefixForSource(requested string, source migrationsource.Source) string {
	if source.OCI != nil {
		return filepath.ToSlash(source.Display)
	}
	return filepath.ToSlash(requested)
}

// runIntegrityGate verifies the resolved migration filesystem before anything
// is applied and then qualifies what that verification established.
//
// Both contracts — the always-on hashed gate (#955) and the stricter
// --verify-sum, where a MISSING sum file is itself an error — check a directory
// against the sum stored beside it, so both get the same provenance qualifier
// when the directory came from a movable OCI tag (#944). They are one call
// rather than two branches here because `up` holding a private copy of the
// first is what let five other verbs execute directories `up` refused, and a
// private copy of the second would have the same shape.
func runIntegrityGate(
	notice io.Writer,
	emit cliobs.Emitter,
	runtime *cliobs.Runtime,
	source migrationsource.Source,
	format migrationfile.DirFormat,
	integrityPolicy migrationintegrity.Policy,
	opts *options,
) error {
	return migrationsource.Verify(notice, emit, runtime, source, format, integrityPolicy, migrationsource.VerifyOptions{
		RequireSum: opts.verifySum,
		Verbose:    opts.verbose,
	})
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

func lintPendingDestructive(
	fsys fs.FS,
	pending []int64,
	dialect,
	pathPrefix string,
) ([]lint.Finding, error) {
	return migrationlintgate.Analyze(fsys, pending, dialect, pathPrefix)
}

func lockedDestructiveLintHook(
	fsys fs.FS,
	policy migrationlintgate.Policy,
	pathPrefix string,
) migrator.PreMigrationHook {
	return func(_ context.Context, plan migrator.MigrationPlan) error {
		findings, err := migrationlintgate.AnalyzeWithPolicy(fsys, plan.Versions, policy, pathPrefix)
		if err != nil {
			return fmt.Errorf("error checking pending migration safety: %w", err)
		}
		if len(findings) > 0 {
			return fmt.Errorf(
				"%s; rerun with --allow-destructive after review:\n%s",
				gateRefusal(findings),
				formatDestructiveFindings(findings),
			)
		}
		return nil
	}
}

// gateRefusal names what the gate refused on: the data-safety wording the
// default gate has always used, or the policy's own widening when a finding
// outside the DS family is among them.
func gateRefusal(findings []lint.Finding) string {
	for _, finding := range findings {
		if !strings.HasPrefix(finding.Rule, migrationlintgate.ReportedFamily) {
			return "pending migrations carry lint findings the policy's gate section blocks on"
		}
	}
	return "pending migrations contain destructive statements"
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
