// Package migratedown implements "ptah migrations down", which rolls back
// applied migrations to a target version against a live database.
package migratedown

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/core/lintext"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/cliobs"
	"ptah.run/internal/cli/internal/cmdadapter"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/dirtyhint"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/internal/migrateflags"
	"ptah.run/internal/cli/internal/migratelock"
	"ptah.run/internal/cli/internal/migraterun"
	"ptah.run/internal/cli/internal/migratetimeout"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/migrationintegrity"
	"ptah.run/internal/onlineddl"
	"ptah.run/internal/preflight"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

const (
	dbURLFlag                = "db-url"
	migrationsFlag           = "migrations-dir"
	targetFlag               = "target"
	toTagFlag                = "to-tag"
	skipChecksFlag           = "skip-checks"
	planFlag                 = "plan"
	shadowDBFlag             = "shadow-db"
	dirFormatFlag            = "dir-format"
	atlasEnvFlag             = "atlas-env"
	dryRunFlag               = "dry-run"
	verboseFlag              = "verbose"
	confirmFlag              = "confirm"
	jsonFlag                 = "json"
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
	featureRuntime       engine.SchemaRuntime
	dbURL                string
	migrationsDir        string
	target               string
	shadowDB             string
	toTag                string
	skipChecks           bool
	plan                 bool
	dirFormat            string
	atlasEnv             string
	dryRun               bool
	verbose              bool
	skipConfirm          bool
	jsonOutput           bool
	serverDetails        bool
	execOrder            string
	runner               string
	migrationLockTimeout string
	migrationLockName    string
	lockTimeout          string
	statementTimeout     string
	preDownHook          string
	preDownHookOrigin    string
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
	migrationsEngine     string
	actor                string
	actorOrigin          string
	runReference         string
	revisionTableFormat  string
	logFormat            string
	logLevel             string
	metricsAddr          string
	// resolvedRunner is what runner names once ptah.yaml is merged under it.
	resolvedRunner migrateflags.Runner
	// lockName is the migration lock name the run selected, once the flag, its
	// environment variable and ptah.yaml are merged and a blank choice is
	// refused.
	lockName migratelock.Name
	// planExclude is the project's exclude list a --plan rollback applies,
	// and planExcludeSources names the file each selector came from.
	planExclude        []string
	planExcludeSources atlasfilter.ExcludeSources
	// allowUnmatchedExclude is the opt-in that turns the refusal of a --plan
	// selector that matched nothing into a warning.
	allowUnmatchedExclude bool
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
be rolled back and the migration process will stop. With --runner golang-migrate,
a down file runs the way golang-migrate runs it: on PostgreSQL in the transactions
it opens itself, and a file of one statement without a transaction.

⚠️  WARNING: This operation can result in data loss! Make sure you have backups
before running down migrations in production.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			document := &migraterun.Document{
				Enabled:       opts.jsonOutput,
				Out:           cmd.OutOrStdout(),
				Command:       Command,
				Direction:     migrator.MigrationDirectionDown,
				ServerDetails: migraterun.NewServerDetails(cmd.Context(), opts.serverDetails),
			}
			if err := migrateDownCommand(cmd, &opts, document); err != nil {
				return document.FailBeforeRun(err, opts.dryRun)
			}
			// The work is finite and it is done, so an interrupt arriving
			// between here and the process exit stopped nothing. See
			// cmdutil.ReportWorkFinished.
			cmdutil.ReportWorkFinished(cmd.Context())
			return nil
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
	flags.StringVar(&opts.toTag, toTagFlag, "",
		"Migration tag to roll back to, resolved against the tags recorded by `migrations tag`")
	flags.BoolVar(&opts.skipChecks, skipChecksFlag, false,
		"Skip the pre-migration checks embedded in the down migrations being rolled back")
	flags.BoolVar(&opts.plan, planFlag, false,
		"Derive the rollback from the schema difference instead of running the down migrations, "+
			"requires --"+shadowDBFlag)
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrationfile.DirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Show what migrations would be rolled back without actually running them")
	flags.BoolVar(&opts.verbose, verboseFlag, false, "Enable verbose output")
	flags.BoolVar(&opts.skipConfirm, confirmFlag, false, "Skip confirmation prompt (use with caution!)")
	flags.BoolVar(&opts.jsonOutput, jsonFlag, false, "Print the run's evidence as one JSON document, on success and on failure; needs --confirm or --dry-run")
	// --json decides what standard output is, so it belongs to the invocation:
	// an environment that exported PTAH_JSON for another verb's document would
	// otherwise turn every rollback there into a JSON run.
	if err := cmdflags.DisableEnvBinding(flags, jsonFlag); err != nil {
		panic(err)
	}
	migraterun.RegisterServerDetailsFlag(flags, &opts.serverDetails)
	flags.StringVar(&opts.execOrder, execOrderFlag, string(migrator.ExecOrderLinear), "Execution order policy for pending migrations below the current version: linear, linear-skip, or non-linear")
	dbcli.RegisterRunnerFlag(flags, &opts.runner)
	flags.StringVar(&opts.migrationLockTimeout, migrationLockTimeoutFlag, "", "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m; "+
		"refused against a dialect that takes no such lock")
	migratelock.RegisterNameFlag(flags, &opts.migrationLockName)
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
	dbcli.RegisterMigrationsEngineFlag(flags, &opts.migrationsEngine)
	dbcli.RegisterActorFlag(flags, &opts.actor, "migration log")
	dbcli.RegisterActorOriginFlag(flags, &opts.actorOrigin, "migration log")
	dbcli.RegisterRunReferenceFlag(flags, &opts.runReference, "migration log")
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
}

// resolveProjectOptions merges the project config under the flags. A command
// hook the config supplies is checked against the program policy here, before
// the command opens the database.
func resolveProjectOptions(cmd *cobra.Command, opts options, projectCfg projectconfig.Config) (options, error) {
	effectiveString := func(flagName, flagValue string, field projectconfig.StringField) string {
		return dbcli.EffectiveString(cmd, flagName, flagValue, projectCfg.StringValue(field))
	}
	opts.dbURL = effectiveString(dbURLFlag, opts.dbURL, projectconfig.StringDatabaseURL)
	opts.migrationsDir = effectiveString(migrationsFlag, opts.migrationsDir, projectconfig.StringMigrationDir)
	opts.shadowDB = effectiveString(shadowDBFlag, opts.shadowDB, projectconfig.StringDevURL)
	opts.dirFormat = effectiveString(dirFormatFlag, opts.dirFormat, projectconfig.StringMigrationFormat)
	opts.atlasEnv = effectiveString(atlasEnvFlag, opts.atlasEnv, projectconfig.StringEnvName)
	opts.execOrder = effectiveString(execOrderFlag, opts.execOrder, projectconfig.StringMigrationExecOrder)
	// The runner decides how each down file runs, as it decides how
	// `migrations up` runs each up file, and both take it from one place. It
	// is parsed here, before the source is resolved, so a value that names no
	// runner is refused before an OCI reference is fetched.
	runner, err := dbcli.ResolveRunner(cmd, opts.runner, projectCfg)
	if err != nil {
		return opts, err
	}
	opts.resolvedRunner = runner
	// The command owns the opt-in, so it is resolved on every invocation and
	// before anything is read: a malformed value is refused whichever rollback
	// the run takes and whatever the selectors match.
	opts.allowUnmatchedExclude, err = atlasfilter.AllowUnmatchedExclude()
	if err != nil {
		return opts, err
	}
	// A rollback derived from the schema difference leaves the objects the
	// project excludes alone, as `schema apply` does; one that runs the down
	// files runs what they say, and has no exclusion to apply.
	if opts.plan {
		opts.planExclude, opts.planExcludeSources, err = dbcli.EffectiveExclude(cmd, "", nil, projectCfg, opts.featureRuntime.Selections())
		if err != nil {
			return opts, err
		}
	}
	opts.migrationLockTimeout = effectiveString(
		migrationLockTimeoutFlag,
		opts.migrationLockTimeout,
		projectconfig.StringMigrationMigrationLockTimeout,
	)
	opts.migrationLockName = effectiveString(
		migratelock.NameFlag,
		opts.migrationLockName,
		projectconfig.StringMigrationMigrationLockName,
	)
	// Before the run connects, so a blank choice is refused without opening the
	// target.
	opts.lockName, err = migratelock.ResolveName(
		migratelock.NameRequest(cmd, projectCfg.StringValue(projectconfig.StringMigrationMigrationLockName).Present, opts.dbURL),
		opts.migrationLockName,
	)
	if err != nil {
		return opts, err
	}
	opts.lockTimeout = effectiveString(lockTimeoutFlag, opts.lockTimeout, projectconfig.StringMigrationLockTimeout)
	opts.statementTimeout = effectiveString(
		statementTimeoutFlag,
		opts.statementTimeout,
		projectconfig.StringMigrationStatementTimeout,
	)
	opts.preDownHook = effectiveString(preDownHookFlag, opts.preDownHook, projectconfig.StringMigrationPreDownHook)
	opts.preDownHookOrigin = dbcli.ConfiguredHookOrigin(cmd, preDownHookFlag, "ptah.yaml migration.pre_down_hook", projectCfg.StringValue(projectconfig.StringMigrationPreDownHook))
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
	if !opts.dryRun {
		if err := dbcli.AuthorizeConfiguredHook(cmd.Context(), opts.preDownHookOrigin); err != nil {
			return opts, err
		}
	}
	return opts, nil
}

// rollbackSettingValues carries the flag strings one rollback parses, so the
// parsers are one call at the call site rather than an error check each.
type rollbackSettingValues struct {
	lockTimeout          string
	statementTimeout     string
	execOrder            string
	migrationLockTimeout string
	connectTimeout       string
	// runner is parsed already, with the project options, and decides the
	// provider options.
	runner migrateflags.Runner
}

// parsedRollbackSettings is what those strings mean to the migrator.
type parsedRollbackSettings struct {
	timeouts             migrationfile.Timeouts
	execOrder            migrator.ExecOrder
	migrationLockTimeout time.Duration
	connectTimeout       time.Duration
	// providerOptions load the directory the way its runner runs it, the same
	// for the rollback and for its verification on --shadow-db.
	providerOptions []migrator.FSProviderOption
}

// parseRollbackSettings turns the flag strings into migrator values, refusing
// on the first one that does not parse. `migrations up` reads the same
// vocabulary through parseMigrationSettings.
func parseRollbackSettings(values rollbackSettingValues) (parsedRollbackSettings, error) {
	timeouts, err := migrateflags.ParseMigrationTimeouts(values.lockTimeout, values.statementTimeout)
	if err != nil {
		return parsedRollbackSettings{}, err
	}
	execOrder, err := migrateflags.ParseExecOrder(values.execOrder)
	if err != nil {
		return parsedRollbackSettings{}, err
	}
	migrationLockTimeout, err := migrateflags.ParseMigrationLockTimeout(values.migrationLockTimeout)
	if err != nil {
		return parsedRollbackSettings{}, err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(values.connectTimeout)
	if err != nil {
		return parsedRollbackSettings{}, err
	}
	var providerOptions []migrator.FSProviderOption
	if values.runner.TransactionOwner() == lintext.TransactionOwnerFile {
		providerOptions = append(providerOptions, migrator.WithOwnTransactions())
	}
	return parsedRollbackSettings{
		timeouts:             timeouts,
		execOrder:            execOrder,
		migrationLockTimeout: migrationLockTimeout,
		connectTimeout:       connectTimeout,
		providerOptions:      providerOptions,
	}, nil
}

// Command is the command a run document names.
const Command = "migrations down"

// typedCommand is the command a refusal names: [Command], or the spelling the
// operator typed on the surface that forwarded to it, such as `migrate down`.
func typedCommand(cmd *cobra.Command) string {
	if typed, forwarded := cmdadapter.ForwardedAs(cmd); forwarded {
		return typed
	}
	return Command
}

// errJSONNeedsConfirmation refuses a document run that would stop to ask a
// person: the caller reading the document cannot answer the prompt, and a run
// that ends at a declined prompt has no outcome the document can name.
var errJSONNeedsConfirmation = fmt.Errorf("--%s needs --%s or --%s: a run that prints a document for a program cannot stop to ask for confirmation",
	jsonFlag, confirmFlag, dryRunFlag)

func migrateDownCommand(cmd *cobra.Command, opts *options, document *migraterun.Document) error {
	if opts.jsonOutput && !opts.skipConfirm && !opts.dryRun {
		return errJSONNeedsConfirmation
	}
	if err := dbcli.CheckRunAttribution(opts.actor, opts.actorOrigin, opts.runReference); err != nil {
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
	opts.featureRuntime, err = engineruntime.Resolve(cmd.Context())
	if err != nil {
		return err
	}
	resolvedOpts, err := resolveProjectOptions(cmd, *opts, projectCfg)
	if err != nil {
		return err
	}
	dbURL := resolvedOpts.dbURL
	migrationsDir := resolvedOpts.migrationsDir
	targetVersionValue := resolvedOpts.target
	migrationsSchema := resolvedOpts.migrationsSchema
	migrationsTable := resolvedOpts.migrationsTable

	runtime, err := startObservability(cmd, opts)
	if err != nil {
		return err
	}
	defer shutdownObservability(runtime)
	// Under --json the document is the output, so everything written for a
	// person goes to standard error.
	emit := cliobs.NewEmitter(humanOutput(cmd, opts), runtime)

	if dbURL == "" {
		return dbcli.MissingProjectValue(projectCfg, fmt.Errorf("database URL is required"))
	}

	lockRequest := migratelock.NewRequests(
		cmd,
		dbURL,
		projectCfg.StringValue(projectconfig.StringMigrationMigrationLockTimeout).Present,
		projectCfg.StringValue(projectconfig.StringMigrationMigrationLockName).Present,
	)
	if err := lockRequest.DecideFromURL(); err != nil {
		return err
	}

	if migrationsDir == "" {
		return dbcli.MissingProjectValue(projectCfg, fmt.Errorf("migrations directory is required"))
	}
	targetVersion, err := strconv.ParseInt(targetVersionValue, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid target version %q: %w", targetVersionValue, err)
	}
	if targetVersion < 0 {
		return fmt.Errorf("target version must be >= 0")
	}

	dirFormat, err := migrationfile.ParseDirFormat(resolvedOpts.dirFormat)
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
	document.ServerDetails.Options.Source = migrationsFS
	migrationsDir = source.Display
	dirFormat = source.DirFormat

	// The integrity gate runs before the database connection, before the
	// confirmation prompt, and before any rollback SQL is read: a run that is
	// going to refuse must not first ask the operator to type YES, and must not
	// hold a connection while it decides. `migrations up` verifies here too.
	// Without the gate on `down`, a rewritten `_init.down.sql` under a stale
	// ptah.sum executes at exit 0 while `up` on the same directory exits 2.
	//
	// --verify-sum adds the half the always-on gate deliberately does not
	// cover: a directory carrying NO sum passes the gate, because there is no
	// recorded intent to compare it against, and this flag is the spelling that
	// demands one. It also qualifies what a
	// verification through a movable OCI tag actually established, which is the
	// same sentence `up` prints — shared, not copied, so the destructive verb
	// cannot say less than the constructive one.
	//
	// integrityPolicy was resolved at the command boundary, above, so the
	// escape hatch is decided once for the whole invocation rather than read
	// again here.
	if err := migrationsource.Verify(
		cmd.Context(), cmd.ErrOrStderr(), emit, runtime, source, dirFormat, integrityPolicy,
		migrationsource.VerifyOptions{RequireSum: opts.verifySum, Verbose: opts.verbose},
	); err != nil {
		return err
	}

	revisionFormat, err := migrateflags.ParseRevisionTableFormat(resolvedOpts.revisionTableFormat)
	if err != nil {
		return err
	}

	if opts.verbose {
		emit.Printf("Connecting to database: %s\n", dburldisplay.Format(dbURL))
	}

	settings, err := parseRollbackSettings(rollbackSettingValues{
		lockTimeout:          resolvedOpts.lockTimeout,
		statementTimeout:     resolvedOpts.statementTimeout,
		execOrder:            resolvedOpts.execOrder,
		migrationLockTimeout: resolvedOpts.migrationLockTimeout,
		connectTimeout:       resolvedOpts.connectTimeout,
		runner:               resolvedOpts.resolvedRunner,
	})
	if err != nil {
		return err
	}

	conn, err := connectTarget(cmd, projectCfg, dbURL, settings.timeouts, settings.connectTimeout)
	if err != nil {
		return err
	}
	defer dbschema.CloseAndWarn(conn)

	// Before any migrator call: reading the status initializes the revision
	// table, and a decision taken after the first write answers nothing.
	if err := decideConnected(cmd, projectCfg, lockRequest, conn, settings.timeouts); err != nil {
		return err
	}

	// Set dry run mode if requested
	conn.SchemaWriter().SetDryRun(opts.dryRun)

	mig, err := newDownMigrator(downMigratorSpec{
		conn: conn, migrationsFS: migrationsFS, emit: emit, projectCfg: projectCfg, opts: opts, resolved: resolvedOpts,
		settings: settings, runtime: runtime, dirFormat: dirFormat, revisionFormat: revisionFormat,
		migrationsSchema: migrationsSchema, migrationsTable: migrationsTable,
	})
	if err != nil {
		return err
	}
	document.Migrator = mig

	// Before the lock, so a run that waits for it has said which lock it waits
	// for.
	writeDownBanner(emit, downBanner{
		dryRun:        opts.dryRun,
		dbURL:         dbURL,
		dialect:       conn.Info().Dialect,
		migrationsDir: migrationsDir,
		dirFormat:     dirFormat,
		lockName:      resolvedOpts.lockName,
	})

	// The run holds the migration lock from its first read: reading the status
	// initializes the revision metadata, and a rollback decided on a history
	// another runner is changing answers nothing. Each operation of mig called
	// with the context of cmd below runs under this lock and takes no second
	// one. A confirmation prompt is the exception: a person reading the plan
	// must not hold back every other runner of the lock, so the lock is
	// released before the prompt, and the rollback takes it again and checks
	// under it that the migrations above the target are still the ones shown.
	// The run reports once the lock is released, through report.
	outer := cmd.Context()
	var confirmed *rollbackRun
	var report downReport
	err = mig.HoldMigrationLock(outer, "migrate down", func(ctx context.Context) error {
		cmd.SetContext(ctx)
		defer cmd.SetContext(outer)

		// A tag names a directory state; resolving it before the target line
		// means the line reports the version the rollback will actually stop at
		// rather than the numeric default the tag replaced.
		if resolvedOpts.toTag != "" {
			if targetVersion, err = resolveDownTag(cmd, emit, downTagResolution{
				toTag:            resolvedOpts.toTag,
				conn:             conn,
				migrationsFS:     migrationsFS,
				dirFormat:        dirFormat,
				migrationsSchema: migrationsSchema,
				migrationsTable:  migrationsTable,
				migrationsEngine: opts.migrationsEngine,
				revisionFormat:   revisionFormat,
			}); err != nil {
				return err
			}
		}
		emit.Printf("Target version: %d\n", targetVersion)
		emit.Println()

		status, err := rollbackStatus(cmd.Context(), mig, emit, conn.Info().Dialect)
		if err != nil {
			return err
		}

		if status.CurrentVersion <= targetVersion {
			report, err = finishDownAtOrBelowTarget(cmd.Context(), downNoopInputs{
				migrator:      mig,
				runtime:       runtime,
				emit:          emit,
				document:      document,
				dialect:       conn.Info().Dialect,
				status:        status,
				targetVersion: targetVersion,
				dryRun:        opts.dryRun,
			})
			return err
		}

		// Get applied migrations from the database
		appliedMigrations, err := mig.GetAppliedMigrations(cmd.Context())
		if err != nil {
			return fmt.Errorf("error getting applied migrations: %w", err)
		}

		// Calculate which migrations will be rolled back
		migrationsToRollback := versionsAboveTarget(appliedMigrations, targetVersion)

		emitRollbackPlan(emit, opts, status.CurrentVersion, targetVersion, migrationsToRollback)

		// --plan takes the dev database for itself: it builds the target schema
		// there to derive the plan from. Verifying a file-based rollback on it
		// first would run exactly the down bodies --plan exists to bypass, and
		// would fail on the migration with no down body that is the reason to
		// reach for --plan at all (stokaro/ptah#1621).
		shadowVerificationURL := resolvedOpts.shadowDB
		if resolvedOpts.plan {
			shadowVerificationURL = ""
		}
		if err := verifyRollbackOnShadow(cmd.Context(), shadowVerification{
			targetConnection: conn,
			shadowDB:         shadowVerificationURL,
			migrationsFS:     migrationsFS,
			currentVersion:   status.CurrentVersion,
			targetVersion:    targetVersion,
			dirFormat:        dirFormat,
			atlasEnv:         resolvedOpts.atlasEnv,
			connectTimeout:   settings.connectTimeout,
			skipChecks:       resolvedOpts.skipChecks,
			providerOptions:  settings.providerOptions,
		}, emit); err != nil {
			return err
		}

		inputs := rollbackInputs{
			migrationsFS: migrationsFS, targetVersion: targetVersion, dirFormat: dirFormat,
			atlasEnv: resolvedOpts.atlasEnv, connectTimeout: settings.connectTimeout,
			migrationsTable: migrationsTable, providerOptions: settings.providerOptions,
		}
		derived, err := planRollback(cmd, resolvedOpts, newDynamicRollback(resolvedOpts, opts, conn, inputs), emit)
		if err != nil {
			return err
		}

		run := rollbackRun{
			opts: opts, resolved: resolvedOpts, migrator: mig, conn: conn, runtime: runtime, emit: emit,
			document: document, dbURL: dbURL, selected: migrationsToRollback, derived: derived, inputs: inputs,
			currentVersion: status.CurrentVersion,
		}
		if rollbackPrompts(opts) {
			confirmed = &run
			return nil
		}
		var runErr error
		report, runErr = runRollback(cmd, run)
		return runErr
	})
	if report != nil {
		return report(err)
	}
	if err != nil || confirmed == nil {
		return err
	}
	return confirmAndRollBack(cmd, *confirmed)
}

// confirmAndRollBack asks for confirmation with the migration lock released,
// then takes the lock again and rolls back what run selected, rechecking under
// the lock that it is still what the prompt showed.
func confirmAndRollBack(cmd *cobra.Command, run rollbackRun) error {
	ok, err := confirmRollbackPrompt(cmd, run.opts, run.currentVersion, run.inputs.targetVersion, run.selected)
	if err != nil || !ok {
		return err
	}
	run.recheck = true
	outer := cmd.Context()
	var report downReport
	err = run.migrator.HoldMigrationLock(outer, "migrate down", func(ctx context.Context) error {
		cmd.SetContext(ctx)
		defer cmd.SetContext(outer)
		var runErr error
		report, runErr = runRollback(cmd, run)
		return runErr
	})
	if report == nil {
		return err
	}
	return report(err)
}

// recheckConfirmed reads the applied migrations again and refuses with a
// [*confirmedRollbackChangedError] when the ones above target are not the
// confirmed ones. A person confirmed rolling back these versions and no
// others; another runner may have applied or rolled back one while the prompt
// waited without the lock. The caller holds the lock.
func recheckConfirmed(ctx context.Context, mig *migrator.Migrator, target int64, confirmed []int64) error {
	applied, err := mig.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("error getting applied migrations: %w", err)
	}
	now := newestFirst(versionsAboveTarget(applied, target))
	if want := newestFirst(confirmed); !slices.Equal(now, want) {
		return &confirmedRollbackChangedError{target: target, confirmed: want, applied: now}
	}
	return nil
}

// confirmedRollbackChangedError reports that the migrations applied above the
// target changed between the confirmation prompt and the rollback. It is
// returned before any statement runs.
type confirmedRollbackChangedError struct {
	target             int64
	confirmed, applied []int64
}

func (e *confirmedRollbackChangedError) Error() string {
	return fmt.Sprintf("the rollback to version %d was confirmed for the applied migrations %v, "+
		"and the migrations applied above version %d are now %v; nothing was rolled back. "+
		"Run migrations down again to confirm the rollback of the database as it is now",
		e.target, e.confirmed, e.target, e.applied)
}

// rollbackPrompts reports whether the run asks for confirmation before it
// rolls back.
func rollbackPrompts(opts *options) bool {
	return !opts.dryRun && !opts.skipConfirm
}

// downMigratorSpec is what the rollback's migrator is built from.
type downMigratorSpec struct {
	conn             *dbschema.DatabaseConnection
	migrationsFS     fs.FS
	emit             cliobs.Emitter
	projectCfg       projectconfig.Config
	opts             *options
	resolved         options
	settings         parsedRollbackSettings
	runtime          *cliobs.Runtime
	dirFormat        migrationfile.DirFormat
	revisionFormat   migrator.RevisionTableFormat
	migrationsSchema string
	migrationsTable  string
}

// newDownMigrator builds the migrator that reads the history and rolls it
// back, under the lock name and timeout the run resolved.
func newDownMigrator(spec downMigratorSpec) (*migrator.Migrator, error) {
	interceptor := downOnlineDDLInterceptor(spec.emit, spec.projectCfg, spec.opts.dryRun)
	mig, err := migrator.NewFSMigrator(
		spec.conn,
		spec.migrationsFS,
		append([]migrator.FSProviderOption{
			migrator.WithStatementInterceptor(interceptor),
			migrator.WithMigrationDirFormat(spec.dirFormat),
			migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: spec.resolved.atlasEnv}),
		}, spec.settings.providerOptions...)...,
	)
	if err != nil {
		return nil, fmt.Errorf("error registering migrations: %w", err)
	}
	return mig.WithMigrationsTable(spec.migrationsSchema, spec.migrationsTable).
		WithMigrationsEngine(spec.opts.migrationsEngine).
		WithActor(spec.opts.actor).
		WithActorOrigin(spec.opts.actorOrigin).
		WithRunReference(spec.opts.runReference).
		WithMigrationLog(spec.projectCfg.Migration.MigrationLogEnabled()).
		WithRevisionTableFormat(spec.revisionFormat).
		WithSkipChecks(spec.resolved.skipChecks).
		WithDefaultTimeouts(spec.settings.timeouts).
		WithExecOrder(spec.settings.execOrder).
		WithMigrationLockTimeout(spec.settings.migrationLockTimeout).
		WithMigrationLockName(spec.resolved.lockName.Value).
		WithLogger(spec.runtime.Logger()).
		WithObserver(spec.runtime.Observer()), nil
}

// rollbackRun is what the rollback and its report need once the operator has
// confirmed it. The report uses document and emit after the migration lock is
// released.
type rollbackRun struct {
	opts     *options
	resolved options
	migrator *migrator.Migrator
	conn     *dbschema.DatabaseConnection
	runtime  *cliobs.Runtime
	emit     cliobs.Emitter
	document *migraterun.Document
	dbURL    string
	// selected is the versions above the target the run read, and the
	// versions a --plan rollback was derived for.
	selected []int64
	// derived is the plan of a --plan rollback, derived before the prompt.
	derived []string
	inputs  rollbackInputs
	// currentVersion is the version the run read before the prompt.
	currentVersion int64
	// recheck is set when the run released the migration lock for the
	// confirmation prompt and holds it again. A rollback through the down
	// bodies then reads the applied migrations again before anything else and
	// refuses when the ones above the target are not the ones the prompt
	// showed; a --plan rollback always does, through
	// [migrator.Migrator.MigrateDownDerived].
	recheck bool
}

// downReport reports a run of migrations down once the migration lock it ran
// under is released: in the document under --json, and in the closing lines.
// runErr is the outcome of the run and of the lock together, because a lock
// lost or not released fails a run whose statements all committed, and a report
// written while the lock was held could not say so.
type downReport func(runErr error) error

// runRollback rolls back and reads the history it left, under the migration
// lock the caller holds. Its error is the rollback's, and the caller passes the
// lock's outcome of it to the report.
func runRollback(cmd *cobra.Command, r rollbackRun) (downReport, error) {
	mig := r.migrator
	// The plan the migrator selects under its lock is what the document
	// reports, and none when it selected nothing there. A derived rollback
	// reports the versions it was derived for until the migrator selects them
	// under its lock: a dry run takes no lock, and a run whose target changed
	// is refused before the selection.
	var plan *migrator.MigrationPlan
	if r.resolved.plan {
		plan = &migrator.MigrationPlan{Direction: migrator.MigrationDirectionDown, Versions: newestFirst(r.selected)}
	}
	preflightHook := capturePlan(&plan, downPreflightHook(cmd, r.opts, r.runtime, r.conn, r.dbURL, r.resolved, r.emit))

	// Run down migrations, once the versions the prompt showed are still the
	// applied ones: refused here, before the migrator's own checks, a rollback
	// another runner emptied in the meantime is not reported as done.
	var runErr error
	if r.recheck && !r.resolved.plan {
		runErr = recheckConfirmed(cmd.Context(), mig, r.inputs.targetVersion, r.selected)
	}
	if runErr == nil {
		runErr = executeRollback(cmd, buildRollbackExecution(r, preflightHook))
	}
	if runErr != nil {
		runErr = fmt.Errorf("error running down migrations: %w", runErr)
	}

	// The final status read does not inherit the cancelation: a rollback that
	// committed before an interrupt still reports the version it left behind.
	// It is read after a failure too, because the run that stopped is the one
	// whose caller most needs to know what the database holds.
	finalStatus, statusErr := mig.GetMigrationStatus(context.WithoutCancel(cmd.Context()))
	if statusErr != nil {
		finalStatus = nil
	}
	report := &rollbackReport{run: r, plan: plan, finalStatus: finalStatus, statusErr: statusErr}
	return report.finish, runErr
}

// rollbackReport is what a rollback leaves to report once the migration lock
// it ran under is released.
type rollbackReport struct {
	run         rollbackRun
	plan        *migrator.MigrationPlan
	finalStatus *migrator.MigrationStatus
	statusErr   error
}

// finish is the rollback's [downReport].
func (rr *rollbackReport) finish(runErr error) error {
	r := rr.run
	opts, emit := r.opts, r.emit
	// Last on every path from here, so it follows any line that says success.
	// Under --json the document carries the same report.
	if !opts.jsonOutput {
		defer emitLostLogEntries(emit, r.migrator)
	}
	if err := r.document.Write(migrator.RunEvidence{
		Plan: rr.plan, After: rr.finalStatus, Err: runErr, DryRun: opts.dryRun,
	}); err != nil {
		return errors.Join(runErr, err)
	}
	if runErr != nil {
		return r.document.ServerDetails.Explain(runErr)
	}
	if rr.statusErr != nil {
		return fmt.Errorf("error getting final migration status: %w", rr.statusErr)
	}

	emitMigrateDownSummary(emit, opts, r.inputs.targetVersion, r.selected, rr.finalStatus)

	return nil
}

// capturePlan records the plan the migrator selected under its lock in *into,
// then runs next. The hook is the one place the migrator hands a rollback's
// selection out, and it runs before any rollback SQL does.
func capturePlan(into **migrator.MigrationPlan, next migrator.PreMigrationHook) migrator.PreMigrationHook {
	return func(ctx context.Context, plan migrator.MigrationPlan) error {
		*into = &plan
		if next == nil {
			return nil
		}
		return next(ctx, plan)
	}
}

// humanOutput selects where this command's human-facing output goes.
func humanOutput(cmd *cobra.Command, opts *options) io.Writer {
	if opts.jsonOutput {
		return cmd.ErrOrStderr()
	}
	return cmd.OutOrStdout()
}

// emitMigrateDownSummary says how the rollback ended.
func emitMigrateDownSummary(
	emit cliobs.Emitter,
	opts *options,
	targetVersion int64,
	migrationsToRollback []int64,
	finalStatus *migrator.MigrationStatus,
) {
	emit.Println()
	if opts.dryRun {
		emit.Println("✅ Dry run completed successfully!")
		emit.Printf("Would have rolled back to version: %d\n", targetVersion)
		if len(migrationsToRollback) > 0 {
			emit.Printf("Would have rolled back these migrations: %v\n", migrationsToRollback)
		}
		return
	}
	emit.Println("✅ Migration rollback completed successfully!")
	emit.Printf("Database is now at version: %d\n", finalStatus.CurrentVersion)
}

// emitLostLogEntries tells a person that the migration log did not get
// everything the rollback tried to write. Without --json this and the warnings
// above it are the whole report.
func emitLostLogEntries(emit cliobs.Emitter, mig *migrator.Migrator) {
	if notice := dbcli.LostLogEntriesNotice(mig.MigrationLogReport()); notice != "" {
		emit.Println(notice)
	}
}

// decideConnected refuses, against the server the run reached, the requests
// that server may not carry: the migration lock, and timeouts for every
// migration.
func decideConnected(
	cmd *cobra.Command,
	projectCfg projectconfig.Config,
	lockRequest migratelock.Requests,
	conn *dbschema.DatabaseConnection,
	timeouts migrationfile.Timeouts,
) error {
	if err := lockRequest.DecideSession(conn); err != nil {
		return err
	}
	server := conn.Info()
	return newTimeoutRequest(cmd, projectCfg, lockRequest.Timeout.DBURL).Decide(server.Dialect, server.Capabilities, timeouts)
}

// newTimeoutRequest is the run-wide timeout input of this command against
// dbURL.
func newTimeoutRequest(cmd *cobra.Command, projectCfg projectconfig.Config, dbURL string) migratetimeout.Request {
	return migratetimeout.Request{
		Cmd:                 cmd,
		LockFlag:            lockTimeoutFlag,
		StatementFlag:       statementTimeoutFlag,
		LockFromConfig:      projectCfg.StringValue(projectconfig.StringMigrationLockTimeout).Present,
		StatementFromConfig: projectCfg.StringValue(projectconfig.StringMigrationStatementTimeout).Present,
		DBURL:               dbURL,
	}
}

// connectTarget refuses run-wide timeouts the dialect dbURL names cannot
// carry, and connects. The refusal comes first: connecting is what creates a
// sqlite:// target's file.
func connectTarget(
	cmd *cobra.Command,
	projectCfg projectconfig.Config,
	dbURL string,
	timeouts migrationfile.Timeouts,
	connectTimeout time.Duration,
) (*dbschema.DatabaseConnection, error) {
	if err := newTimeoutRequest(cmd, projectCfg, dbURL).DecideFromURL(timeouts); err != nil {
		return nil, err
	}
	connectCtx, cancelConnect := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancelConnect()
	conn, err := dbschema.ConnectToDatabase(connectCtx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}
	return conn, nil
}

// newestFirst returns versions highest first, the order the migrator selects
// a rollback in, as a slice of its own.
func newestFirst(versions []int64) []int64 {
	sorted := slices.Clone(versions)
	slices.SortFunc(sorted, func(a, b int64) int { return cmp.Compare(b, a) })
	return sorted
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

// emitRollbackPlan prints what the rollback selected, and under --verbose the
// versions it selected and the span they cover.
func emitRollbackPlan(
	emit cliobs.Emitter,
	opts *options,
	currentVersion, targetVersion int64,
	versions []int64,
) {
	emit.Printf("Migrations to roll back: %d\n", len(versions))
	if opts.verbose {
		emit.Printf("Will roll back from version %d to %d\n", currentVersion, targetVersion)
		if len(versions) > 0 {
			emit.Printf("Specific migrations to rollback: %v\n", versions)
		}
	}
	emit.Println()
}

// downNoopInputs is what the run with nothing to roll back needs to finish.
type downNoopInputs struct {
	migrator      *migrator.Migrator
	runtime       *cliobs.Runtime
	emit          cliobs.Emitter
	document      *migraterun.Document
	dialect       string
	status        *migrator.MigrationStatus
	targetVersion int64
	dryRun        bool
}

// rollbackStatus reads and prints the migration status a rollback starts
// from, and refuses a rollback over a revision a failed or interrupted run
// left, with the command that ends that revision, before the version decides
// anything.
//
// A revision a rollback left dirty is not applied, so the version reads as
// already rolled back over it, and the branch that answers a database at or
// below its target would report the rollback done. The refusal is the
// migrator's, the one every run issues, so this branch and the rollback refuse
// the same rows.
func rollbackStatus(
	ctx context.Context,
	mig *migrator.Migrator,
	emit cliobs.Emitter,
	dialect string,
) (*migrator.MigrationStatus, error) {
	status, err := mig.GetMigrationStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting migration status: %w", err)
	}
	emit.Printf("Current version: %d\n", status.CurrentVersion)
	emit.Printf("Total migrations: %d\n", status.TotalMigrations)

	err = mig.RefuseDirtyRevision(ctx)
	if err == nil {
		return status, nil
	}
	dirty, ok := errors.AsType[*migrator.DirtyMigrationError](err)
	if !ok {
		return nil, fmt.Errorf("error reading the migration revisions: %w", err)
	}
	return nil, fmt.Errorf("error running down migrations: %w\n%s", err, dirtyhint.Recovery(dirtyhint.Revision{
		Revision:        &dirty.Revision,
		Dialect:         dialect,
		OwnTransactions: mig.RevisionBodyOpensOwnTransactions(dirty.Revision),
	}))
}

// finishDownAtOrBelowTarget answers the run whose database is already at or
// below the version it was asked for, and returns its report.
//
// It verifies applied checksums first. The rollback path does that before it
// rolls anything back, and this is the branch that never reaches it, so an
// applied migration whose file no longer accounts for its recorded checksum
// would otherwise leave here reported as success (stokaro/ptah#3438). The
// question goes to the migrator rather than to a second reading of the status,
// so both branches refuse on the same rule.
//
// The report says nothing of its own on a lock lost or not released: the run
// sent no statement, so the document the command writes for an error before
// the run is the true one, though it leaves out the status this run read. The
// no-op observation is not recorded for that run either.
func finishDownAtOrBelowTarget(ctx context.Context, in downNoopInputs) (downReport, error) {
	if _, err := in.migrator.VerifyAppliedChecksums(ctx); err != nil &&
		!in.migrator.AppliesOverMissingMigration(err) {
		return nil, err
	}
	return func(runErr error) error {
		if runErr != nil {
			return runErr
		}
		observeNoopDown(in.runtime, in.dialect, in.status.CurrentVersion, in.targetVersion)
		in.emit.Printf("✅ Database is already at or below target version %d!\n", in.targetVersion)
		return in.document.Write(migrator.RunEvidence{Plan: &migrator.MigrationPlan{}, After: in.status, DryRun: in.dryRun})
	}, nil
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
	dirFormat        migrationfile.DirFormat
	atlasEnv         string
	connectTimeout   time.Duration
	skipChecks       bool
	// providerOptions are the rollback's own, so the shadow database runs each
	// file the way the target will.
	providerOptions []migrator.FSProviderOption
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

	err = shadow.VerifyRollback(ctx, shadow.RollbackVerifyOptions{
		TargetConnection:  v.targetConnection,
		ShadowDatabaseURL: shadowDB,
		FS:                v.migrationsFS,
		CurrentVersion:    v.currentVersion,
		TargetVersion:     v.targetVersion,
		ProviderOptions: append([]migrator.FSProviderOption{
			migrator.WithMigrationDirFormat(v.dirFormat),
			migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: v.atlasEnv}),
		}, v.providerOptions...),
		ConnectTimeout: v.connectTimeout,
		SkipChecks:     v.skipChecks,
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
	runtime, err := cliobs.Start(context.Background(), cliobs.Options{
		Command:     "migrations.down",
		LogFormat:   opts.logFormat,
		LogLevel:    opts.logLevel,
		MetricsAddr: opts.metricsAddr,
		LogWriter:   logWriter,
	})
	if err != nil {
		return nil, err
	}
	runtime.ShareWithRun(cmd.Context())
	return runtime, nil
}

// confirmRollbackPrompt routes the confirmation prompt to stdout, or to stderr
// under JSON log format where stdout carries structured output.
func confirmRollbackPrompt(cmd *cobra.Command, opts *options, currentVersion, targetVersion int64, migrationsToRollback []int64) (bool, error) {
	promptWriter := cmd.OutOrStdout()
	if opts.logFormat == "json" || opts.jsonOutput {
		promptWriter = cmd.ErrOrStderr()
	}
	return confirmRollback(opts, promptWriter, cmd.InOrStdin(), currentVersion, targetVersion, migrationsToRollback)
}

func confirmRollback(opts *options, prompt io.Writer, input io.Reader, currentVersion, targetVersion int64, migrationsToRollback []int64) (bool, error) {
	if !rollbackPrompts(opts) {
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

// downTagResolution carries what resolving --to-tag needs.
type downTagResolution struct {
	toTag            string
	conn             *dbschema.DatabaseConnection
	migrationsFS     fs.FS
	dirFormat        migrationfile.DirFormat
	migrationsSchema string
	migrationsTable  string
	migrationsEngine string
	revisionFormat   migrator.RevisionTableFormat
}

// resolveDownTag turns --to-tag into the version the rollback stops at.
//
// Passing --target as well is refused rather than resolved by precedence: both
// name where to stop, and silently preferring one would roll back to a version
// the operator did not choose.
func resolveDownTag(cmd *cobra.Command, emit cliobs.Emitter, r downTagResolution) (int64, error) {
	if cmd.Flags().Changed(targetFlag) {
		return 0, fmt.Errorf("--%s and --%s both name where to stop; pass one",
			targetFlag, toTagFlag)
	}
	resolver, err := migrator.NewFSMigrator(r.conn, r.migrationsFS,
		migrator.WithMigrationDirFormat(r.dirFormat))
	if err != nil {
		return 0, fmt.Errorf("error registering migrations: %w", err)
	}
	resolver = resolver.
		WithMigrationsTable(r.migrationsSchema, r.migrationsTable).
		WithMigrationsEngine(r.migrationsEngine).
		WithRevisionTableFormat(r.revisionFormat)
	version, err := resolver.ResolveMigrationTag(cmd.Context(), r.toTag)
	if err != nil {
		return 0, err
	}
	emit.Printf("Target tag: %s\n", r.toTag)
	return version, nil
}

// downBanner is what the run reports about itself before it starts.
type downBanner struct {
	dryRun        bool
	dbURL         string
	dialect       string
	migrationsDir string
	dirFormat     migrationfile.DirFormat
	// lockName is the migration lock the operator chose, and is reported only
	// then: the default is the lock a run takes without being asked.
	lockName migratelock.Name
}

func writeDownBanner(emit cliobs.Emitter, b downBanner) {
	if b.dryRun {
		emit.Println("=== DRY RUN MODE ===")
		emit.Println("No actual changes will be made to the database")
		emit.Println()
	}
	emit.Println("=== MIGRATE DOWN ===")
	emit.Printf("Database: %s\n", dburldisplay.Format(b.dbURL))
	emit.Printf("Dialect: %s\n", b.dialect)
	emit.Printf("Migrations directory: %s\n", b.migrationsDir)
	emit.Printf("Migration directory format: %s\n", b.dirFormat)
	if b.lockName.Explicit() {
		emit.Printf("Migration lock: %s\n", b.lockName.Value)
	}
}

// downOnlineDDLInterceptor builds the statement interceptor for a rollback and
// reports the routing it will do.
//
// Online-DDL routing works for down migrations too: a rollback ALTER on a large
// table is just as lock-heavy as the forward one.
func downOnlineDDLInterceptor(
	emit cliobs.Emitter,
	projectCfg projectconfig.Config,
	dryRun bool,
) *onlineddl.Executor {
	cfg := projectCfg.OnlineDDL
	if cfg.Enabled() {
		emit.Printf("Online DDL: tool=%s threshold_rows=%d\n", cfg.Tool, cfg.ThresholdRows)
	}
	return onlineddl.New(cfg).WithDryRun(dryRun)
}

// dynamicRollback carries what a --plan rollback needs.
type dynamicRollback struct {
	runtime        engine.SchemaRuntime
	conn           *dbschema.DatabaseConnection
	devURL         string
	migrationsFS   fs.FS
	targetVersion  int64
	dirFormat      migrationfile.DirFormat
	atlasEnv       string
	connectTimeout time.Duration
	dryRun         bool
	// revisionsTable names the revision table this run was configured with,
	// which the comparison leaves out.
	revisionsTable string
	// exclude is the project's exclude list: the objects the derived plan
	// leaves alone, as `schema apply` leaves them. excludeSources names where
	// each selector was written.
	exclude        []string
	excludeSources atlasfilter.ExcludeSources
	// allowUnmatchedExclude turns the refusal of a selector that matched
	// nothing into a warning.
	allowUnmatchedExclude bool
	// statements is the plan deriveRollback derived before the prompt, and
	// the statements the rollback runs.
	statements []string
	// derivedFor is the versions above the target the plan was derived for.
	// The rollback runs only while they are still the applied ones.
	derivedFor []int64
	// providerOptions are the rollback's own. The target schema is built on
	// the dev database by running the up files, and a runner that leaves a
	// file its own transactions runs them that way there too.
	providerOptions []migrator.FSProviderOption
}

// planRollback derives the rollback a --plan run applies, and nothing for a
// run that applies the down files. It runs before the prompt, as `schema
// apply` plans before it asks: the operator approves the statements that run,
// and a plan the run refuses is refused before anyone is asked to approve it.
func planRollback(cmd *cobra.Command, resolved options, r dynamicRollback, emit cliobs.Emitter) ([]string, error) {
	if !resolved.plan {
		return nil, nil
	}
	return deriveRollback(cmd, r, emit)
}

// deriveRollback plans the rollback --plan applies, from the schema
// difference instead of the down bodies the directory carries, and prints it.
//
// It runs before the confirmation prompt. The dev database is released once
// the plan is read, since nothing after the prompt needs it, so a declined
// prompt still costs the replay that built the target schema there.
func deriveRollback(cmd *cobra.Command, r dynamicRollback, emit cliobs.Emitter) ([]string, error) {
	if strings.TrimSpace(r.devURL) == "" {
		return nil, fmt.Errorf("--%s requires --%s: the schema at version %d has to be built somewhere "+
			"before it can be compared against the live database",
			planFlag, shadowDBFlag, r.targetVersion)
	}
	devURL, releaseDev, err := devdocker.Resolve(cmd.Context(), r.devURL, devdocker.Options{})
	if err != nil {
		return nil, err
	}
	defer releaseDev()

	statements, err := shadow.PlanDynamicRollback(cmd.Context(), shadow.DynamicRollbackOptions{
		Runtime:          r.runtime,
		TargetConnection: r.conn,
		DevDatabaseURL:   devURL,
		FS:               r.migrationsFS,
		TargetVersion:    r.targetVersion,
		ProviderOptions: append([]migrator.FSProviderOption{
			migrator.WithMigrationDirFormat(r.dirFormat),
			migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: r.atlasEnv}),
		}, r.providerOptions...),
		ConnectTimeout: r.connectTimeout,
		RevisionsTable: r.revisionsTable,
		Exclude:        r.exclude,
		ExcludeSources: r.excludeSources,
		// The derived plan executes, as the plan of `schema apply` does, so a
		// selector that protects nothing is refused before any statement runs.
		RefuseUnmatchedExclude: !r.allowUnmatchedExclude,
		Diagnostics:            cmd.ErrOrStderr(),
	})
	if err != nil {
		// The refusal is worded as `schema apply` words it, naming the
		// command the operator typed. A failure the planning reported beside
		// it, such as a dev database it could not empty, stays in the error.
		return nil, atlasschema.UnmatchedExcludeRefusal(typedCommand(cmd)+" --"+planFlag, err)
	}

	emit.Printf("Derived rollback plan: %d statements\n", len(statements))
	for _, statement := range statements {
		emit.Printf("  %s\n", statement)
	}
	emit.Println()
	return statements, nil
}

// runDynamicRollback applies the plan [deriveRollback] derived, instead of
// running the down bodies the directory carries.
//
// The plan was derived before the prompt, with no lock held, so another runner
// may have applied or rolled back a migration while the operator read it. The
// migrator takes the migration lock, reads the applied migrations again, and
// refuses before any statement runs when they are not the ones the plan was
// derived for: the statements the operator approved describe a database that
// is no longer there. The lock is held from that reading until the revision
// boundary has moved, and never across the prompt, where it would keep every
// other runner waiting on a person.
//
// The revision boundary moves through the path `migrations set` uses, so a
// derived rollback leaves the bookkeeping in the state a file-based one would:
// the rows above the target are removed, and nothing below is disturbed. Doing
// it after the statements, not before, means a plan that fails partway leaves
// revisions that still describe a database ahead of the target -- which is the
// honest reading of what happened, and what `migrations repair` expects.
func runDynamicRollback(cmd *cobra.Command, r rollbackExecution) error {
	if r.dryRun {
		return nil
	}
	err := r.migrator.MigrateDownDerived(cmd.Context(), migrator.DerivedRollback{
		TargetVersion: r.targetVersion,
		Versions:      r.derivedFor,
		Apply: func(ctx context.Context) error {
			return applyDerivedStatements(ctx, r.conn, r.statements)
		},
	}, r.preflightHook)
	if _, stale := errors.AsType[*migrator.StaleDerivedRollbackError](err); stale {
		return fmt.Errorf("%w. Run %s --%s again to derive the rollback from the database as it is now",
			err, typedCommand(cmd), planFlag)
	}
	return err
}

// applyDerivedStatements runs the derived statements, stopping at the first
// that fails.
//
// The statements go through the connection's schema writer, as every other
// schema statement of a migration does. A connection an executable provider
// serves exposes no database/sql operations, so a statement sent through
// ExecContext is refused there before it reaches the provider.
func applyDerivedStatements(ctx context.Context, conn *dbschema.DatabaseConnection, statements []string) error {
	for _, statement := range statements {
		if err := conn.Writer().ExecuteSQL(ctx, statement); err != nil {
			return fmt.Errorf("apply derived rollback statement %q: %w", statement, err)
		}
	}
	return nil
}

// rollbackExecution is everything either rollback strategy needs.
type rollbackExecution struct {
	plan          bool
	migrator      *migrator.Migrator
	preflightHook migrator.PreMigrationHook
	dynamicRollback
}

// executeRollback picks between running the authored down bodies and deriving
// the rollback from the schema difference.
//
// The choice lives here rather than inline so the two strategies read as
// alternatives. They are not variations on one path: one runs SQL a person
// wrote and reviewed, the other runs SQL inferred from structure minutes ago.
func executeRollback(cmd *cobra.Command, r rollbackExecution) error {
	if r.plan {
		return runDynamicRollback(cmd, r)
	}
	// The rollback watches the command's context, which an interrupt cancels.
	// A down body that kept executing after the signal would commit a
	// destructive change behind a status that says it was stopped.
	return r.migrator.MigrateDownToWithPreflight(
		cmd.Context(), r.targetVersion, r.preflightHook)
}

// rollbackInputs are the resolved values a rollback strategy reads, gathered so
// the command body does not have to spell one struct inside another.
type rollbackInputs struct {
	migrationsFS    fs.FS
	targetVersion   int64
	dirFormat       migrationfile.DirFormat
	atlasEnv        string
	connectTimeout  time.Duration
	migrationsTable string
	providerOptions []migrator.FSProviderOption
}

func buildRollbackExecution(r rollbackRun, hook migrator.PreMigrationHook) rollbackExecution {
	rollback := newDynamicRollback(r.resolved, r.opts, r.conn, r.inputs)
	rollback.statements = r.derived
	rollback.derivedFor = r.selected
	return rollbackExecution{
		plan:            r.resolved.plan,
		migrator:        r.migrator,
		preflightHook:   hook,
		dynamicRollback: rollback,
	}
}

// newDynamicRollback gathers what deriving a --plan rollback and applying it
// read.
func newDynamicRollback(resolved options, opts *options, conn *dbschema.DatabaseConnection, in rollbackInputs) dynamicRollback {
	return dynamicRollback{
		runtime:               resolved.featureRuntime,
		conn:                  conn,
		devURL:                resolved.shadowDB,
		migrationsFS:          in.migrationsFS,
		targetVersion:         in.targetVersion,
		dirFormat:             in.dirFormat,
		atlasEnv:              in.atlasEnv,
		connectTimeout:        in.connectTimeout,
		dryRun:                opts.dryRun,
		exclude:               resolved.planExclude,
		excludeSources:        resolved.planExcludeSources,
		allowUnmatchedExclude: resolved.allowUnmatchedExclude,
		providerOptions:       in.providerOptions,
		revisionsTable:        in.migrationsTable,
	}
}

// downPreflightHook builds the hook that runs inside the migration lock, just
// before any rollback SQL does.
func downPreflightHook(
	cmd *cobra.Command,
	opts *options,
	runtime *cliobs.Runtime,
	conn *dbschema.DatabaseConnection,
	dbURL string,
	resolved options,
	emit cliobs.Emitter,
) migrator.PreMigrationHook {
	return dbcli.LockedMigrationPreflightHook(opts.dryRun, preflight.Options{
		Direction:          preflight.DirectionDown,
		DatabaseURL:        dbURL,
		DisplayDatabaseURL: dburldisplay.Format(dbURL),
		Dialect:            conn.Info().Dialect,
		Command:            resolved.preDownHook,
		CommandOrigin:      resolved.preDownHookOrigin,
		PostgresDumpDir:    resolved.pgDumpTo,
		MySQLDumpDir:       resolved.mySQLDumpTo,
		WebhookURL:         resolved.webhook,
	}, emit, cliobs.NewOutputWriter(humanOutput(cmd, opts), runtime, "pre-flight output"))
}
