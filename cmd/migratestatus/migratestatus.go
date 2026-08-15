// Package migratestatus implements "ptah migrations status", which reports
// applied and pending migrations for a live database and migrations
// directory.
package migratestatus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cliobs"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	dbURLFlag      = "db-url"
	migrationsFlag = "migrations-dir"
	dirFormatFlag  = "dir-format"
	atlasEnvFlag   = "atlas-env"
	verboseFlag    = "verbose"
	jsonFlag       = "json"
	exitCodeFlag   = "exit-code"
	plainHTTPFlag  = "plain-http"
	verifySumFlag  = "verify-sum"
)

type options struct {
	dbURL               string
	migrationsDir       string
	dirFormat           string
	atlasEnv            string
	verbose             bool
	jsonOutput          bool
	exitOnPending       bool
	plainHTTP           bool
	verifySum           bool
	connectTimeout      string
	configPath          string
	envName             string
	migrationsSchema    string
	migrationsTable     string
	revisionTableFormat string
	logFormat           string
	logLevel            string
	metricsAddr         string
}

func NewMigrateStatusCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show current migration status",
		Long: `Show the current migration status of the database.

This command displays information about:
- Current database schema version
- Total number of available migrations
- Number of pending migrations
- List of pending migration versions

This is useful for checking the state of your database before running
migrations or for debugging migration issues.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return migrateStatusCommand(cmd, &opts)
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
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.BoolVar(&opts.verbose, verboseFlag, false, "Enable verbose output with detailed migration information")
	flags.BoolVar(&opts.jsonOutput, jsonFlag, false, "Output status in JSON format")
	flags.BoolVar(&opts.exitOnPending, exitCodeFlag, false, "Exit with 1 when pending migrations are available")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	flags.BoolVar(
		&opts.verifySum,
		verifySumFlag,
		false,
		migrationsource.VerifySumUsage(
			"Verify the migration directory against its ptah.sum or atlas.sum before reporting, "+
				"and fail when it carries neither. status reads the directory and executes none "+
				"of its SQL, so it runs no gate without this flag",
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

func migrateStatusCommand(cmd *cobra.Command, opts *options) error {
	dbURL := opts.dbURL
	migrationsDir := opts.migrationsDir
	dirFormatValue := opts.dirFormat
	atlasEnv := opts.atlasEnv
	migrationsSchema := opts.migrationsSchema
	migrationsTable := opts.migrationsTable
	revisionFormatValue := opts.revisionTableFormat

	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return err
	}
	dbURL = dbcli.EffectiveString(
		cmd,
		dbURLFlag,
		dbURL,
		projectCfg.StringValue(projectconfig.StringDatabaseURL),
	)
	migrationsDir = dbcli.EffectiveString(
		cmd,
		migrationsFlag,
		migrationsDir,
		projectCfg.StringValue(projectconfig.StringMigrationDir),
	)
	dirFormatValue = dbcli.EffectiveString(
		cmd,
		dirFormatFlag,
		dirFormatValue,
		projectCfg.StringValue(projectconfig.StringMigrationFormat),
	)
	atlasEnv = dbcli.EffectiveString(
		cmd,
		atlasEnvFlag,
		atlasEnv,
		projectCfg.StringValue(projectconfig.StringEnvName),
	)
	migrationsSchema = dbcli.EffectiveString(
		cmd,
		dbcli.MigrationsSchemaFlagName,
		migrationsSchema,
		projectCfg.StringValue(projectconfig.StringMigrationRevisionsSchema),
	)
	migrationsTable = dbcli.EffectiveString(
		cmd,
		dbcli.MigrationsTableFlagName,
		migrationsTable,
		projectCfg.StringValue(projectconfig.StringMigrationRevisionsTable),
	)
	revisionFormatValue = dbcli.EffectiveString(
		cmd,
		dbcli.RevisionTableFormatFlagName,
		revisionFormatValue,
		projectCfg.StringValue(projectconfig.StringMigrationRevisionFormat),
	)
	connectTimeoutValue := dbcli.EffectiveString(
		cmd,
		dbcli.ConnectTimeoutFlagName,
		opts.connectTimeout,
		projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout),
	)

	logWriter := cmd.ErrOrStderr()
	if opts.logFormat == "json" && !opts.jsonOutput {
		logWriter = cmd.OutOrStdout()
	}
	runtime, err := cliobs.Start(context.Background(), cliobs.Options{
		Command:     "migrations.status",
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
	dirFormat = source.DirFormat

	// The gate runs ONLY when the operator asked for it.
	//
	// status is deliberately outside the always-on integrity class that
	// stokaro/ptah#1450 gave every verb which EXECUTES SQL from the directory.
	// status executes none of it, and it is the verb an operator reaches for
	// while diagnosing a directory that has drifted — a default gate here would
	// refuse to describe the very thing being investigated, and would turn a
	// long-standing exit 0 into an exit 2 for every project whose directory has
	// drifted for a reason they already know about.
	//
	// --verify-sum is the opt-in that makes the report itself an integrity
	// claim, which is what stokaro/ptah#928 item 4 asked for. A CI job reading
	// status to decide whether to deploy wants the directory checked, and wants
	// a directory carrying no sum at all to fail rather than report cleanly.
	// The provenance qualifier rides along, so status cannot claim more for a
	// movable OCI tag than `up` does.
	//
	// The warning goes to standard error under --json, because a `Warning:`
	// line on standard output would corrupt the document a caller is parsing.
	//
	// The escape-hatch policy is resolved here rather than at the top of the
	// command for the same reason the gate itself is conditional: status is
	// outside the always-on integrity class, so a run that was never going to
	// check anything has no configuration to refuse. It still resolves BEFORE
	// the one gate it does run, so a malformed value refuses `--verify-sum`
	// whatever state the directory is in — and status runs a single gate, so
	// there is no second one that could observe a different value.
	if opts.verifySum {
		integrityPolicy, err := migrationintegrity.Resolve()
		if err != nil {
			return err
		}
		if err := migrationsource.Verify(
			cmd.ErrOrStderr(), integrityEmitter(cmd, runtime, opts), runtime, source, dirFormat, integrityPolicy,
			migrationsource.VerifyOptions{RequireSum: true, Verbose: opts.verbose},
		); err != nil {
			return err
		}
	}

	revisionFormat, err := migrator.ParseRevisionTableFormat(revisionFormatValue)
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

	mig, err := migrator.NewFSMigrator(
		conn,
		migrationsFS,
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: atlasEnv}),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(migrationsSchema, migrationsTable).
		WithRevisionTableFormat(revisionFormat).
		WithLogger(runtime.Logger()).
		WithObserver(runtime.Observer())

	// Get migration status
	status, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting migration status: %w", err)
	}

	if opts.jsonOutput {
		if err := outputJSON(cmd.OutOrStdout(), status); err != nil {
			return err
		}
	} else if err := outputHuman(emit, status, conn, opts.verbose); err != nil {
		return err
	}

	if opts.exitOnPending {
		return pendingMigrationsExitCode(status)
	}
	return nil
}

// integrityEmitter selects where the integrity gate's operator-facing output
// goes.
//
// Everything else this verb prints for a human is suppressed under --json by
// the output branch at the end of the run, but the gate speaks BEFORE that
// branch is reached and cannot be routed the same way. Writing its provenance
// warning to standard output under --json would put a `Warning:` line in front
// of the document a caller is parsing, which is a worse failure than the
// warning is a help: the caller sees a JSON parse error rather than the
// unpinned tag the warning is about.
func integrityEmitter(cmd *cobra.Command, runtime *cliobs.Runtime, opts *options) cliobs.Emitter {
	if opts.jsonOutput {
		return cliobs.NewEmitter(cmd.ErrOrStderr(), runtime)
	}
	return cliobs.NewEmitter(cmd.OutOrStdout(), runtime)
}

func shutdownObservability(runtime *cliobs.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		runtime.Logger().Warn("failed to shut down observability", "error", err)
	}
}

func pendingMigrationsExitCode(status *migrator.MigrationStatus) error {
	if status.HasPendingChanges {
		return exitcode.New(1, errors.New("pending migrations available"))
	}
	return nil
}

func outputJSON(w io.Writer, status *migrator.MigrationStatus) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(status)
}

func outputHuman(emit cliobs.Emitter, status *migrator.MigrationStatus, conn *dbschema.DatabaseConnection, verbose bool) error { //revive:disable-line:flag-parameter output mode is caller-owned command state
	emit.Println("=== MIGRATION STATUS ===")
	emit.Printf("Database: %s\n", dbschema.FormatDatabaseURL("***"))
	emit.Printf("Dialect: %s\n", conn.Info().Dialect)
	emit.Printf("Schema: %s\n", conn.Info().Schema)
	emit.Println()

	emit.Printf("Current Version: %d\n", status.CurrentVersion)
	emit.Printf("Total Migrations: %d\n", status.TotalMigrations)
	emit.Printf("Applied Migrations: %d\n", len(status.AppliedMigrations))
	emit.Printf("Pending Migrations: %d\n", len(status.PendingMigrations))
	emit.Printf("Out-of-order Migrations: %d\n", len(status.OutOfOrderMigrations))

	if status.DirtyRevision != nil {
		emit.Println("Status: ❌ Dirty migration state detected")
		emit.Printf(
			"Dirty Migration: version=%d state=%s direction=%s applied=%d/%d\n",
			status.DirtyRevision.Version,
			status.DirtyRevision.State,
			status.DirtyRevision.Direction,
			status.DirtyRevision.Applied,
			status.DirtyRevision.Total,
		)
		if status.DirtyRevision.Error != "" {
			emit.Printf("Error: %s\n", status.DirtyRevision.Error)
		}
		if status.DirtyRevision.ErrorStatement != "" {
			emit.Printf("Error Statement: %s\n", status.DirtyRevision.ErrorStatement)
		}
		emit.Printf("\n%s\n", dirtyRevisionRecoveryHint(status.DirtyRevision))
		return nil
	}

	if status.HasPendingChanges {
		emit.Println("Status: ⚠️  Pending migrations available")

		if verbose && len(status.PendingMigrations) > 0 {
			emit.Println("\nPending migration versions:")
			for _, version := range status.PendingMigrations {
				emit.Printf("  - %d\n", version)
			}
		}
		if verbose && len(status.OutOfOrderMigrations) > 0 {
			emit.Println("\nOut-of-order migration versions:")
			for _, version := range status.OutOfOrderMigrations {
				emit.Printf("  - %d\n", version)
			}
		}

		emit.Println("\nRun 'ptah migrations up' to apply pending migrations.")
	} else {
		emit.Println("Status: ✅ Database is up to date")
	}

	if verbose {
		emit.Println("\n=== DETAILED INFORMATION ===")

		if status.TotalMigrations == 0 {
			emit.Println("No migrations found in the migrations directory.")
		} else {
			emit.Printf("Applied migrations: %d\n", len(status.AppliedMigrations))

			if len(status.PendingMigrations) > 0 {
				emit.Printf("Next migration to apply: %d\n", status.PendingMigrations[0])
			}
		}
	}

	return nil
}

// dirtyRevisionRecoveryHint names the command that ends this particular dirty
// state. A rollback that stopped partway is not repaired by recording the
// migration applied -- that would sign it off over a schema whose objects the
// rollback already dropped -- so it points at the resume that finishes the
// rollback instead, at the statement the revision says comes next.
func dirtyRevisionRecoveryHint(revision *migrator.MigrationRevision) string {
	if revision.Direction != migrator.MigrationDirectionDown {
		return "Run 'ptah migrations repair --version <version>' after fixing the database state."
	}
	return fmt.Sprintf(
		"This rollback stopped partway. Run 'ptah migrations repair --version %d --resume-from %d' "+
			"to run the remaining down statements and remove the revision.",
		revision.Version,
		revision.Applied+1,
	)
}
