// Package migrationsdrift implements "ptah migrations drift", which reports
// whether a database still holds the schema its applied migration history
// produces, and applies nothing.
//
// The comparison is the one `ptah migrations up --check-drift` makes before it
// applies, [driftgate.Check], run here under the migration lock and reported
// in full: the version the database is at, the migrations still pending, each
// difference, the diff, and the statements that would bring the database back
// to its history.
package migrationsdrift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/catalog"
	"ptah.run/config/projectconfig"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/cliobs"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/cli/internal/migrateflags"
	"ptah.run/internal/cli/internal/migratelock"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/driftgate"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/featurejson"
	"ptah.run/internal/migrationintegrity"
	"ptah.run/internal/omissionnote"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

const (
	// ContractVersion is the version of the document `migrations drift
	// --json` prints. It rises when a field changes meaning or leaves; a new
	// field a reader may ignore is not a new version.
	ContractVersion = 1
	// Command is the command the document names.
	Command = "migrations drift"
)

const (
	dbURLFlag                = "db-url"
	migrationsFlag           = "migrations-dir"
	dirFormatFlag            = "dir-format"
	atlasEnvFlag             = "atlas-env"
	shadowDBFlag             = "shadow-db"
	excludeFlag              = "exclude"
	jsonFlag                 = "json"
	plainHTTPFlag            = "plain-http"
	migrationLockTimeoutFlag = migratelock.TimeoutFlag
)

// errDrifted is the exit status of a database that differs from its history.
// The report has said what differs, so the root prints nothing more.
var errDrifted = errors.New("the database differs from the schema its applied migrations produce")

type options struct {
	dbURL                string
	migrationsDir        string
	dirFormat            string
	atlasEnv             string
	shadowDB             string
	exclude              []string
	jsonOutput           bool
	plainHTTP            bool
	connectTimeout       string
	configPath           string
	envName              string
	migrationsSchema     string
	migrationsTable      string
	migrationsEngine     string
	revisionTableFormat  string
	migrationLockTimeout string
	migrationLockName    string
}

// NewMigrationsDriftCommand returns `ptah migrations drift`.
func NewMigrationsDriftCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "drift",
		Short: "Report drift between a database and its applied migration history",
		Long: `Report whether the database still holds the schema its applied migrations produce.

The command reads the revision table under the migration lock, replays the
applied migrations on the disposable --shadow-db, and compares the database with
the schema the replay produced. Pending migrations are listed and never read as
drift. The report names the version the database is at, the pending migrations,
every missing, extra and modified object, the diff, and the statements that
would bring the database back to its history. Nothing runs them, and nothing is
written to the database.

It is the comparison 'ptah migrations up --check-drift' makes before it applies.

Exit status: 0 when the database matches its history, 1 when it differs, and 2
when the comparison could not be made or decided.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out := &report{json: opts.jsonOutput, out: cmd.OutOrStdout()}
			err := runDrift(cmd, opts, out)
			if err == nil {
				return nil
			}
			if writeErr := out.writeFailure(err); writeErr != nil {
				return errors.Join(err, writeErr)
			}
			return err
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
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrationfile.DirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.StringVar(&opts.shadowDB, shadowDBFlag, "",
		"Disposable database the applied history is replayed on (required, or the project's dev_url); "+
			"it must be empty and distinct from the target, and it is emptied again afterwards. "+
			"A docker:// URL starts a container for the run")
	flags.StringArrayVar(&opts.exclude, excludeFlag, nil,
		"Schema objects the comparison leaves out, as `schema diff --exclude` selects them (repeatable)")
	flags.BoolVar(&opts.jsonOutput, jsonFlag, false,
		"Print the report as one JSON document, or the failure when no report could be made")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	flags.StringVar(&opts.migrationLockTimeout, migrationLockTimeoutFlag, "",
		"Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m; "+
			"refused against a dialect that takes no such lock")
	migratelock.RegisterNameFlag(flags, &opts.migrationLockName)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterMigrationsEngineFlag(flags, &opts.migrationsEngine)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
}

// resolveProjectOptions merges the project config under the flags.
func resolveProjectOptions(cmd *cobra.Command, opts options, projectCfg projectconfig.Config) options {
	effective := func(flagName, flagValue string, field projectconfig.StringField) string {
		return dbcli.EffectiveString(cmd, flagName, flagValue, projectCfg.StringValue(field))
	}
	opts.dbURL = effective(dbURLFlag, opts.dbURL, projectconfig.StringDatabaseURL)
	opts.migrationsDir = effective(migrationsFlag, opts.migrationsDir, projectconfig.StringMigrationDir)
	opts.shadowDB = effective(shadowDBFlag, opts.shadowDB, projectconfig.StringDevURL)
	opts.dirFormat = effective(dirFormatFlag, opts.dirFormat, projectconfig.StringMigrationFormat)
	opts.atlasEnv = effective(atlasEnvFlag, opts.atlasEnv, projectconfig.StringEnvName)
	opts.migrationsSchema = effective(dbcli.MigrationsSchemaFlagName, opts.migrationsSchema, projectconfig.StringMigrationRevisionsSchema)
	opts.migrationsTable = effective(dbcli.MigrationsTableFlagName, opts.migrationsTable, projectconfig.StringMigrationRevisionsTable)
	opts.revisionTableFormat = effective(dbcli.RevisionTableFormatFlagName, opts.revisionTableFormat, projectconfig.StringMigrationRevisionFormat)
	opts.connectTimeout = effective(dbcli.ConnectTimeoutFlagName, opts.connectTimeout, projectconfig.StringMigrationConnectTimeout)
	opts.migrationLockTimeout = effective(migrationLockTimeoutFlag, opts.migrationLockTimeout, projectconfig.StringMigrationMigrationLockTimeout)
	opts.migrationLockName = effective(migratelock.NameFlag, opts.migrationLockName, projectconfig.StringMigrationMigrationLockName)
	return opts
}

// settings are the resolved values the run is made with, parsed before the
// command opens anything.
type settings struct {
	dirFormat            migrationfile.DirFormat
	revisionFormat       migrator.RevisionTableFormat
	connectTimeout       time.Duration
	migrationLockTimeout time.Duration
	lockName             migratelock.Name
	lockRequests         migratelock.Requests
}

func parseSettings(cmd *cobra.Command, opts options, projectCfg projectconfig.Config) (settings, error) {
	if opts.dbURL == "" {
		return settings{}, dbcli.MissingProjectValue(projectCfg, errors.New("database URL is required"))
	}
	if opts.migrationsDir == "" {
		return settings{}, dbcli.MissingProjectValue(projectCfg, errors.New("migrations directory is required"))
	}
	if strings.TrimSpace(opts.shadowDB) == "" {
		return settings{}, dbcli.MissingProjectValue(projectCfg, fmt.Errorf(
			"--%s is required: the applied history is replayed on a disposable shadow database", shadowDBFlag))
	}
	dirFormat, err := migrationfile.ParseDirFormat(opts.dirFormat)
	if err != nil {
		return settings{}, err
	}
	revisionFormat, err := migrateflags.ParseRevisionTableFormat(opts.revisionTableFormat)
	if err != nil {
		return settings{}, err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return settings{}, err
	}
	lockTimeout, err := migrateflags.ParseMigrationLockTimeout(opts.migrationLockTimeout)
	if err != nil {
		return settings{}, err
	}
	requests := migratelock.NewRequests(
		cmd,
		opts.dbURL,
		projectCfg.StringValue(projectconfig.StringMigrationMigrationLockTimeout).Present,
		projectCfg.StringValue(projectconfig.StringMigrationMigrationLockName).Present,
	)
	if err := requests.DecideFromURL(); err != nil {
		return settings{}, err
	}
	lockName, err := migratelock.ResolveName(requests.Name, opts.migrationLockName)
	if err != nil {
		return settings{}, err
	}
	return settings{
		dirFormat:            dirFormat,
		revisionFormat:       revisionFormat,
		connectTimeout:       connectTimeout,
		migrationLockTimeout: lockTimeout,
		lockName:             lockName,
		lockRequests:         requests,
	}, nil
}

func runDrift(cmd *cobra.Command, raw options, out *report) error {
	ctx := cmd.Context()
	integrityPolicy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, raw.configPath)
	if err != nil {
		return err
	}
	opts := resolveProjectOptions(cmd, raw, projectCfg)
	set, err := parseSettings(cmd, opts, projectCfg)
	if err != nil {
		return err
	}

	// Warnings and errors only: every Info record the migrator writes narrates
	// an apply, and with writes suppressed an empty database reads as "[DRY
	// RUN] Would initialize migrations metadata", which is not what a report
	// does.
	observability, err := cliobs.Start(context.Background(), cliobs.Options{
		Command: "migrations.drift", LogLevel: "warn", LogWriter: cmd.ErrOrStderr(),
	})
	if err != nil {
		return err
	}
	defer shutdownObservability(observability)
	observability.ShareWithRun(ctx)

	source, err := migrationsource.Resolve(ctx, opts.migrationsDir, migrationsource.Options{
		DirFormat: set.dirFormat,
		PlainHTTP: opts.plainHTTP,
	})
	if err != nil {
		return err
	}
	set.dirFormat = source.DirFormat
	// The replay executes the directory's SQL on the shadow database, so the
	// directory passes the integrity gate every verb that executes it passes.
	// Its notices go to standard error, where they cannot corrupt the report.
	notices := cliobs.NewEmitter(cmd.ErrOrStderr(), observability)
	if err := migrationsource.Verify(ctx, cmd.ErrOrStderr(), notices, observability, source, set.dirFormat, integrityPolicy,
		migrationsource.VerifyOptions{}); err != nil {
		return err
	}

	runtime, err := engineruntime.Resolve(ctx, engineruntime.DatabaseURL(opts.dbURL))
	if err != nil {
		return err
	}
	exclude, excludeSources, err := dbcli.EffectiveExclude(cmd, excludeFlag, opts.exclude, projectCfg, runtime.Selections())
	if err != nil {
		return err
	}
	// Before the migration lock, so the lock is not held while a container
	// starts.
	shadowURL, releaseShadow, err := devdocker.Resolve(ctx, opts.shadowDB, devdocker.Options{})
	if err != nil {
		return err
	}
	defer releaseShadow()

	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, set.connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", errorkind.TargetConnection(err))
	}
	defer dbschema.CloseAndWarn(conn)
	if err := set.lockRequests.DecideSession(conn); err != nil {
		return err
	}

	mig, err := migrator.NewFSMigrator(
		conn,
		source.FileSystem,
		migrator.WithMigrationDirFormat(set.dirFormat),
		migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: opts.atlasEnv}),
		migrator.WithRepeatableMigrations(),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(opts.migrationsSchema, opts.migrationsTable).
		WithMigrationsEngine(opts.migrationsEngine).
		WithRevisionTableFormat(set.revisionFormat).
		WithMigrationLockTimeout(set.migrationLockTimeout).
		WithMigrationLockName(set.lockName.Value).
		WithLogger(observability.Logger()).
		WithObserver(observability.Observer())

	check := driftgate.New(driftgate.Options{
		ShadowURL: shadowURL,
		Selection: atlasschema.LiveSelection{
			Exclude: exclude, ExcludeSources: excludeSources, Diagnostics: cmd.ErrOrStderr(),
		},
		ConnectTimeout: set.connectTimeout,
		Runtime:        runtime,
		Conn:           conn,
		History:        mig,
		MetadataTables: revisiontable.Configured(string(set.revisionFormat), opts.migrationsTable),
		OnDrift:        driftgate.Continue,
	})

	var (
		status *migrator.MigrationStatus
		found  driftgate.Result
		// checkErr is what the check said about the target: a drift or an
		// undecided comparison. err below is a run that made no report.
		checkErr error
	)
	err = mig.HoldMigrationLock(ctx, "drift", func(ctx context.Context) error {
		// The lock is taken by now. A report writes nothing to the target, so
		// the reads under it inspect the revision table rather than create it,
		// and an absent table reads as no migration applied.
		restore := dbcli.SuppressSchemaWrites(conn)
		defer restore()
		var statusErr error
		if status, statusErr = mig.GetMigrationStatus(ctx); statusErr != nil {
			return fmt.Errorf("error getting migration status: %w", statusErr)
		}
		found, checkErr = check.Run(ctx)
		return nil
	})
	if err != nil {
		return err
	}

	document := newDocument(mig, conn, status, found)
	if found.Check.Outcome == migrator.DriftDrifted {
		document.reconcile(ctx, cmd.ErrOrStderr(), runtime, conn.Info(), found.Diff)
	}
	if err := out.write(ctx, document, runtime.Codecs()); err != nil {
		return err
	}
	return exitStatus(found.Check, checkErr)
}

// exitStatus is the run's answer once the report is written: nothing for a
// match, 1 for drift, which the report has described, and an error the root
// prints and exits 2 for when the comparison could not decide. The gate's own
// error says no pending migration ran, which is true of every report and says
// nothing here.
func exitStatus(check migrator.DriftCheck, checkErr error) error {
	if checkErr == nil {
		return nil
	}
	if _, drifted := errors.AsType[*driftgate.DriftError](checkErr); drifted {
		return exitcode.New(1, errDrifted)
	}
	return &UndecidedError{Check: check, Err: checkErr}
}

// UndecidedError is a report whose comparison could not decide whether the
// database matches its applied history. The report says where and why.
type UndecidedError struct {
	Check migrator.DriftCheck
	Err   error
}

func (e *UndecidedError) Error() string {
	return "could not compare the database with its applied history: " + e.Check.Message
}

func (e *UndecidedError) Unwrap() error { return e.Err }

// Document is what `migrations drift --json` prints when it made a
// comparison, whatever the comparison found.
type Document struct {
	ContractVersion int    `json:"contract_version"`
	Command         string `json:"command"`
	Dialect         string `json:"dialect"`
	// CurrentVersion is the version the database last applied, as a person
	// names it. It is absent when the revision table records none.
	CurrentVersion string `json:"current_version,omitempty"`
	// Pending are the versions of the migrations the directory holds and the
	// database has not applied, in the order a run applies them. They are not
	// part of the comparison.
	Pending []string `json:"pending,omitempty"`
	// Check is the comparison with the replayed history, in the form the run
	// document of `migrations up --check-drift` reports it.
	Check migrator.DriftCheck `json:"check"`
	// Diff is the change that brings the database back to its history. It is
	// present only when the check found drift.
	Diff *difftypes.SchemaDiff `json:"diff,omitempty"`
	// Statements are the statements that make Diff, for a person to review.
	// Nothing runs them.
	Statements []string `json:"statements,omitempty"`
	// PlanError says why the statements could not be planned. The drift
	// stands, and Diff still describes it.
	PlanError string `json:"plan_error,omitempty"`
	// script is the plan as one script, delimiters included, which the text
	// report prints.
	script string
}

// reconcile plans the statements that make the document's diff. A plan that
// fails leaves the drift reported and says why there are no statements. What
// the plan leaves out is reported on diagnostics, as `schema compare` reports
// it.
func (d *Document) reconcile(
	ctx context.Context, diagnostics io.Writer, runtime *engine.Runtime, info catalog.ServerInfo, diff *difftypes.SchemaDiff,
) {
	plan, err := driftgate.Reconcile(ctx, runtime, info, diff)
	if err == nil {
		d.Statements, err = plan.Statements()
	}
	switch {
	case err != nil:
	case plan.Result.ExecutableUnits:
		// A provider's execution units are exact fragments, and joined as one
		// script two of them can share a line.
		d.script = strings.Join(d.Statements, "\n")
	default:
		d.script, err = plan.SQL()
	}
	if err != nil {
		d.Statements, d.script, d.PlanError = nil, "", err.Error()
		return
	}
	omissionnote.Report(ctx, diagnostics, plan.Result.Omissions)
}

func newDocument(mig *migrator.Migrator, conn *dbschema.DatabaseConnection, status *migrator.MigrationStatus, found driftgate.Result) Document {
	document := Document{
		ContractVersion: ContractVersion,
		Command:         Command,
		Dialect:         conn.Info().Dialect,
		Pending:         mig.VersionLabels(status.PendingMigrations),
		Check:           found.Check,
		Diff:            found.Diff,
	}
	if len(status.AppliedMigrations) > 0 {
		document.CurrentVersion = mig.VersionLabel(status.CurrentVersion)
	}
	return document
}

// NewFailureDocument is what `migrations drift --json` prints when it fails
// with err before it made a comparison: the contract version, the command and
// the error. A document carrying error is never a report.
func NewFailureDocument(err error) errorkind.Failure {
	return errorkind.NewFailure(ContractVersion, Command, err)
}

// report is where the run's one document goes: the report when the run made
// a comparison, and the failure otherwise. A failure after the report, such as
// an undecided comparison, adds no second document.
type report struct {
	json    bool
	out     io.Writer
	written bool
}

func (r *report) write(ctx context.Context, document Document, codecs schemaext.Registry) error {
	r.written = true
	if !r.json {
		return writeText(r.out, document)
	}
	encoded, err := featurejson.MarshalIndent(ctx, codecs, schemaext.Desired, document, "", "  ")
	if err != nil {
		return fmt.Errorf("write the JSON result: %w", err)
	}
	_, err = r.out.Write(append(encoded, '\n'))
	return err
}

func (r *report) writeFailure(err error) error {
	if !r.json || r.written {
		return nil
	}
	r.written = true
	encoder := json.NewEncoder(r.out)
	encoder.SetIndent("", "  ")
	if encodeErr := encoder.Encode(NewFailureDocument(err)); encodeErr != nil {
		return fmt.Errorf("write the JSON result: %w", encodeErr)
	}
	return nil
}

func writeText(w io.Writer, document Document) error {
	var b strings.Builder
	fmt.Fprintf(&b, "Dialect: %s\n", document.Dialect)
	current := document.CurrentVersion
	if current == "" {
		current = "none applied"
	}
	fmt.Fprintf(&b, "Current version: %s\n", current)
	fmt.Fprintf(&b, "Pending migrations: %d\n", len(document.Pending))
	for _, version := range document.Pending {
		fmt.Fprintf(&b, "  - %s\n", version)
	}
	b.WriteString("\n")
	check := document.Check
	switch check.Outcome {
	case migrator.DriftClean:
		fmt.Fprintf(&b, "No drift: the database matches the schema its %s.\n", driftgate.AppliedMigrations(check))
	case migrator.DriftDrifted:
		fmt.Fprintf(&b, "Drift: the database differs from the schema its %s:\n", driftgate.AppliedMigrations(check))
		for _, finding := range check.Findings {
			fmt.Fprintf(&b, "  - %s\n", finding.Message)
		}
		writeStatements(&b, document)
	default:
		fmt.Fprintf(&b, "Could not compare the database with its applied history: %s\n", check.Message)
		for _, finding := range check.Findings {
			fmt.Fprintf(&b, "  - %s\n", finding.Message)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func writeStatements(b *strings.Builder, document Document) {
	switch {
	case document.PlanError != "":
		fmt.Fprintf(b, "\nThe statements that would bring the database back to its history could not be planned: %s\n",
			document.PlanError)
	case document.script != "":
		b.WriteString("\nStatements that would bring the database back to its history (review them; nothing ran):\n")
		b.WriteString(document.script)
		if !strings.HasSuffix(document.script, "\n") {
			b.WriteString("\n")
		}
	}
}

func shutdownObservability(runtime *cliobs.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		runtime.Logger().Warn("failed to shut down observability", "error", err)
	}
}
