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
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/cli/internal/migrateflags"
	"ptah.run/internal/cli/internal/migratelock"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/dburldisplay"
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
	// envPrefix is the prefix of the variables the root binds to the flags.
	envPrefix = "PTAH"
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
would bring the database back to its history. Nothing runs them. The revision
state is only read; on Oracle the migration lock creates its table ptah_locks
beside the revision table when it is absent and adds a row for the lock name,
which needs CREATE TABLE (CREATE ANY TABLE in a schema the account does not
own), or a table a DBA created with SELECT and INSERT granted.

An applied migration whose file changed since it ran cannot be replayed, so it
is reported as a comparison that could not decide, as 'migrations up' refuses
it. The exclusions are the project's exclude list, replaced by an atlas.hcl
drift block's own list when it sets one, and --exclude.

It is the comparison 'ptah migrations up --check-drift' makes before it applies.

An atlas.hcl env with for_each names several targets. The report runs over
each in the order the env expands, names each before its report, goes on past
a target that drifted or failed, and exits with the most serious status of any
target; --json prints one document that lists them. Each target is the
database its instance's url names, so --db-url, which would replace every one
of them, is refused over such an env.

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
		"Schema objects the comparison leaves out, written as schema diff --exclude takes them (repeatable)")
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
	integrityPolicy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	configs, err := dbcli.LoadProjectConfigs(cmd, raw.configPath)
	if err != nil {
		return err
	}
	if err := refuseOneDatabaseForEveryTarget(cmd, configs); err != nil {
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
	observability.ShareWithRun(cmd.Context())

	run := driftRun{cmd: cmd, raw: raw, integrityPolicy: integrityPolicy, observability: observability}
	if collection(configs) {
		return run.targets(configs, out)
	}
	// The project binds a migration directory it rendered, such as a
	// data.template_dir, to the context the target runs under, so the context
	// is read after the load.
	ctx := dbcli.WithProjectMigrationDirectory(cmd.Context(), configs[0])
	result, err := run.target(ctx, configs[0])
	if err != nil {
		return err
	}
	if err := out.write(ctx, result.document, result.codecs); err != nil {
		return err
	}
	return result.status
}

// refuseOneDatabaseForEveryTarget refuses a run over the targets of an env
// when --db-url is set, by the flag or by the variable bound to it. Each
// target is the database its instance's url names, and a set --db-url
// replaces every one of those urls, so the run would read that one database
// once per target and report it as each of them, drifted and unreachable
// tenants included. A single env takes --db-url as it always does.
func refuseOneDatabaseForEveryTarget(cmd *cobra.Command, configs []projectconfig.Config) error {
	flag := cmd.Flags().Lookup(dbURLFlag)
	if !collection(configs) || flag == nil || !flag.Changed {
		return nil
	}
	setBy := "--" + dbURLFlag
	if variable, bound := cmdflags.EnvBindingName(envPrefix, flag); bound {
		setBy += " or " + variable
	}
	return errors.New(oneDatabaseRefusal(setBy, flag.Value.String(), configs[0].EnvName, len(configs)))
}

// oneDatabaseRefusal words the refusal of the database url value, set by
// setBy, over the targets of env. An empty value names no database and is
// still refused, because it is set; one target is named as one.
func oneDatabaseRefusal(setBy, value, env string, targets int) string {
	every, named, each := "every target", fmt.Sprintf("names %d targets with a url each", targets), "each target"
	if targets == 1 {
		every, named, each = "the target", "names one target with a url of its own", "that target"
	}
	set := setBy + " sets one database for " + every + ","
	if value == "" {
		set = setBy + " is set, though empty,"
	}
	return fmt.Sprintf("%s and env %q %s; leave both unset to report %s", set, env, named, each)
}

// collection reports whether configs are the targets of an env that names
// several: more than one config, or an instance of a for_each env. One
// instance is a collection too, so a script reading the report keeps the
// shape it reads when the for_each shrinks to one element.
func collection(configs []projectconfig.Config) bool {
	return len(configs) > 1 || configs[0].EnvInstance > 0
}

// driftRun is what every target of one run shares.
type driftRun struct {
	cmd             *cobra.Command
	raw             options
	integrityPolicy migrationintegrity.Policy
	observability   *cliobs.Runtime
}

// targetResult is the report a run over one target made.
type targetResult struct {
	// dbURL is the database the target names once the flags and the project
	// resolve it. It is set when the run failed after resolving it, too.
	dbURL    string
	document Document
	codecs   schemaext.Registry
	// status is what the comparison found, as [exitStatus] gives it.
	status error
}

// target runs the report over one project config, under ctx, the context
// that config's migration directory is bound to. It returns an error when it
// made no comparison, and the result names the target's database either way.
func (r driftRun) target(ctx context.Context, projectCfg projectconfig.Config) (targetResult, error) {
	cmd := r.cmd
	opts := resolveProjectOptions(cmd, r.raw, projectCfg)
	result := targetResult{dbURL: opts.dbURL}
	set, err := parseSettings(cmd, opts, projectCfg)
	if err != nil {
		return result, err
	}

	source, err := migrationsource.Resolve(ctx, opts.migrationsDir, migrationsource.Options{
		DirFormat: set.dirFormat,
		PlainHTTP: opts.plainHTTP,
	})
	if err != nil {
		return result, err
	}
	set.dirFormat = source.DirFormat
	// The replay executes the directory's SQL on the shadow database, so the
	// directory passes the integrity gate every verb that executes it passes.
	// Its notices go to standard error, where they cannot corrupt the report.
	notices := cliobs.NewEmitter(cmd.ErrOrStderr(), r.observability)
	if err := migrationsource.Verify(ctx, cmd.ErrOrStderr(), notices, r.observability, source, set.dirFormat, r.integrityPolicy,
		migrationsource.VerifyOptions{}); err != nil {
		return result, err
	}

	runtime, err := engineruntime.Resolve(ctx, engineruntime.DatabaseURL(opts.dbURL))
	if err != nil {
		return result, err
	}
	// The exclusions `migrations up --check-drift` makes, an atlas.hcl drift
	// block's included, so the report and the check leave out the same objects.
	exclude, excludeSources, err := dbcli.EffectiveDriftExclude(cmd, excludeFlag, opts.exclude, projectCfg, runtime.Selections())
	if err != nil {
		return result, err
	}
	// Before the migration lock, so the lock is not held while a container
	// starts.
	shadowURL, releaseShadow, err := devdocker.Resolve(ctx, opts.shadowDB, devdocker.Options{})
	if err != nil {
		return result, err
	}
	defer releaseShadow()

	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, set.connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return result, fmt.Errorf("error connecting to database: %w", errorkind.TargetConnection(err))
	}
	defer dbschema.CloseAndWarn(conn)
	if err := set.lockRequests.DecideSession(conn); err != nil {
		return result, err
	}

	mig, err := migrator.NewFSMigrator(
		conn,
		source.FileSystem,
		migrator.WithMigrationDirFormat(set.dirFormat),
		migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: opts.atlasEnv}),
		migrator.WithRepeatableMigrations(),
	)
	if err != nil {
		return result, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(opts.migrationsSchema, opts.migrationsTable).
		WithMigrationsEngine(opts.migrationsEngine).
		WithRevisionTableFormat(set.revisionFormat).
		WithMigrationLockTimeout(set.migrationLockTimeout).
		WithMigrationLockName(set.lockName.Value).
		WithLogger(r.observability.Logger()).
		WithObserver(r.observability.Observer())

	metadataTables := revisiontable.Configured(string(set.revisionFormat), opts.migrationsTable)
	check := driftgate.New(driftgate.Options{
		ShadowURL: shadowURL,
		Selection: atlasschema.LiveSelection{
			Exclude: exclude, ExcludeSources: excludeSources, Diagnostics: cmd.ErrOrStderr(),
		},
		ConnectTimeout: set.connectTimeout,
		Runtime:        runtime,
		Conn:           conn,
		History:        mig,
		MetadataTables: metadataTables,
		OnDrift:        driftgate.Continue,
	})

	var (
		status *migrator.MigrationStatus
		found  driftgate.Result
		// checkErr is what the check said about the target: a drift or an
		// undecided comparison. err below is a run that made no report.
		checkErr error
		// other is the revision table of the other format, when the one the
		// run read records nothing and that one records a history.
		other *otherHistory
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
		if len(status.AppliedMigrations) == 0 && found.Check.Outcome == migrator.DriftDrifted {
			other = readOtherFormat(ctx, mig, set.revisionFormat)
		}
		return nil
	})
	if err != nil {
		return result, err
	}

	result.document = newDocument(mig, conn, status, found, other)
	if found.Check.Outcome == migrator.DriftDrifted {
		result.document.reconcile(ctx, cmd.ErrOrStderr(), runtime, conn.Info(), found.Diff)
	}
	result.codecs = runtime.Codecs()
	result.status = exitStatus(found.Check, checkErr)
	return result, nil
}

// exitStatus is the run's answer once the report is written: nothing for a
// match, 1 for drift, and 2 when the comparison could not decide. The report
// has said which, and why, so the root prints neither again. The gate's own
// error says no pending migration ran, which is true of every report and says
// nothing here.
func exitStatus(check migrator.DriftCheck, checkErr error) error {
	if checkErr == nil {
		return nil
	}
	if _, drifted := errors.AsType[*driftgate.DriftError](checkErr); drifted {
		return exitcode.New(1, errDrifted)
	}
	return exitcode.New(2, &UndecidedError{Check: check, Err: checkErr})
}

// TargetsDocument is what `migrations drift --json` prints for an env that
// expands to targets, such as an atlas.hcl env with for_each: one entry per
// target, in the order the env expands. A for_each env that selected one
// target prints this shape too.
type TargetsDocument struct {
	ContractVersion int    `json:"contract_version"`
	Command         string `json:"command"`
	// Env is the env the run selected.
	Env     string   `json:"env"`
	Targets []Target `json:"targets"`
}

// Target is the report over one target of a [TargetsDocument].
type Target struct {
	// Instance is the target's position, from 1, in its env's for_each
	// expansion, and 0 for an env block without for_each.
	Instance int `json:"instance"`
	// URL is the target database, with its password and secret parameters
	// hidden. It is empty when the target names none.
	URL string `json:"url"`
	// ExitCode is the status a run over this target alone exits with: 0 for
	// a match, 1 for drift, and 2 when the comparison could not be made or
	// decided.
	ExitCode int `json:"exit_code"`
	// Report is the [Document] a run over this target alone prints. It is
	// absent when the run made no comparison.
	Report json.RawMessage `json:"report,omitempty"`
	// Error is why the run made no comparison. It is absent when Report is
	// present: a comparison that could not decide says why in its report.
	Error *errorkind.Report `json:"error,omitempty"`
}

// targets runs the report over every target the env expands to, in order. A
// target that drifted, could not be compared, or failed does not stop the
// run: each is reported, and the run exits with the most serious status any
// target had. Only the operator stopping the run stops it early.
func (r driftRun) targets(configs []projectconfig.Config, out *report) error {
	document := TargetsDocument{ContractVersion: ContractVersion, Command: Command, Env: configs[0].EnvName}
	for _, projectCfg := range configs {
		ctx := dbcli.WithProjectMigrationDirectory(r.cmd.Context(), projectCfg)
		result, err := r.target(ctx, projectCfg)
		if err != nil && r.cmd.Context().Err() != nil {
			return err
		}
		entry := Target{Instance: projectCfg.EnvInstance, URL: dburldisplay.Format(result.dbURL)}
		if err == nil {
			entry.Report, err = out.encode(ctx, result.document, result.codecs)
		}
		if err == nil {
			entry.ExitCode = exitcode.Code(result.status, 0)
		} else {
			// As the root redacts the error of a run over one target: a
			// connection error can quote the address it failed to open.
			err = dburldisplay.RedactRecordedError(ctx, err)
			entry.ExitCode = exitcode.Code(err, 2)
			entry.Error = errorkind.NewReport(err)
		}
		document.Targets = append(document.Targets, entry)
		if writeErr := out.writeTarget(document.Env, entry, result.document); writeErr != nil {
			return writeErr
		}
	}
	if err := out.writeTargets(document); err != nil {
		return err
	}
	return targetsStatus(document.Targets)
}

// targetsStatus is the run's answer once every target is reported: 2 when
// any target could not be compared, else 1 when any drifted, else nothing.
// The report has said which target did what, so the root prints nothing more.
func targetsStatus(targets []Target) error {
	count := tally(targets)
	drifted, failed := count.drifted, count.failed
	switch {
	case failed > 0:
		return exitcode.New(2, fmt.Errorf("%d of %d targets could not be compared with their applied history",
			failed, len(targets)))
	case drifted > 0:
		return exitcode.New(1, fmt.Errorf("%d of %d targets differ from the schema their applied migrations produce",
			drifted, len(targets)))
	default:
		return nil
	}
}

// targetCount counts the targets of a run by their exit status.
type targetCount struct {
	clean, drifted, failed int
}

func tally(targets []Target) targetCount {
	var count targetCount
	for _, target := range targets {
		switch target.ExitCode {
		case 0:
			count.clean++
		case 1:
			count.drifted++
		default:
			count.failed++
		}
	}
	return count
}

// otherHistory is a revision table in the format the run did not select that
// records applied migrations.
type otherHistory struct {
	format  migrator.RevisionTableFormat
	table   string
	applied int
}

// readOtherFormat reads the revision table the other format keeps by
// default, for a run whose own revision table records nothing. A history
// written in one format and read in the other is the likeliest reason a
// database with objects has no applied migration, and naming it is what keeps
// a person from running statements that drop the whole database. It reads
// under the caller's suppressed writes, so an absent table is created by
// neither format, and a read that fails says nothing: this is a hint, and
// the report stands without it.
func readOtherFormat(ctx context.Context, mig *migrator.Migrator, format migrator.RevisionTableFormat) *otherHistory {
	otherFormat := migrator.RevisionTableFormatAtlas
	if format == migrator.RevisionTableFormatAtlas {
		otherFormat = migrator.RevisionTableFormatPtah
	}
	probe := mig.WithRevisionTableFormat(otherFormat).WithMigrationsTable("", "")
	status, err := probe.GetMigrationStatus(ctx)
	if err != nil || len(status.AppliedMigrations) == 0 {
		return nil
	}
	return &otherHistory{format: otherFormat, table: probe.MigrationsTableIdentifier(), applied: len(status.AppliedMigrations)}
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
	// RevisionTable is the revision table the history was read from, quoted
	// and qualified as the engine names it.
	RevisionTable string `json:"revision_table"`
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
	// Undecided are the objects the applied migrations create that the
	// database read could not describe, reported beside the drift a check
	// found. Whether the database holds them is unknown, and the statements
	// do not cover them. A check that found nothing else carries them in
	// Check.Findings instead, as one that could not decide.
	Undecided []migrator.DriftFinding `json:"undecided,omitempty"`
	// Warnings say what a person must know before acting on the report, such
	// as a revision table that records no applied migration while the
	// database holds objects.
	Warnings []string `json:"warnings,omitempty"`
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

func newDocument(
	mig *migrator.Migrator, conn *dbschema.DatabaseConnection, status *migrator.MigrationStatus,
	found driftgate.Result, other *otherHistory,
) Document {
	revisionTable := mig.MigrationsTableIdentifier()
	document := Document{
		ContractVersion: ContractVersion,
		Command:         Command,
		Dialect:         conn.Info().Dialect,
		RevisionTable:   revisionTable,
		Pending:         mig.VersionLabels(status.PendingMigrations),
		Check:           found.Check,
		Undecided:       found.Undecided,
		Diff:            found.Diff,
	}
	if len(status.AppliedMigrations) > 0 {
		document.CurrentVersion = mig.VersionLabel(status.CurrentVersion)
	}
	// Every object then reads as one no migration created, and the statements
	// drop them all, the history's real revision table among them when the
	// run named the wrong one. A database with no object is not drift, and
	// has nothing to warn about.
	if len(status.AppliedMigrations) == 0 && found.Check.Outcome == migrator.DriftDrifted {
		document.Warnings = append(document.Warnings, emptyHistoryWarning(revisionTable, other))
	}
	return document
}

// emptyHistoryWarning says why a database with objects reads as having no
// applied migration, and how to name the history the run missed.
func emptyHistoryWarning(revisionTable string, other *otherHistory) string {
	const dropsEverything = "the statements below would drop every object the report compares"
	if other != nil {
		applied := "applied migrations"
		if other.applied == 1 {
			applied = "applied migration"
		}
		return fmt.Sprintf("the revision table %s records no applied migration, or does not exist, "+
			"but the %s-format revision table %s records %d %s: run the report again with --%s %s; %s",
			revisionTable, other.format, other.table, other.applied, applied,
			dbcli.RevisionTableFormatFlagName, other.format, dropsEverything)
	}
	return fmt.Sprintf("the revision table %s records no applied migration, or does not exist, and the database "+
		"holds objects: if the history is recorded elsewhere, name its table with --%s and --%s, "+
		"and its format with --%s; %s",
		revisionTable, dbcli.MigrationsTableFlagName, dbcli.MigrationsSchemaFlagName,
		dbcli.RevisionTableFormatFlagName, dropsEverything)
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

// encode is the JSON a target's document is reported with in a
// [TargetsDocument], or nil for a text report, which needs none.
func (r *report) encode(ctx context.Context, document Document, codecs schemaext.Registry) (json.RawMessage, error) {
	if !r.json {
		return nil, nil
	}
	encoded, err := featurejson.Marshal(ctx, codecs, schemaext.Desired, document)
	if err != nil {
		return nil, fmt.Errorf("write the JSON result: %w", err)
	}
	return encoded, nil
}

// writeTarget reports one target of a run over an env's targets as soon as
// it is done, so a long run shows its progress: a heading naming the target,
// then its report, or the error that kept it from making one. A JSON report
// waits for [report.writeTargets].
func (r *report) writeTarget(env string, target Target, document Document) error {
	if r.json {
		return nil
	}
	r.written = true
	heading := fmt.Sprintf("Env %q", env)
	if target.Instance > 0 {
		heading = fmt.Sprintf("Instance %d of env %q", target.Instance, env)
	}
	if target.URL != "" {
		heading += ": " + target.URL
	}
	if _, err := fmt.Fprintf(r.out, "%s\n", heading); err != nil {
		return err
	}
	if target.Error != nil {
		_, err := fmt.Fprintf(r.out, "Error: %s\n\n", target.Error.Message)
		return err
	}
	if err := writeText(r.out, document); err != nil {
		return err
	}
	_, err := io.WriteString(r.out, "\n")
	return err
}

// writeTargets closes a run over an env's targets: the one JSON document, or
// a line that counts the targets by what their reports found.
func (r *report) writeTargets(document TargetsDocument) error {
	r.written = true
	if r.json {
		encoded, err := json.MarshalIndent(document, "", "  ")
		if err != nil {
			return fmt.Errorf("write the JSON result: %w", err)
		}
		_, err = r.out.Write(append(encoded, '\n'))
		return err
	}
	count := tally(document.Targets)
	noun := "targets"
	if len(document.Targets) == 1 {
		noun = "target"
	}
	_, err := fmt.Fprintf(r.out, "Summary: %d %s: %d without drift, %d with drift, %d not compared.\n",
		len(document.Targets), noun, count.clean, count.drifted, count.failed)
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
	fmt.Fprintf(&b, "Revision table: %s\n", document.RevisionTable)
	fmt.Fprintf(&b, "Current version: %s\n", current)
	fmt.Fprintf(&b, "Pending migrations: %d\n", len(document.Pending))
	for _, version := range document.Pending {
		fmt.Fprintf(&b, "  - %s\n", version)
	}
	b.WriteString("\n")
	for _, warning := range document.Warnings {
		fmt.Fprintf(&b, "Warning: %s.\n\n", warning)
	}
	check := document.Check
	switch check.Outcome {
	case migrator.DriftClean:
		fmt.Fprintf(&b, "No drift: the database matches the schema its %s.\n", driftgate.AppliedMigrations(check))
	case migrator.DriftDrifted:
		fmt.Fprintf(&b, "Drift: the database differs from the schema its %s:\n", driftgate.AppliedMigrations(check))
		for _, finding := range check.Findings {
			fmt.Fprintf(&b, "  - %s\n", finding.Message)
		}
		writeUndecided(&b, document.Undecided)
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

func writeUndecided(b *strings.Builder, undecided []migrator.DriftFinding) {
	if len(undecided) == 0 {
		return
	}
	b.WriteString("\nThe database read could not describe these objects the applied migrations create, " +
		"so whether the database holds them is unknown, and the statements below do not cover them:\n")
	for _, finding := range undecided {
		fmt.Fprintf(b, "  - %s\n", finding.Message)
	}
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
