// Package migraterepair implements "ptah migrations repair", which fixes dirty
// or partial migration revision metadata after a failed run.
package migraterepair

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/core/lintext"
	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/migrateflags"
	"ptah.run/internal/cli/internal/migratelock"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/migrationintegrity"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

const (
	dbURLFlag      = "db-url"
	migrationsFlag = "migrations-dir"
	versionFlag    = "version"
	dirFormatFlag  = "dir-format"
	atlasEnvFlag   = "atlas-env"
	forceFlag      = "force"
	resumeFromFlag = "resume-from"
	repeatableFlag = "repeatable"
)

type options struct {
	dbURL               string
	migrationsDir       string
	version             string
	dirFormat           string
	atlasEnv            string
	force               bool
	resumeFrom          string
	repeatable          string
	runner              string
	connectTimeout      string
	configPath          string
	envName             string
	migrationsSchema    string
	migrationsTable     string
	migrationsEngine    string
	revisionTableFormat string
	migrationLockName   string
}

func NewMigrateRepairCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "repair",
		Short: "Repair dirty migration metadata",
		Long: `Repair dirty migration metadata after an operator has fixed a
half-applied migration manually, or resume the migration from a specific
statement.

Without --resume-from, repair runs no migration SQL. An incomplete up
migration requires --force to record it applied after you have run or
reconciled the missing SQL yourself.

The revision records which direction left it dirty, and repair follows it. A
migration that failed on the way up resumes its up statements and is recorded
applied. A rollback that stopped partway resumes its down statements and its
revision is removed, because a finished rollback means the migration is no
longer applied; without --resume-from, a rollback that already committed a
statement is refused rather than recorded applied over a schema it changed.
Repair holds the migration advisory lock across inspection, resumed SQL, and
the final metadata write. The lock is named ptah_migrate unless
--migration-lock-name selects another name, and it has to be the name the
history's other commands use, or repair does not wait for them.

With --runner golang-migrate, a file that opens and closes its own
transactions is read with those transactions, as "migrations up" and
"migrations down" run it. A revision whose every statement committed is
finished without running SQL: an up migration is recorded applied, and a
rollback's revision is removed. Such a file is never resumed from a
statement, because a resume would run the rest of it outside the
transactions it opens; the refusal names what ends the revision instead.

With --repeatable in place of --version, repair deletes the row of one
repeatable migration from the record beside the revision table, and runs no
migration SQL. That is the way out of a row whose file was deleted or renamed:
such a row blocks every rollback, and one whose last attempt did not finish
blocks every up run, because nothing can run it again. What the migration
created stays. A row whose file is still in the directory is removed only with
--force, and the next up run then executes that file as new.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return migrateRepairCommand(cmd, &opts)
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
	flags.StringVar(&opts.version, versionFlag, "", "Migration version to repair; required unless --repeatable is given")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrationfile.DirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.BoolVar(&opts.force, forceFlag, false, "Allow metadata-only repair: record an incomplete up migration applied without running SQL, "+
		"or rewrite or create its revision row; with --repeatable, remove the row of a repeatable migration whose file is still in the directory")
	flags.StringVar(&opts.resumeFrom, resumeFromFlag, "", "Execute the remaining statements of the body that failed, starting from this 1-based statement number. An up migration is then marked applied; a rollback's revision is removed")
	flags.StringVar(&opts.repeatable, repeatableFlag, "", "Identity of a repeatable migration, the name between R__ and .sql, whose row to delete from the repeatable migration record instead of repairing a version")
	// An identity names one row to delete, which is a choice for one command
	// line rather than state an environment carries; a PTAH_REPEATABLE left
	// set would also collide with a PTAH_VERSION meant for another repair.
	// The flag was registered above, so the lookup cannot fail.
	_ = cmdflags.DisableEnvBinding(flags, repeatableFlag)
	dbcli.RegisterRunnerFlag(flags, &opts.runner)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterMigrationsEngineFlag(flags, &opts.migrationsEngine)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
	migratelock.RegisterNameFlag(flags, &opts.migrationLockName)
}

// resolveProjectOptions merges ptah.yaml under the flags, the way
// `migrations set` reads it, and resolves the runner as `migrations up` and
// `migrations down` do.
func resolveProjectOptions(cmd *cobra.Command, opts options, projectCfg projectconfig.Config) (options, migrateflags.Runner, error) {
	effectiveString := func(flagName, flagValue string, field projectconfig.StringField) string {
		return dbcli.EffectiveString(cmd, flagName, flagValue, projectCfg.StringValue(field))
	}
	opts.dbURL = effectiveString(dbURLFlag, opts.dbURL, projectconfig.StringDatabaseURL)
	opts.migrationsDir = effectiveString(migrationsFlag, opts.migrationsDir, projectconfig.StringMigrationDir)
	opts.dirFormat = effectiveString(dirFormatFlag, opts.dirFormat, projectconfig.StringMigrationFormat)
	opts.atlasEnv = effectiveString(atlasEnvFlag, opts.atlasEnv, projectconfig.StringEnvName)
	opts.migrationsSchema = effectiveString(
		dbcli.MigrationsSchemaFlagName, opts.migrationsSchema, projectconfig.StringMigrationRevisionsSchema)
	opts.migrationsTable = effectiveString(
		dbcli.MigrationsTableFlagName, opts.migrationsTable, projectconfig.StringMigrationRevisionsTable)
	opts.revisionTableFormat = effectiveString(
		dbcli.RevisionTableFormatFlagName, opts.revisionTableFormat, projectconfig.StringMigrationRevisionFormat)
	opts.connectTimeout = effectiveString(
		dbcli.ConnectTimeoutFlagName, opts.connectTimeout, projectconfig.StringMigrationConnectTimeout)
	opts.migrationLockName = effectiveString(
		migratelock.NameFlag, opts.migrationLockName, projectconfig.StringMigrationMigrationLockName)
	runner, err := dbcli.ResolveRunner(cmd, opts.runner, projectCfg)
	if err != nil {
		return options{}, "", err
	}
	return opts, runner, nil
}

// lockNameRequest is the lock-name input of this command against dbURL.
func lockNameRequest(cmd *cobra.Command, projectCfg projectconfig.Config, dbURL string) migratelock.Request {
	return migratelock.NameRequest(
		cmd, projectCfg.StringValue(projectconfig.StringMigrationMigrationLockName).Present, dbURL)
}

func migrateRepairCommand(cmd *cobra.Command, opts *options) error {
	integrityPolicy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return err
	}
	// Before the early returns, so a runner that names no tool is refused on
	// every invocation, as `migrations up` and `migrations down` refuse it.
	resolved, runner, err := resolveProjectOptions(cmd, *opts, projectCfg)
	if err != nil {
		return err
	}
	opts = &resolved
	lockRequest := lockNameRequest(cmd, projectCfg, opts.dbURL)
	lockName, err := migratelock.ResolveName(lockRequest, opts.migrationLockName)
	if err != nil {
		return err
	}
	if strings.TrimSpace(opts.dbURL) == "" {
		return dbcli.MissingProjectValue(projectCfg, fmt.Errorf("database URL is required"))
	}
	if strings.TrimSpace(opts.migrationsDir) == "" {
		return dbcli.MissingProjectValue(projectCfg, fmt.Errorf("migrations directory is required"))
	}
	if err := settleRepeatableSelection(cmd, opts); err != nil {
		return err
	}
	target, err := parseRepairTarget(opts)
	if err != nil {
		return err
	}
	dirFormat, err := migrationfile.ParseDirFormat(opts.dirFormat)
	if err != nil {
		return err
	}
	revisionFormat, err := migrateflags.ParseRevisionTableFormat(opts.revisionTableFormat)
	if err != nil {
		return err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}
	// Before the source is read and the target opened, so a lock name the
	// target's dialect cannot honor is refused without touching either.
	if err := lockRequest.DecideFromURL(); err != nil {
		return err
	}

	source, err := migrationsource.CaptureLocal(opts.migrationsDir, migrationsource.LocalOptions{})
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	// The shared integrity gate, and it fires only for --resume-from.
	resumeFrom := target.resumeFrom
	//
	// That is the class predicate applied exactly, not a softened version of
	// it. A plain repair rewrites revision metadata and executes none of the
	// directory's SQL, so it is outside the class and stays usable on a drifted
	// directory — which matters, because clearing a dirty row is a recovery
	// step an operator may genuinely need before they can re-hash anything.
	// --resume-from is different: it executes the remaining statements of the
	// body that failed, straight from the file, so it is a member and gates
	// like one. Resuming a half-applied migration out of a rewritten file is
	// the same hazard as `down`, with the operator's attention already
	// elsewhere.
	if resumeFrom > 0 {
		if _, err := migrationintegrity.GateWithPolicy(
			cmd.Context(), cmd.ErrOrStderr(), source.FileSystem, dirFormat, integrityPolicy, migrationintegrity.Options{},
		); err != nil {
			return err
		}
	}
	providerOptions := []migrator.FSProviderOption{
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: opts.atlasEnv}),
	}
	// The directory is read the way its runner runs it, so a file that opens
	// its own transactions is repaired with them, as it was applied or
	// rolled back.
	if runner.TransactionOwner() == lintext.TransactionOwnerFile {
		providerOptions = append(providerOptions, migrator.WithOwnTransactions())
	}
	// Only a repeatable repair reads the repeatable files, to tell a row no
	// file carries from one whose file is still here.
	if target.repeatable != "" {
		providerOptions = append(providerOptions, migrator.WithRepeatableMigrations())
	}
	provider, err := migrator.NewFSMigrationProvider(source.FileSystem, providerOptions...)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}

	ctx := cmd.Context()
	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	// Before the migrator is built, so no revision row is written under a lock
	// the operator asked for and the target never took.
	if err := lockRequest.DecideSession(conn); err != nil {
		return err
	}

	mig := migrator.NewMigrator(conn, provider).
		WithMigrationsTable(opts.migrationsSchema, opts.migrationsTable).
		WithMigrationsEngine(opts.migrationsEngine).
		WithRevisionTableFormat(revisionFormat).
		WithMigrationLockName(lockName.Value)

	if target.repeatable != "" {
		return removeRepeatableRecord(cmd, mig, target.repeatable, opts.force)
	}
	version, err := mig.ResolveVersion(target.version)
	if err != nil {
		return fmt.Errorf("invalid migration version %q: %w", target.version, err)
	}
	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    version,
		Force:      opts.force,
		ResumeFrom: resumeFrom,
	})
	if err != nil {
		return err
	}

	if resumeFrom == 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "Updated migration %s revision metadata; no migration SQL ran\n", mig.VersionLabel(version))
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "Repaired migration %s\n", mig.VersionLabel(version))
	}
	return nil
}

// settleRepeatableSelection withdraws a --version or --resume-from that
// arrived from PTAH_VERSION or PTAH_RESUME_FROM once --repeatable is given.
// --repeatable reads no variable, so it is always typed, and a typed choice
// of the record row settles which repair this command line asks for: a
// variable exported for a versioned repair would otherwise refuse it as a
// request for two repairs, naming a flag the command line never carried.
func settleRepeatableSelection(cmd *cobra.Command, opts *options) error {
	if opts.repeatable == "" {
		return nil
	}
	flags := cmd.Flags()
	if _, fromEnv := cmdflags.AppliedEnvName(flags, versionFlag); fromEnv {
		if err := cmdflags.WithdrawEnvValue(flags, versionFlag); err != nil {
			return err
		}
		opts.version = ""
	}
	if _, fromEnv := cmdflags.AppliedEnvName(flags, resumeFromFlag); fromEnv {
		if err := cmdflags.WithdrawEnvValue(flags, resumeFromFlag); err != nil {
			return err
		}
		opts.resumeFrom = ""
	}
	return nil
}

// repairTarget is what one invocation repairs: the revision of a versioned
// migration, or the record row of a repeatable one.
type repairTarget struct {
	// version is --version as written; the migrator resolves it once it has
	// read the directory, so v1 names v1_x.sql.
	version    string
	resumeFrom int
	repeatable string
}

// parseRepairTarget reads which repair the flags ask for. --version and
// --repeatable name different tables, so exactly one of them is given, and
// --resume-from belongs to a version: a repeatable migration that did not
// finish runs again from its first statement under 'migrations up
// --allow-dirty'.
func parseRepairTarget(opts *options) (repairTarget, error) {
	switch {
	case opts.version != "" && opts.repeatable != "":
		return repairTarget{}, fmt.Errorf("--version and --repeatable select different repairs; give one of them")
	case opts.repeatable != "" && opts.resumeFrom != "":
		return repairTarget{}, fmt.Errorf("--resume-from resumes a versioned migration; a repeatable migration " +
			"that did not finish runs again from its first statement with 'ptah migrations up --allow-dirty'")
	case opts.repeatable != "":
		if _, err := migrationfile.ParseRepeatableFileName(migrationfile.RepeatableFileName(opts.repeatable)); err != nil {
			return repairTarget{}, fmt.Errorf("invalid repeatable migration identity %q: %w", opts.repeatable, err)
		}
		return repairTarget{repeatable: opts.repeatable}, nil
	case opts.version == "":
		return repairTarget{}, fmt.Errorf("migration version is required, or --repeatable for a repeatable migration's record")
	}
	// Text that is not a number names an Atlas file whose version is not one,
	// such as v1; a number has to name a migration.
	version, err := strconv.ParseInt(opts.version, 10, 64)
	if errors.Is(err, strconv.ErrRange) || (err == nil && version <= 0) {
		return repairTarget{}, fmt.Errorf("invalid migration version %q", opts.version)
	}
	resumeFrom, err := parseResumeFrom(opts.resumeFrom)
	if err != nil {
		return repairTarget{}, err
	}
	return repairTarget{version: opts.version, resumeFrom: resumeFrom}, nil
}

// removeRepeatableRecord deletes the record row of identity and says what the
// row recorded.
func removeRepeatableRecord(cmd *cobra.Command, mig *migrator.Migrator, identity string, force bool) error {
	removed, err := mig.RemoveRepeatableRecord(cmd.Context(), migrator.RemoveRepeatableRecordOptions{
		Identity: identity,
		Force:    force,
	})
	if err != nil {
		return err
	}
	where := "its file is still in the directory, and the next up run executes it as new"
	if removed.Orphaned {
		where = "no file of the directory carries it"
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Removed the record of repeatable migration %s, state %s; %s; %sno migration SQL ran\n",
		identity, removed.State, where, lastAttemptNote(removed))
	return nil
}

// lastAttemptNote says what a removed row leaves unknown about the database:
// an attempt with no outcome may have taken effect, and a failed one that ran
// without a transaction may have taken effect in part. The row was the only
// record of that, so the operator is told once more before it goes.
func lastAttemptNote(record *migrator.RepeatableRecord) string {
	switch {
	case record.State == migrator.RepeatableStateRunning:
		return "its last attempt recorded no outcome, so it may have taken effect; "
	case record.State == migrator.RepeatableStateFailed && record.NoTransaction:
		return "its last attempt failed without a transaction, so it may have taken effect in part; "
	}
	return ""
}

func parseResumeFrom(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	resumeFrom, err := strconv.Atoi(value)
	if err != nil || resumeFrom <= 0 {
		return 0, fmt.Errorf("invalid resume-from value %q", value)
	}
	return resumeFrom, nil
}
