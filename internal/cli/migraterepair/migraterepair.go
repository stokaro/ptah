// Package migraterepair implements "ptah migrations repair", which fixes dirty
// or partial migration revision metadata after a failed run.
package migraterepair

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/core/lintext"
	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/migrateflags"
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
)

type options struct {
	dbURL               string
	migrationsDir       string
	version             string
	dirFormat           string
	atlasEnv            string
	force               bool
	resumeFrom          string
	runner              string
	connectTimeout      string
	configPath          string
	envName             string
	migrationsSchema    string
	migrationsTable     string
	migrationsEngine    string
	revisionTableFormat string
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
the final metadata write.

With --runner golang-migrate, a file that opens and closes its own
transactions is read with those transactions, as "migrations up" and
"migrations down" run it. A revision whose every statement committed is
finished without running SQL: an up migration is recorded applied, and a
rollback's revision is removed. Such a file is never resumed from a
statement, because a resume would run the rest of it outside the
transactions it opens; the refusal names what ends the revision instead.`,
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
	flags.StringVar(&opts.version, versionFlag, "", "Migration version to repair (required)")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrationfile.DirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.BoolVar(&opts.force, forceFlag, false, "Allow metadata-only repair: record an incomplete up migration applied without running SQL, or rewrite or create its revision row")
	flags.StringVar(&opts.resumeFrom, resumeFromFlag, "", "Execute the remaining statements of the body that failed, starting from this 1-based statement number. An up migration is then marked applied; a rollback's revision is removed")
	dbcli.RegisterRunnerFlag(flags, &opts.runner)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterMigrationsEngineFlag(flags, &opts.migrationsEngine)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
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
	runner, err := dbcli.ResolveRunner(cmd, opts.runner, projectCfg)
	if err != nil {
		return options{}, "", err
	}
	return opts, runner, nil
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
	if strings.TrimSpace(opts.dbURL) == "" {
		return dbcli.MissingProjectValue(projectCfg, fmt.Errorf("database URL is required"))
	}
	if strings.TrimSpace(opts.migrationsDir) == "" {
		return dbcli.MissingProjectValue(projectCfg, fmt.Errorf("migrations directory is required"))
	}
	if opts.version == "" {
		return fmt.Errorf("migration version is required")
	}

	version, err := strconv.ParseInt(opts.version, 10, 64)
	if err != nil || version <= 0 {
		return fmt.Errorf("invalid migration version %q", opts.version)
	}
	resumeFrom, err := parseResumeFrom(opts.resumeFrom)
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

	source, err := migrationsource.CaptureLocal(opts.migrationsDir, migrationsource.LocalOptions{})
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	// The shared integrity gate, and it fires only for --resume-from.
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

	mig := migrator.NewMigrator(conn, provider).
		WithMigrationsTable(opts.migrationsSchema, opts.migrationsTable).
		WithMigrationsEngine(opts.migrationsEngine).
		WithRevisionTableFormat(revisionFormat)

	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    version,
		Force:      opts.force,
		ResumeFrom: resumeFrom,
	})
	if err != nil {
		return err
	}

	if resumeFrom == 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "Updated migration %d revision metadata; no migration SQL ran\n", version)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "Repaired migration %d\n", version)
	}
	return nil
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
