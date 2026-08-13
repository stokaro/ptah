// Package migraterepair implements "ptah migrations repair", which fixes dirty
// or partial migration revision metadata after a failed run.
package migraterepair

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/migration/migrator"
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
	connectTimeout      string
	migrationsSchema    string
	migrationsTable     string
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

The revision records which direction left it dirty, and repair follows it. A
migration that failed on the way up resumes its up statements and is recorded
applied. A rollback that stopped partway resumes its down statements and its
revision is removed, because a finished rollback means the migration is no
longer applied; without --resume-from, a rollback that already committed a
statement is refused rather than recorded applied over a schema it changed.
Repair holds the migration advisory lock across inspection, resumed SQL, and
the final metadata write.`,
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
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.BoolVar(&opts.force, forceFlag, false, "Rewrite or create the revision row even when it is not dirty")
	flags.StringVar(&opts.resumeFrom, resumeFromFlag, "", "Execute the remaining statements of the body that failed, starting from this 1-based statement number. An up migration is then marked applied; a rollback's revision is removed")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
}

func migrateRepairCommand(cmd *cobra.Command, opts *options) error {
	integrityPolicy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
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
	dirFormat, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
	if err != nil {
		return err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(opts.revisionTableFormat)
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
			cmd.ErrOrStderr(), source.FileSystem, dirFormat, integrityPolicy, migrationintegrity.Options{},
		); err != nil {
			return err
		}
	}
	provider, err := migrator.NewFSMigrationProvider(
		source.FileSystem,
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: opts.atlasEnv}),
	)
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
		WithRevisionTableFormat(revisionFormat)

	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    version,
		Force:      opts.force,
		ResumeFrom: resumeFrom,
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Repaired migration %d\n", version)
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
