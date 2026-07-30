// Package migrateset implements the native `ptah migrations set` command: it
// moves the revision boundary to an arbitrary migration version in both
// directions without executing migration SQL.
package migrateset

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/migrationsource"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	dbURLFlag      = "db-url"
	migrationsFlag = "migrations-dir"
	versionFlag    = "version"
	dirFormatFlag  = "dir-format"
	atlasEnvFlag   = "atlas-env"
	dryRunFlag     = "dry-run"
)

type options struct {
	dbURL               string
	migrationsDir       string
	version             string
	dirFormat           string
	atlasEnv            string
	dryRun              bool
	connectTimeout      string
	configPath          string
	envName             string
	migrationsSchema    string
	migrationsTable     string
	revisionTableFormat string
}

// NewMigrateSetCommand returns the migrations set command.
func NewMigrateSetCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Set the revision boundary to a migration version",
		Long: `Move the migration revision boundary to --version without executing any
migration SQL, in both directions: every migration through --version is
recorded as applied (dirty rows are marked applied, missing rows are
inserted), and revision rows above --version are removed.

This is a metadata-only operation for adopting Ptah on databases whose schema
was changed outside the migration flow, or for resetting revision bookkeeping
after a manual intervention. It never runs or reverts migration SQL; use
"ptah migrations up" and "ptah migrations down" for that.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runMigrateSet(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.StringVar(&opts.migrationsDir, migrationsFlag, "", "Local directory containing migration files (required)")
	flags.StringVar(&opts.version, versionFlag, "", "Migration version the revision boundary is moved to (required)")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Validate inputs and report the target version without changing revision metadata")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runMigrateSet(cmd *cobra.Command, opts options) error {
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	opts.dbURL = dbcli.EffectiveString(
		cmd,
		dbURLFlag,
		opts.dbURL,
		projectCfg.StringValue(projectconfig.StringDatabaseURL),
	)
	opts.migrationsDir = dbcli.EffectiveString(
		cmd,
		migrationsFlag,
		opts.migrationsDir,
		projectCfg.StringValue(projectconfig.StringMigrationDir),
	)
	opts.dirFormat = dbcli.EffectiveString(
		cmd,
		dirFormatFlag,
		opts.dirFormat,
		projectCfg.StringValue(projectconfig.StringMigrationFormat),
	)
	opts.atlasEnv = dbcli.EffectiveString(
		cmd,
		atlasEnvFlag,
		opts.atlasEnv,
		projectCfg.StringValue(projectconfig.StringEnvName),
	)
	opts.migrationsSchema = dbcli.EffectiveString(
		cmd,
		dbcli.MigrationsSchemaFlagName,
		opts.migrationsSchema,
		projectCfg.StringValue(projectconfig.StringMigrationRevisionsSchema),
	)
	opts.migrationsTable = dbcli.EffectiveString(
		cmd,
		dbcli.MigrationsTableFlagName,
		opts.migrationsTable,
		projectCfg.StringValue(projectconfig.StringMigrationRevisionsTable),
	)
	opts.revisionTableFormat = dbcli.EffectiveString(
		cmd,
		dbcli.RevisionTableFormatFlagName,
		opts.revisionTableFormat,
		projectCfg.StringValue(projectconfig.StringMigrationRevisionFormat),
	)
	connectTimeoutValue := dbcli.EffectiveString(
		cmd,
		dbcli.ConnectTimeoutFlagName,
		opts.connectTimeout,
		projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout),
	)

	if strings.TrimSpace(opts.dbURL) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("database URL is required"))
	}
	if strings.TrimSpace(opts.migrationsDir) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("migrations directory is required"))
	}
	version, err := parseSetVersion(opts.version)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	dirFormat, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(opts.revisionTableFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(connectTimeoutValue)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	source, err := migrationsource.CaptureLocal(opts.migrationsDir, migrationsource.LocalOptions{})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("error connecting to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)
	conn.SchemaWriter().SetDryRun(opts.dryRun)

	result, err := atlasmigrate.Set(cmd.Context(), conn, version, atlasmigrate.SetOptions{
		Dir:             source.Display,
		FS:              source.FileSystem,
		AtlasEnv:        opts.atlasEnv,
		RevisionsSchema: opts.migrationsSchema,
		RevisionsTable:  opts.migrationsTable,
		DirFormat:       dirFormat,
		RevisionFormat:  revisionFormat,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.dryRun {
		fmt.Fprintf(cmd.OutOrStdout(), "Dry run: would set the revision boundary to version %d.\n", version)
		return nil
	}
	if err := writeSetResult(cmd.OutOrStdout(), result); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return nil
}

func parseSetVersion(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("--%s is required", versionFlag)
	}
	version, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("--%s %q is not a valid migration version: %w", versionFlag, value, err)
	}
	if version <= 0 {
		return 0, fmt.Errorf("--%s must be greater than zero", versionFlag)
	}
	return version, nil
}

func writeSetResult(out io.Writer, result migrator.AtlasRevisionSetResult) error {
	if len(result.Set) == 0 && len(result.Removed) == 0 {
		_, err := fmt.Fprintf(out, "Revision state already at version %d; no changes to be made.\n", result.CurrentVersion)
		return err
	}
	changes := make([]string, 0, 2)
	if len(result.Set) > 0 {
		changes = append(changes, fmt.Sprintf("%d set", len(result.Set)))
	}
	if len(result.Removed) > 0 {
		changes = append(changes, fmt.Sprintf("%d removed", len(result.Removed)))
	}
	if _, err := fmt.Fprintf(out, "Current version is %d (%s):\n\n", result.CurrentVersion, strings.Join(changes, ", ")); err != nil {
		return fmt.Errorf("write migrations set summary: %w", err)
	}
	for _, revision := range result.Set {
		if err := writeSetRevision(out, "+", revision); err != nil {
			return err
		}
	}
	for _, revision := range result.Removed {
		if err := writeSetRevision(out, "-", revision); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(out); err != nil {
		return fmt.Errorf("write migrations set terminator: %w", err)
	}
	return nil
}

func writeSetRevision(out io.Writer, action string, revision migrator.AtlasRevisionChange) error {
	if revision.Description == "" {
		if _, err := fmt.Fprintf(out, "  %s %d\n", action, revision.Version); err != nil {
			return fmt.Errorf("write migrations set revision: %w", err)
		}
		return nil
	}
	if _, err := fmt.Fprintf(out, "  %s %d (%s)\n", action, revision.Version, revision.Description); err != nil {
		return fmt.Errorf("write migrations set revision: %w", err)
	}
	return nil
}
