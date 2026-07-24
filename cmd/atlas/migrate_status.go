package atlas

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/pathguard"
)

type atlasMigrateStatusOptions struct {
	url             string
	dir             string
	dirFormat       string
	atlasEnv        string
	revisionsSchema string
	format          string
}

func newAtlasMigrateStatusCommand() *cobra.Command {
	opts := atlasMigrateStatusOptions{
		dir:       "file://migrations",
		dirFormat: atlasDirFormatDefault,
	}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show migration status",
		Long: `Report Atlas migration status for a live database and migration directory.

Native Ptah equivalent: ptah migrations status with Atlas revision-table
metadata.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasMigrateStatus(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL")
	flags.StringVar(&opts.dir, "dir", opts.dir, "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", opts.dirFormat, "Migration directory format")
	flags.StringVar(&opts.revisionsSchema, "revisions-schema", "", "Schema for the revision table")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasMigrateStatus(cmd *cobra.Command, opts atlasMigrateStatusOptions) error {
	formatOutput := cmd.Flags().Changed("format")
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if needsAtlasMigrateStatusConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		opts.url = dbcli.EffectiveString(cmd, "url", opts.url, projectCfg.DatabaseURL)
		opts.dir = dbcli.EffectiveString(cmd, "dir", opts.dir, projectCfg.Migration.Dir)
		opts.dirFormat = dbcli.EffectiveString(cmd, "dir-format", opts.dirFormat, projectCfg.Migration.Format)
		opts.atlasEnv = dbcli.EffectiveString(cmd, dbcli.EnvFlagName, opts.atlasEnv, projectCfg.EnvName)
		opts.revisionsSchema = dbcli.EffectiveString(cmd, "revisions-schema", opts.revisionsSchema, projectCfg.Migration.RevisionsSchema)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, projectCfg.Format.Migrate.Status)
		formatOutput = formatOutput || projectCfg.Format.Migrate.Status != ""
	}
	if loaded && !cmd.Flags().Changed("dir") && projectCfg.Migration.Dir != "" {
		opts.dir, err = atlasProjectConfigLocalDir(cmd, opts.dir)
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir: %w", err))
		}
	}
	if err := validateAtlasMigrateStatusOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	format, err := atlasMigrateDirFormatValue(opts.dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir-format: %w", err))
	}
	if format != atlasDirFormatDefault {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir-format: expected atlas"))
	}
	if formatOutput {
		if err := validateAtlasMigrateStatusFormat(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := atlasreport.ValidateMigrateStatusTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}

	dir, err := atlasargs.LocalDirValue(opts.dir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir: %w", err))
	}
	dir, err = pathguard.ResolveCLIPath(dir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("resolve migration directory: %w", err))
	}
	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("error connecting to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.Status(cmd.Context(), conn, atlasmigrate.StatusOptions{
		Dir:             dir,
		AtlasEnv:        opts.atlasEnv,
		RevisionsSchema: opts.revisionsSchema,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if formatOutput {
		err := atlasreport.WriteMigrateStatusFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateStatusOptions{
			Driver:           conn.Info().Dialect,
			URL:              opts.url,
			Dir:              atlasStatusDirURL(opts.dir),
			FS:               os.DirFS(dir),
			Status:           result.Status,
			AppliedRevisions: result.AppliedRevisions,
		})
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		return nil
	}
	writeAtlasMigrateStatusDefault(cmd, result)
	return nil
}

func needsAtlasMigrateStatusConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("url")
}

func validateAtlasMigrateStatusOptions(opts atlasMigrateStatusOptions) error {
	if opts.url == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.dir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	return nil
}

func validateAtlasMigrateStatusFormat(format string) error {
	if strings.TrimSpace(format) == "" {
		return fmt.Errorf("--format must not be empty")
	}
	return nil
}

func writeAtlasMigrateStatusDefault(cmd *cobra.Command, result atlasmigrate.StatusResult) {
	status := result.Status
	out := cmd.OutOrStdout()
	fmt.Fprintln(out, "=== MIGRATION STATUS ===")
	fmt.Fprintf(out, "Current Version: %d\n", status.CurrentVersion)
	fmt.Fprintf(out, "Total Migrations: %d\n", status.TotalMigrations)
	fmt.Fprintf(out, "Applied Migrations: %d\n", len(status.AppliedMigrations))
	fmt.Fprintf(out, "Pending Migrations: %d\n", len(status.PendingMigrations))
	if status.HasPendingChanges {
		fmt.Fprintln(out, "Status: Pending migrations available")
		return
	}
	fmt.Fprintln(out, "Status: Database is up to date")
}

func atlasStatusDirURL(raw string) string {
	value := raw
	if !strings.Contains(value, "://") {
		value = "file://" + value
	}
	if strings.Contains(value, "?") {
		return value + "&format=atlas"
	}
	return value + "?format=atlas"
}
