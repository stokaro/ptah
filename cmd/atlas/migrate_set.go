package atlas

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/migrator"
)

type atlasMigrateSetOptions struct {
	url             string
	dir             string
	dirFormat       string
	atlasEnv        string
	revisionsSchema string
}

func newAtlasMigrateSetCommand() *cobra.Command {
	opts := atlasMigrateSetOptions{
		dir:       "file://migrations",
		dirFormat: atlasDirFormatDefault,
	}
	cmd := &cobra.Command{
		Use:   "set [flags] [version]",
		Short: "Set migration revision state",
		Long: `Edit Atlas revision metadata to consider every migration through the
given version applied, without executing migration SQL.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAtlasMigrateSet(cmd, opts, args)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL")
	flags.StringVar(&opts.dir, "dir", opts.dir, "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", opts.dirFormat, "Migration directory format")
	flags.StringVar(&opts.revisionsSchema, "revisions-schema", "", "Schema for the revision table")
	cmdutil.ConfigureCommandArgs(cmd, nil)
	cmd.SetFlagErrorFunc(failAtlasCommand)
	return cmd
}

func atlasMigrateSetExactArgs(cmd *cobra.Command, args []string) error {
	if err := cobra.ExactArgs(1)(cmd, args); err != nil {
		return failAtlasCommand(cmd, err)
	}
	return nil
}

func runAtlasMigrateSet(cmd *cobra.Command, opts atlasMigrateSetOptions, args []string) error {
	opts, dir, err := prepareAtlasMigrateSet(cmd, opts)
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	if opts.url == "" {
		return failAtlasCommand(cmd, fmt.Errorf("sql/sqlclient: missing driver. See: https://atlasgo.io/url"))
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("error connecting to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	if err := atlasMigrateSetExactArgs(cmd, args); err != nil {
		return err
	}
	version, err := parseAtlasMigrateSetVersion(args[0])
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	result, err := atlasmigrate.Set(cmd.Context(), conn, version, atlasmigrate.SetOptions{
		Dir:             dir,
		FS:              os.DirFS(dir),
		AtlasEnv:        opts.atlasEnv,
		RevisionsSchema: opts.revisionsSchema,
	})
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	if err := writeAtlasMigrateSetResult(cmd, result); err != nil {
		return failAtlasCommand(cmd, err)
	}
	return nil
}

func prepareAtlasMigrateSet(
	cmd *cobra.Command,
	opts atlasMigrateSetOptions,
) (atlasMigrateSetOptions, string, error) {
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if !cmd.Flags().Changed("url") {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return atlasMigrateSetOptions{}, "", err
	}
	if loaded {
		opts.url = dbcli.EffectiveString(
			cmd,
			"url",
			opts.url,
			projectCfg.StringValue(projectconfig.StringDatabaseURL),
		)
		opts.dir = dbcli.EffectiveString(
			cmd,
			"dir",
			opts.dir,
			projectCfg.StringValue(projectconfig.StringMigrationDir),
		)
		opts.dirFormat = dbcli.EffectiveString(
			cmd,
			"dir-format",
			opts.dirFormat,
			projectCfg.StringValue(projectconfig.StringMigrationFormat),
		)
		opts.atlasEnv = dbcli.EffectiveString(
			cmd,
			dbcli.EnvFlagName,
			opts.atlasEnv,
			projectCfg.StringValue(projectconfig.StringEnvName),
		)
		opts.revisionsSchema = dbcli.EffectiveString(
			cmd,
			"revisions-schema",
			opts.revisionsSchema,
			projectCfg.StringValue(projectconfig.StringMigrationRevisionsSchema),
		)
	}
	if opts.dir == "" {
		return atlasMigrateSetOptions{}, "", fmt.Errorf("migrations directory is required")
	}
	format, err := atlasMigrateDirFormatValue(opts.dirFormat)
	if err != nil {
		return atlasMigrateSetOptions{}, "", fmt.Errorf("atlas migrate set --dir-format: %w", err)
	}
	if format != atlasDirFormatDefault {
		return atlasMigrateSetOptions{}, "", fmt.Errorf("atlas migrate set --dir-format: expected atlas")
	}
	if loaded &&
		!cmd.Flags().Changed("dir") &&
		projectCfg.StringValue(projectconfig.StringMigrationDir).Present {
		opts.dir, err = atlasProjectConfigLocalDir(cmd, opts.dir)
		if err != nil {
			return atlasMigrateSetOptions{}, "", fmt.Errorf("atlas migrate set --dir: %w", err)
		}
	}
	dir, err := atlasargs.LocalDirValue(opts.dir)
	if err != nil {
		return atlasMigrateSetOptions{}, "", fmt.Errorf("atlas migrate set --dir: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return atlasMigrateSetOptions{}, "", fmt.Errorf("sql/migrate: %w", err)
	}
	if !info.IsDir() {
		return atlasMigrateSetOptions{}, "", fmt.Errorf("sql/migrate: %q is not a dir", dir)
	}
	dir, err = pathguard.ResolveCLIPath(dir)
	if err != nil {
		return atlasMigrateSetOptions{}, "", fmt.Errorf("resolve migration directory: %w", err)
	}
	return opts, dir, nil
}

func parseAtlasMigrateSetVersion(value string) (int64, error) {
	version, err := atlasmigrate.ParseMigrationVersionFlag("version", value)
	if err != nil {
		return 0, err
	}
	if version == 0 {
		return 0, fmt.Errorf("migration version must be greater than zero")
	}
	return version, nil
}

func writeAtlasMigrateSetResult(cmd *cobra.Command, result migrator.AtlasRevisionSetResult) error {
	if len(result.Set) == 0 && len(result.Removed) == 0 {
		return nil
	}
	changes := make([]string, 0, 2)
	if len(result.Set) > 0 {
		changes = append(changes, fmt.Sprintf("%d set", len(result.Set)))
	}
	if len(result.Removed) > 0 {
		changes = append(changes, fmt.Sprintf("%d removed", len(result.Removed)))
	}

	out := cmd.OutOrStdout()
	if _, err := fmt.Fprintf(
		out,
		"Current version is %d (%s):\n\n",
		result.CurrentVersion,
		strings.Join(changes, ", "),
	); err != nil {
		return fmt.Errorf("write migrate set summary: %w", err)
	}
	for _, revision := range result.Set {
		if err := writeAtlasMigrateSetRevision(out, "+", revision); err != nil {
			return err
		}
	}
	for _, revision := range result.Removed {
		if err := writeAtlasMigrateSetRevision(out, "-", revision); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(out); err != nil {
		return fmt.Errorf("write migrate set terminator: %w", err)
	}
	return nil
}

func writeAtlasMigrateSetRevision(
	out io.Writer,
	action string,
	revision migrator.AtlasRevisionChange,
) error {
	if revision.Description == "" {
		if _, err := fmt.Fprintf(out, "  %s %d\n", action, revision.Version); err != nil {
			return fmt.Errorf("write migrate set revision: %w", err)
		}
		return nil
	}
	if _, err := fmt.Fprintf(out, "  %s %d (%s)\n", action, revision.Version, revision.Description); err != nil {
		return fmt.Errorf("write migrate set revision: %w", err)
	}
	return nil
}
