package atlas

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/migration/migrator"
)

type atlasMigrateSetOptions struct {
	url             string
	dir             string
	dirFormat       string
	atlasEnv        string
	revisionsSchema string
}

type atlasMigrateSetPreparation struct {
	options atlasMigrateSetOptions
	dir     atlasargs.LocalDir
	project atlasProject
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

func runAtlasMigrateSet(
	cmd *cobra.Command,
	opts atlasMigrateSetOptions,
	args []string,
) (runErr error) {
	prepared, err := prepareAtlasMigrateSet(cmd, opts)
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	defer closeAtlasProject(&prepared.project, &runErr)
	opts = prepared.options
	if opts.url == "" {
		return failAtlasCommand(cmd, fmt.Errorf("sql/sqlclient: missing driver. See: https://atlasgo.io/url"))
	}
	source, err := prepared.project.captureLocal(prepared.dir)
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("atlas migrate set --dir: %w", err))
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("error connecting to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	// Atlas validates the environment, migration directory, and database
	// connection before the positional version. Binary compatibility tests pin
	// this otherwise surprising diagnostic and side-effect order.
	if err := atlasMigrateSetExactArgs(cmd, args); err != nil {
		return err
	}
	version, err := parseAtlasMigrateSetVersion(args[0])
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	result, err := atlasmigrate.Set(cmd.Context(), conn, version, atlasmigrate.SetOptions{
		Dir:             source.Display,
		FS:              source.FileSystem,
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
) (
	prepared atlasMigrateSetPreparation,
	returnErr error,
) {
	mode := ignoreMissingEnvSelection
	if !cmd.Flags().Changed("url") {
		mode = reportMissingEnvSelection
	}
	project, loaded, err := openAtlasProjectForCommand(cmd, mode)
	if err != nil {
		return atlasMigrateSetPreparation{}, err
	}
	prepared.project = project
	defer closeAtlasProjectOnError(&prepared.project, &returnErr)
	projectCfg := project.Config
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
		return prepared, fmt.Errorf("migrations directory is required")
	}
	format, err := atlasMigrateDirFormatValue(opts.dirFormat)
	if err != nil {
		return prepared, fmt.Errorf("atlas migrate set --dir-format: %w", err)
	}
	if format != atlasDirFormatDefault {
		return prepared, fmt.Errorf("atlas migrate set --dir-format: expected atlas")
	}

	var localDir atlasargs.LocalDir
	if loaded &&
		!cmd.Flags().Changed("dir") &&
		projectCfg.StringValue(projectconfig.StringMigrationDir).Present {
		localDir, err = project.localDirWithQuery(opts.dir)
	} else {
		localDir, err = atlasargs.ParseLocalDir(opts.dir)
	}
	if err != nil {
		return prepared, fmt.Errorf("atlas migrate set --dir: %w", err)
	}
	if len(localDir.Query) > 0 {
		return prepared,
			fmt.Errorf("atlas migrate set --dir: migration directory URL query parameters are not supported for this command")
	}
	// Preserve Atlas-compatible directory diagnostics; the subsequent
	// CaptureLocal call remains the authoritative rooted read and rejects any
	// path changed after this check.
	info, err := project.statLocalDir(localDir)
	if err != nil {
		return prepared, fmt.Errorf("sql/migrate: %w", err)
	}
	if !info.IsDir() {
		return prepared, fmt.Errorf("sql/migrate: %q is not a dir", localDir.Path)
	}
	prepared.options = opts
	prepared.dir = localDir
	return prepared, nil
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
