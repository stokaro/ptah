package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasreport"
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

func runAtlasMigrateStatus(
	cmd *cobra.Command,
	opts atlasMigrateStatusOptions,
) (runErr error) {
	formatOutput := cmd.Flags().Changed("format")
	mode := ignoreMissingEnvSelection
	if needsAtlasMigrateStatusConfig(cmd) {
		mode = reportMissingEnvSelection
	}
	project, loaded, err := openAtlasProjectForCommand(cmd, mode)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer closeAtlasProject(&project, &runErr)
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
		formatValue := projectCfg.StringValue(projectconfig.StringFormatMigrateStatus)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatOutput = formatOutput || formatValue.Present
	}
	if err := validateAtlasMigrateStatusOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The flag value is validated here, before --format and before --dir is
	// parsed, because that is where it was validated when only the Atlas layout
	// was accepted: moving the whole resolution below --dir would change which
	// diagnostic an invocation carrying two bad values prints. The query
	// spelling cannot be resolved yet — it lives in --dir — so this pass sees
	// the configured value alone and the two are combined below.
	if _, err := resolveAtlasRevisionDirFormat("status", opts.dirFormat, nil); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if formatOutput {
		if err := validateAtlasMigrateStatusFormat(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := atlasreport.ValidateMigrateStatusTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
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
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir: %w", err))
	}
	format, err := resolveAtlasRevisionDirFormat("status", opts.dirFormat, localDir.Query)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	source, err := project.captureLocal(localDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir: %w", err))
	}
	captured, err := captureAtlasRevisionSource(source.FileSystem, format)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir: %w", err))
	}
	// Integrity gate (#974). Reporting status on a directory whose atlas.sum is
	// missing or stale is the most misleading failure of the set: a removed
	// migration reports "Database is up to date". Measured against the pinned
	// community binary v1.3.0, `migrate status` refuses such a directory exactly
	// as `migrate validate` and `migrate apply` do.
	//
	// Placement is measured, not stylistic. The refusal precedes the database
	// connection (it is emitted even when --url is unreachable) and it covers a
	// directory resolved from atlas.hcl as well as one named by --dir, which is
	// why it sits here rather than beside the flag parsing.
	//
	// Both branches are reachable since #1002: a foreign layout is gated over
	// the file set atlas.sum covers for THAT layout, before it is converted.
	//
	// Returned bare on purpose — cmdutil.Fail would prepend `error: ` and move
	// the message off the stream the community binary writes it to.
	if err := verifyAtlasApplyChecksum(cmd, captured.gateFS(), format); err != nil {
		return err
	}
	dir := source.Display
	migrationFS, err := captured.migrationFS(dir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir: %w", err))
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
		FS:              migrationFS,
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
			FS:               migrationFS,
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
