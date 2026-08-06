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
	// The scheme refusal runs before everything this command body validates,
	// and the position is measured rather than chosen. On the pinned community
	// binary v1.3.0, `migrate status --dir mig` beats a missing --url, an
	// unknown --dir-format and a malformed --format template with
	// `missing scheme for dir url. Did you mean "file://mig"?`; the control
	// `--dir file://mig --dir-format nosuchfmt` prints `unknown dir format
	// "nosuchfmt"`, so the dir-format diagnostic is reachable and only the
	// scheme outranks it. Ptah answered `database URL is required` here.
	//
	// Only a command-line --dir is gated; a directory named by atlas.hcl is out
	// of scope for stokaro/ptah#1186.
	dirFromProject := loaded &&
		!cmd.Flags().Changed("dir") &&
		projectCfg.StringValue(projectconfig.StringMigrationDir).Present
	if !dirFromProject {
		if err := atlasargs.RequireDirScheme(opts.dir); err != nil {
			return cmdutil.Fail(cmd, err)
		}
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
	if _, err := resolveAtlasVerbDirFormat("status", opts.dirFormat, nil); err != nil {
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
	if dirFromProject {
		localDir, err = project.localDirWithQuery(opts.dir)
	} else {
		// The scheme was already required above, where the measured ordering
		// puts it.
		localDir, err = atlasargs.ParseLocalDir(opts.dir)
	}
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir: %w", err))
	}
	format, err := resolveAtlasVerbDirFormat("status", opts.dirFormat, localDir.Query)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	source, err := project.captureLocal(localDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate status --dir: %w", err))
	}
	captured, err := captureAtlasDirSource(source.FileSystem, format)
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
	reportOpts := atlasreport.MigrateStatusOptions{
		Driver:           conn.Info().Dialect,
		URL:              opts.url,
		Dir:              atlasStatusDirURL(opts.dir),
		FS:               migrationFS,
		Status:           result.Status,
		AppliedRevisions: result.AppliedRevisions,
	}
	if formatOutput {
		if err := atlasreport.WriteMigrateStatusFormat(cmd.OutOrStdout(), opts.format, reportOpts); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		return nil
	}
	if err := writeAtlasMigrateStatusDefault(cmd, reportOpts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
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

// writeAtlasMigrateStatusDefault writes the mirrored Atlas status report
// (stokaro/ptah#1102).
//
// This verb is the one a deploy pipeline parses with a machine rather than
// reads, so its field names, sentinel strings and value encodings are the
// interface, not a wording preference — separate from the deliberate prose
// divergence settled on `migrate lint` in #1062/#1078. The block Ptah used to
// print here (`=== MIGRATION STATUS ===`, `Current Version: 0`, `Status:
// Database is up to date`) shared no line with the community binary, so
// `grep -q 'Migration Status: OK'` as a deploy gate never fired, `-- Current
// Version:` matched nothing, and `Next Version:` had no counterpart at all —
// while both binaries exited 0, so nothing caught it.
//
// Native `ptah migrations status` deliberately keeps its own block, including
// its own counts and its `Dirty Migration:` line: the two surfaces are allowed
// to differ and only the compat one is a contract. `--format` is not a
// substitute either, because a caller has to already know it is not talking to
// the binary being mirrored in order to pass one.
//
// The rendering lives in internal/atlasreport beside the `--format` model so
// the two paths agree on Current, Next, the file counts and the failure by
// construction rather than by two implementations staying in step.
func writeAtlasMigrateStatusDefault(cmd *cobra.Command, opts atlasreport.MigrateStatusOptions) error {
	return atlasreport.WriteMigrateStatusText(cmd.OutOrStdout(), opts)
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
