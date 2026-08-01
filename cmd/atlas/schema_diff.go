package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/atlasfilter"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/atlassource"
	"github.com/stokaro/ptah/internal/schemafile"
)

type atlasSchemaDiffOptions struct {
	fromURLs []string
	toURLs   []string
	devURL   string
	schemas  []string
	include  []string
	exclude  []string
	format   string
}

func newAtlasSchemaDiffCommand() *cobra.Command {
	opts := atlasSchemaDiffOptions{}
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Diff desired schema against another schema",
		Long: `Atlas OSS ` + "`atlas schema diff`" + ` command path.

Calculates SQL statements that migrate the --from schema state to the --to
schema state. Each side accepts local file:// schema files with .hcl, .yaml,
.yml, or .sql extensions, one directly connectable database URL whose live
schema is introspected, one migration directory (a file:// directory containing
atlas.sum) replayed on the required --dev-url dev database, or one
env://<attribute> reference (src, schema.src, url, dev, migration.dir) resolved
through the evaluated atlas.hcl env. All URLs of one flag must be one source
kind. The SQL dialect is pinned by --dev-url first, then by --from and --to
database URLs; local schema files alone still require --dev-url. Unsupported
schemes such as atlas:// fail during validation. When --env is set, the
selected atlas.hcl env can provide schema.src, dev, exclude, schema.mode,
format.schema.diff, and supported diff policy values. --schema and --include
positively select what both comparison sides see: --schema names define the
schema universe, --include selectors pick top-level resources inside it, and
--exclude plus env.schema.mode subtract from the result. A selected object
that depends on an unselected object refuses the diff with an explicit
diagnostic, and a selection that matches nothing reports synced schemas.
Atlas Cloud web output is an explicit follow-up gap.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaDiff(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVarP(&opts.fromURLs, atlasFromFlagName, atlasFromFlagShorthand, nil, "Current schema state URL")
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema state URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to choose the SQL dialect for local schema files")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from diffing")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schemas to diff when a database URL is used")
	flags.StringArrayVar(&opts.include, "include", nil, "Schema objects to include in diffing")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaDiff(cmd *cobra.Command, opts atlasSchemaDiffOptions) error {
	formatConfigured := cmd.Flags().Changed("format")
	policy := atlasschema.DiffPolicy{}
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if needsAtlasSchemaDiffConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		opts.toURLs = effectiveStringArray(cmd, "to", opts.toURLs, projectCfg.SchemaSources)
		opts.devURL = dbcli.EffectiveString(
			cmd,
			"dev-url",
			opts.devURL,
			projectCfg.StringValue(projectconfig.StringDevURL),
		)
		opts.exclude = effectiveAtlasExclude(cmd, opts.exclude, projectCfg)
		formatValue := projectCfg.StringValue(projectconfig.StringFormatSchemaDiff)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatConfigured = formatConfigured || formatValue.Present
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if loaded && !cmd.Flags().Changed("to") && len(projectCfg.SchemaSources) > 0 {
		opts.toURLs, err = atlasProjectConfigSchemaURLs(cmd, opts.toURLs)
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("atlas.hcl schema.src: %w", err))
		}
	}
	if loaded && !cmd.Flags().Changed("to") && atlasExternalSchemaConfigured(projectCfg) {
		opts.toURLs = []string{"env://src"}
	}
	if formatConfigured && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	format := atlasreport.NormalizeSchemaDiffFormat(opts.format)
	if err := atlasreport.ValidateSchemaDiffTemplate(format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	projectEnv := atlassource.ProjectEnv{}
	if loaded {
		projectEnv, err = atlasSourceProjectEnv(cmd, projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if err := validateAtlasSchemaDiffOptions(cmd, opts, projectEnv); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	report, err := atlasschema.Diff(cmd.Context(), atlasschema.DiffOptions{
		FromURLs:   opts.fromURLs,
		ToURLs:     opts.toURLs,
		DevURL:     opts.devURL,
		Exclude:    opts.exclude,
		Schemas:    opts.schemas,
		Include:    opts.include,
		Policy:     policy,
		ProjectEnv: projectEnv,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := atlasreport.WriteSchemaDiff(cmd.OutOrStdout(), format, report); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return nil
}

func needsAtlasSchemaDiffConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("to")
}

func validateAtlasSchemaDiffOptions(
	cmd *cobra.Command,
	opts atlasSchemaDiffOptions,
	projectEnv atlassource.ProjectEnv,
) error {
	if len(opts.fromURLs) == 0 {
		return fmt.Errorf("--from is required")
	}
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	// Malformed or unsupported --include selectors fail before any database
	// is contacted.
	if err := atlasfilter.ValidateIncludeSelectors(opts.include); err != nil {
		return err
	}
	// Classification rejects unsupported schemes and source conflicts, and
	// migration-directory sources require a dev database, before any database
	// is contacted. --from is validated first, then --to.
	fromSet, err := atlassource.ClassifySet("--from", opts.fromURLs, projectEnv)
	if err != nil {
		return err
	}
	if err := fromSet.EnsureDevDatabase(opts.devURL); err != nil {
		return err
	}
	toSet, err := atlassource.ClassifySet("--to", opts.toURLs, projectEnv)
	if err != nil {
		return err
	}
	return toSet.EnsureDevDatabase(opts.devURL)
}

func ensureLocalSchemaURLs(flag string, urls []string) error {
	for _, rawURL := range urls {
		if _, err := schemafile.LocalFilePath(rawURL); err != nil {
			return fmt.Errorf("%s %q: %w", flag, rawURL, err)
		}
	}
	return nil
}
