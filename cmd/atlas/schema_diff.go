package atlas

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
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
format.schema.diff, and supported diff policy values. Include filters and Atlas
Cloud web output are explicit follow-up gaps.`,
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
	flags.StringArray("include", nil, "Schema objects to include in diffing")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaDiff(cmd *cobra.Command, opts atlasSchemaDiffOptions) error {
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
		opts.devURL = dbcli.EffectiveString(cmd, "dev-url", opts.devURL, projectCfg.DevURL)
		opts.exclude = effectiveAtlasExclude(cmd, opts.exclude, projectCfg)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, projectCfg.Format.Schema.Diff)
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
	if len(opts.schemas) > 0 {
		return fmt.Errorf("atlas schema diff accepts --schema, but Ptah only supports local schema files for this command yet")
	}
	if values, err := cmd.Flags().GetStringArray("include"); err == nil && len(values) > 0 {
		return fmt.Errorf("atlas schema diff accepts --include, but Ptah only supports local schema files for this command yet")
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
