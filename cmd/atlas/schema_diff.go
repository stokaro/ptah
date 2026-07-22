package atlas

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/schemafile"
)

type atlasSchemaDiffOptions struct {
	fromURLs []string
	toURLs   []string
	devURL   string
	envName  string
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
schema state. This implementation currently supports local file:// schema files
with .hcl, .yaml, .yml, or .sql extensions. When --env is set, the selected
atlas.hcl env can provide schema.src, dev, exclude, schema.mode,
format.schema.diff, and supported diff policy values. Database URLs, migration
directory URLs, Atlas project env:// URLs, include filters, and Atlas Cloud web
output are explicit follow-up gaps.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaDiff(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.fromURLs, "from", nil, "Current schema state URL")
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema state URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to choose the SQL dialect for local schema files")
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from diffing")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.StringArray("schema", nil, "Schemas to inspect when a database URL is used")
	flags.StringArray("include", nil, "Schema objects to include in diffing")
	flags.BoolP("web", "w", false, "Visualize the schema diff on Atlas Cloud")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaDiff(cmd *cobra.Command, opts atlasSchemaDiffOptions) error {
	policy := atlasschema.DiffPolicy{}
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd, opts.envName)
	if needsAtlasSchemaDiffConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd, opts.envName)
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
	format := atlasreport.NormalizeSchemaDiffFormat(opts.format)
	if err := atlasreport.ValidateSchemaDiffTemplate(format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := validateAtlasSchemaDiffOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	report, err := atlasschema.DiffLocalFiles(atlasschema.DiffOptions{
		FromURLs: opts.fromURLs,
		ToURLs:   opts.toURLs,
		DevURL:   opts.devURL,
		Exclude:  opts.exclude,
		Policy:   policy,
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

func validateAtlasSchemaDiffOptions(cmd *cobra.Command, opts atlasSchemaDiffOptions) error {
	if len(opts.fromURLs) == 0 {
		return fmt.Errorf("--from is required")
	}
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	if web, err := cmd.Flags().GetBool("web"); err == nil && web {
		return fmt.Errorf("atlas schema diff accepts --web, but Ptah does not implement Atlas Cloud visualization")
	}
	for _, name := range []string{"schema", "include"} {
		if values, err := cmd.Flags().GetStringArray(name); err == nil && len(values) > 0 {
			return fmt.Errorf("atlas schema diff accepts --%s, but Ptah only supports local schema files for this command yet", name)
		}
	}
	if err := ensureLocalSchemaURLs("--from", opts.fromURLs); err != nil {
		return err
	}
	return ensureLocalSchemaURLs("--to", opts.toURLs)
}

func ensureLocalSchemaURLs(flag string, urls []string) error {
	for _, rawURL := range urls {
		if _, err := schemafile.LocalFilePath(rawURL); err != nil {
			return fmt.Errorf("%s %q: %w", flag, rawURL, err)
		}
	}
	return nil
}
