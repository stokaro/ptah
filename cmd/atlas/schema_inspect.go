package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/atlassource"
)

type atlasSchemaInspectOptions struct {
	url     string
	devURL  string
	schemas []string
	include []string
	exclude []string
	format  string
}

func newAtlasSchemaInspectCommand() *cobra.Command {
	opts := atlasSchemaInspectOptions{}
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect a database schema",
		Long: `Atlas OSS ` + "`atlas schema inspect`" + ` command path.

Inspects the --url source and writes Atlas-compatible schema output to stdout
without Ptah status banners. The source is a live database URL, a local schema
file (.hcl, .yaml, .yml, or .sql), a migration directory, or an env://
reference into the evaluated atlas.hcl environment. Non-database sources
require --dev-url: the dev database is reset, the source is materialized on
it, and the result is introspected, mirroring Atlas dev-database
normalization.

The default output is HCL. SQL output is supported with --format sql or
--format '{{ sql . }}', JSON with --format json, and custom Go templates
through the same --format flag. Split/write exports support the documented
Atlas split strategies — per object (default), ` + "`split \"schema\"`" + `,
and ` + "`split \"type\"`" + ` with an optional file extension — through
` + "`{{ hcl . | split | write \"dir\" }}`" + ` and
` + "`{{ sql . | split | write \"dir\" }}`" + `. The OSS --exclude filter
supports resource selectors plus the documented ` + "`[type=extension].version`" + `
field selector; unsupported selector forms fail before any database is
contacted.

--include positively selects which top-level resources the inspected output
keeps, using the same selectors as ` + "`schema apply`" + ` and
` + "`schema diff`" + `: --schema names the schema universe, --include picks
resources inside it, and --exclude subtracts from the result. Child resources
(columns, indexes, constraints, triggers, policies, grants) ride along with
their parent and cannot be selected on their own, in either the
` + "`[type=column]`" + ` or the ` + "`table.column`" + ` spelling. A selection
that keeps an object whose dependency it dropped is refused rather than
rendered, so inspected output never references an object it omitted. The flag
is absent from Atlas CE, which rejects it as an unknown flag on this command.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaInspect(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL, schema file, migration directory, or env:// reference to inspect")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to evaluate non-database inspection sources")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schema to inspect")
	flags.StringArrayVar(&opts.include, "include", nil, "Schema objects to include in inspection")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from inspection")
	flags.StringVar(&opts.format, "format", "", "Output format or Go template: hcl, sql, json, or custom template")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaInspect(cmd *cobra.Command, opts atlasSchemaInspectOptions) error {
	formatConfigured := cmd.Flags().Changed("format")
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if needsAtlasSchemaInspectConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	projectEnv := atlassource.ProjectEnv{}
	if loaded {
		opts.url = dbcli.EffectiveString(
			cmd,
			"url",
			opts.url,
			projectCfg.StringValue(projectconfig.StringDatabaseURL),
		)
		opts.devURL = dbcli.EffectiveString(
			cmd,
			"dev-url",
			opts.devURL,
			projectCfg.StringValue(projectconfig.StringDevURL),
		)
		opts.exclude = effectiveAtlasExclude(cmd, opts.exclude, projectCfg)
		formatValue := projectCfg.StringValue(projectconfig.StringFormatSchemaInspect)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatConfigured = formatConfigured || formatValue.Present
		projectEnv, err = atlasSourceProjectEnv(cmd, projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if formatConfigured && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	if _, err := atlasschema.NormalizeInspectFormat(opts.format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := validateAtlasSchemaInspectOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	rendered, err := atlasschema.InspectSource(cmd.Context(), atlasschema.InspectSourceOptions{
		URL:            opts.url,
		DevURL:         opts.devURL,
		Schemas:        opts.schemas,
		Include:        opts.include,
		Exclude:        opts.exclude,
		Format:         opts.format,
		Diagnostics:    cmd.ErrOrStderr(),
		ProjectEnv:     projectEnv,
		ConnectTimeout: dbcli.DefaultConnectTimeout,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fmt.Fprint(cmd.OutOrStdout(), rendered)
	return nil
}

func needsAtlasSchemaInspectConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("url")
}

func validateAtlasSchemaInspectOptions(opts atlasSchemaInspectOptions) error {
	if strings.TrimSpace(opts.url) == "" {
		return fmt.Errorf("--url is required")
	}
	return nil
}
