package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasschema"
)

type atlasSchemaInspectOptions struct {
	url     string
	devURL  string
	schemas []string
	exclude []string
	format  string
}

func newAtlasSchemaInspectCommand() *cobra.Command {
	opts := atlasSchemaInspectOptions{}
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect a database schema",
		Long: `Atlas OSS ` + "`atlas schema inspect`" + ` command path.

Inspects a live database from --url and writes Atlas-compatible schema output to
stdout without Ptah status banners. The default output is HCL. SQL output is
supported with --format sql or --format '{{ sql . }}'. JSON output and custom
Go templates are supported through the same --format flag, including basic
` + "`{{ hcl . | split | write \"schema\" }}`" + ` and
` + "`{{ sql . | split | write \"schema\" }}`" + ` exports. The OSS --exclude
filter supports resource-level live database inspection filters. Field-level
exclude selectors beyond the supported extension version selector, file-backed
inspection, include filtering, and Atlas dev-database inference remain explicit
follow-up gaps.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaInspect(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to inspect")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used by Atlas for inference")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schema to inspect")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from inspection")
	flags.StringVar(&opts.format, "format", "", "Output format or Go template: hcl, sql, json, or custom template")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaInspect(cmd *cobra.Command, opts atlasSchemaInspectOptions) error {
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if needsAtlasSchemaInspectConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		opts.url = dbcli.EffectiveString(cmd, "url", opts.url, projectCfg.DatabaseURL)
		opts.devURL = dbcli.EffectiveString(cmd, "dev-url", opts.devURL, projectCfg.DevURL)
		opts.exclude = effectiveAtlasExclude(cmd, opts.exclude, projectCfg)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, projectCfg.Format.Schema.Inspect)
	}
	format, err := atlasschema.NormalizeInspectFormat(opts.format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := validateAtlasSchemaInspectOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	rendered, err := atlasschema.Inspect(conn, atlasschema.InspectOptions{
		DevURL:      opts.devURL,
		Schemas:     opts.schemas,
		Exclude:     opts.exclude,
		Format:      format,
		Diagnostics: cmd.ErrOrStderr(),
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
