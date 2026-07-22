package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
)

type atlasSchemaInspectOptions struct {
	url     string
	devURL  string
	schemas []string
	exclude []string
	include []string
	format  string
}

func newAtlasSchemaInspectCommand() *cobra.Command {
	opts := atlasSchemaInspectOptions{}
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect a database schema",
		Long: `Atlas OSS ` + "`atlas schema inspect`" + ` command path.

Inspects a live database from --url and writes Atlas-shaped schema output to
stdout without Ptah status banners. The default output is Atlas HCL. SQL output
is supported with --format sql or --format '{{ sql . }}'. JSON output and
custom Go templates are supported through the same --format flag. Include/exclude
filters, split/write templates, and Atlas dev-database inference remain explicit
follow-up gaps.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaInspect(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to inspect")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used by Atlas for inference")
	flags.StringArrayVar(&opts.schemas, "schema", nil, "Schema to inspect")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from inspection")
	flags.StringArrayVar(&opts.include, "include", nil, "Schema objects to include in inspection")
	flags.StringVar(&opts.format, "format", "", "Output format or Go template: hcl, sql, json, or custom template")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaInspect(cmd *cobra.Command, opts atlasSchemaInspectOptions) error {
	format, err := atlasreport.NormalizeSchemaInspectFormat(opts.format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := atlasreport.ValidateSchemaInspectTemplate(format); err != nil {
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

	if err := atlasurl.ValidateDialectMatch(opts.devURL, conn.Info().Dialect); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	schema, err := dbschema.ReadSchemaWithSchemas(conn, parseAtlasSchemaInspectSchemas(opts.schemas))
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("read database schema: %w", err))
	}
	dbsch := dbschematogo.ConvertDBSchemaToGoSchema(schema)
	rendered, err := atlasreport.RenderSchemaInspectFormat(format, atlasreport.NewSchemaInspectReport(
		dbsch,
		schema,
		conn.Info(),
		cmd.ErrOrStderr(),
	))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fmt.Fprint(cmd.OutOrStdout(), rendered)
	return nil
}

func validateAtlasSchemaInspectOptions(opts atlasSchemaInspectOptions) error {
	if strings.TrimSpace(opts.url) == "" {
		return fmt.Errorf("--url is required")
	}
	if len(opts.exclude) > 0 {
		return fmt.Errorf("atlas schema inspect accepts --exclude, but Ptah does not implement its behavior yet")
	}
	if len(opts.include) > 0 {
		return fmt.Errorf("atlas schema inspect accepts --include, but Ptah does not implement its behavior yet")
	}
	return nil
}

func parseAtlasSchemaInspectSchemas(values []string) []string {
	var schemas []string
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			if schema := strings.TrimSpace(part); schema != "" {
				schemas = append(schemas, schema)
			}
		}
	}
	return schemas
}
