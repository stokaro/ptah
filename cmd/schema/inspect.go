package schema

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/atlassource"
	"github.com/stokaro/ptah/internal/pathguard"
)

const (
	inspectDBURLFlag         = "db-url"
	inspectSchemaFileFlag    = "schema-file"
	inspectMigrationsDirFlag = "migrations-dir"
	inspectDevURLFlag        = "dev-url"
	inspectIncludeFlag       = "include"
	inspectExcludeFlag       = "exclude"
	inspectFormatFlag        = "format"
	inspectOutDirFlag        = "out-dir"
	inspectSplitFlag         = "split"
)

type schemaInspectOptions struct {
	dbURL          string
	schemaFile     string
	migrationsDir  string
	devURL         string
	schemas        string
	include        []string
	exclude        []string
	format         string
	outDir         string
	split          string
	connectTimeout string
	configPath     string
	envName        string
}

func newSchemaInspectCommand() *cobra.Command {
	opts := schemaInspectOptions{}
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect a schema as machine-clean HCL, SQL, or JSON",
		Long: `Inspect a schema source and write machine-clean output to stdout, without
status banners: pipe it into files, diffs, or other tools.

The source is a live database (--db-url), a local schema file (--schema-file:
.hcl, .yaml, .yml, or .sql), or an Atlas-format migration directory
(--migrations-dir). Non-database sources require --dev-url: the dev database
is reset destructively, the source is materialized on it (schema files
executed, migration directories replayed), and the result is introspected so
the output is normalized by a real database of the target dialect.

The default output is HCL; --format selects hcl, sql, or json. With --out-dir
the inspected schema is exported as files instead of one stream: one file per
object by default, or grouped with --split schema|type.

--schemas, --include, and --exclude select what is inspected, in that order:
--schemas names the schema universe, --include picks top-level resources
inside it with Atlas-style selectors, and --exclude subtracts from the result.
Child resources ride along with their parent and cannot be selected on their
own: the [type=column] and literal-dot table.column spellings both fail before
the database is contacted, while glob metacharacters match a dot and escape
that check. A selection that keeps an object whose dependency it dropped is
refused rather than rendered, so inspected output never references an object
it omitted.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaInspect(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, inspectDBURLFlag, "", "Live database URL to inspect")
	flags.StringVar(&opts.schemaFile, inspectSchemaFileFlag, "", "Local schema file to inspect (.hcl, .yaml, .yml, or .sql); requires --dev-url")
	flags.StringVar(&opts.migrationsDir, inspectMigrationsDirFlag, "", "Atlas-format migration directory to inspect; requires --dev-url")
	flags.StringVar(&opts.devURL, inspectDevURLFlag, "", "Dev database URL used to evaluate non-database inspection sources; it is reset destructively")
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
	flags.StringArrayVar(&opts.include, inspectIncludeFlag, nil, "Schema objects to include in inspection (Atlas-style selectors)")
	flags.StringArrayVar(&opts.exclude, inspectExcludeFlag, nil, "Schema objects to exclude from inspection (Atlas-style selectors)")
	flags.StringVar(&opts.format, inspectFormatFlag, "hcl", "Output format: hcl, sql, or json")
	flags.StringVar(&opts.outDir, inspectOutDirFlag, "", "Directory the inspected schema is exported into as files (hcl and sql formats only)")
	flags.StringVar(&opts.split, inspectSplitFlag, "", "File grouping for --out-dir: object (default), schema, or type")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	cmd.MarkFlagsMutuallyExclusive(inspectDBURLFlag, inspectSchemaFileFlag, inspectMigrationsDirFlag)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runSchemaInspect(cmd *cobra.Command, opts schemaInspectOptions) error {
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.schemaFile == "" && opts.migrationsDir == "" {
		opts.dbURL = dbcli.EffectiveString(
			cmd,
			inspectDBURLFlag,
			opts.dbURL,
			projectCfg.StringValue(projectconfig.StringDatabaseURL),
		)
	}
	opts.devURL = dbcli.EffectiveString(
		cmd,
		inspectDevURLFlag,
		opts.devURL,
		projectCfg.StringValue(projectconfig.StringDevURL),
	)

	sourceURL, err := resolveInspectSource(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	format, err := resolveInspectFormat(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(
		dbcli.EffectiveString(
			cmd,
			dbcli.ConnectTimeoutFlagName,
			opts.connectTimeout,
			projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout),
		))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	rendered, err := atlasschema.InspectSource(cmd.Context(), atlasschema.InspectSourceOptions{
		URL:            sourceURL,
		DevURL:         opts.devURL,
		Schemas:        dbcli.ParseSchemas(opts.schemas),
		Include:        opts.include,
		Exclude:        opts.exclude,
		Format:         format,
		Diagnostics:    cmd.ErrOrStderr(),
		ConnectTimeout: connectTimeout,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fmt.Fprint(cmd.OutOrStdout(), rendered)
	return nil
}

// resolveInspectSource maps the native source flags onto one inspection URL.
func resolveInspectSource(opts schemaInspectOptions) (string, error) {
	switch {
	case strings.TrimSpace(opts.migrationsDir) != "":
		dir, err := pathguard.ResolveCLIPath(opts.migrationsDir)
		if err != nil {
			return "", fmt.Errorf("invalid migrations directory: %w", err)
		}
		if err := cmdutil.StatDir(dir); err != nil {
			return "", err
		}
		source, err := atlassource.Classify(dir)
		if err != nil {
			return "", fmt.Errorf("--%s %q: %w", inspectMigrationsDirFlag, opts.migrationsDir, err)
		}
		if source.Kind != atlassource.KindMigrationDir {
			return "", fmt.Errorf(
				"--%s %q is not recognized as a migration directory (missing atlas.sum); only Atlas-format directories can be inspected",
				inspectMigrationsDirFlag, opts.migrationsDir)
		}
		return dir, nil
	case strings.TrimSpace(opts.schemaFile) != "":
		return strings.TrimSpace(opts.schemaFile), nil
	case strings.TrimSpace(opts.dbURL) != "":
		return strings.TrimSpace(opts.dbURL), nil
	default:
		return "", fmt.Errorf(
			"an inspection source is required: pass --%s, --%s, or --%s",
			inspectDBURLFlag, inspectSchemaFileFlag, inspectMigrationsDirFlag)
	}
}

// resolveInspectFormat maps the native format flags onto the shared inspect
// renderer. Plain hcl/sql/json stream to stdout; --out-dir composes the
// split/write export the renderer implements.
func resolveInspectFormat(opts schemaInspectOptions) (string, error) {
	format := strings.ToLower(strings.TrimSpace(opts.format))
	switch format {
	case "", "hcl":
		format = "hcl"
	case "sql", "json":
	default:
		return "", fmt.Errorf("unsupported --%s %q: expected hcl, sql, or json", inspectFormatFlag, opts.format)
	}

	split := strings.ToLower(strings.TrimSpace(opts.split))
	outDir := strings.TrimSpace(opts.outDir)
	if outDir == "" {
		if split != "" {
			return "", fmt.Errorf("--%s requires --%s", inspectSplitFlag, inspectOutDirFlag)
		}
		return format, nil
	}
	if format == "json" {
		return "", fmt.Errorf("--%s supports the hcl and sql formats only", inspectOutDirFlag)
	}
	splitArg := ""
	switch split {
	case "", "object":
	case "schema", "type":
		splitArg = fmt.Sprintf(" %q", split)
	default:
		return "", fmt.Errorf("unsupported --%s %q: expected object, schema, or type", inspectSplitFlag, opts.split)
	}
	return fmt.Sprintf("{{ %s . | split%s | write %q }}", format, splitArg, outDir), nil
}
