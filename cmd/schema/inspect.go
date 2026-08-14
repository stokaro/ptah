package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemaartifact"
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

// inspectOCIMaterializedName is the file the pulled schema artifact is written
// to before it is inspected.
//
// The extension is load-bearing, not decorative: the inspection source is a
// path handed to the shared resolver, which decides what a source IS from its
// extension. A schema artifact carries canonical HCL — that is what
// `ptah schema push` publishes and what `ptah schema pull` writes — so the
// materialized file has to say .hcl or the resolver refuses a file it can
// read.
const inspectOCIMaterializedName = "schema.hcl"

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
	plainHTTP      bool
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
own: the [type=column] spelling fails before the database is contacted. A
positional spelling such as table.column is not refused on its shape, because
it is indistinguishable from a table literally named that; an identifier
holding a dot can also be named as main."my.table" or a\.b\.c. An --include
selection that matches nothing renders no objects and keeps exit status 0, but
reports the empty selection on standard error. A selection that keeps an
object whose dependency it dropped is refused rather than rendered, so
inspected output never references an object it omitted.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaInspect(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, inspectDBURLFlag, "", "Live database URL to inspect")
	flags.StringVar(&opts.schemaFile, inspectSchemaFileFlag, "", "Schema file to inspect: a local .hcl, .yaml, .yml, or .sql file, or an oci:// schema artifact; requires --dev-url")
	flags.StringVar(&opts.migrationsDir, inspectMigrationsDirFlag, "", "Atlas-format migration directory to inspect; requires --dev-url")
	flags.StringVar(&opts.devURL, inspectDevURLFlag, "", "Dev database URL used to evaluate non-database inspection sources; it is reset destructively")
	dbcli.RegisterURLScopedSchemasFlag(flags, &opts.schemas)
	flags.StringArrayVar(&opts.include, inspectIncludeFlag, nil, "Schema objects to include in inspection (Atlas-style selectors)")
	flags.StringArrayVar(&opts.exclude, inspectExcludeFlag, nil, "Schema objects to exclude from inspection (Atlas-style selectors)")
	flags.StringVar(&opts.format, inspectFormatFlag, "hcl", "Output format: hcl, sql, or json")
	flags.StringVar(&opts.outDir, inspectOutDirFlag, "", "Directory the inspected schema is exported into as files (hcl and sql formats only)")
	flags.StringVar(&opts.split, inspectSplitFlag, "", "File grouping for --out-dir: object (default), schema, or type")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
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

	// An oci:// --schema-file is resolved here, into a real local file, before
	// anything classifies the value. See materializeOCISchemaFile for why the
	// shared classifier is deliberately left alone.
	if materialized, cleanup, err := materializeOCISchemaFile(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	} else if cleanup != nil {
		defer cleanup()
		opts.schemaFile = materialized
	}

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

// materializeOCISchemaFile pulls an oci:// --schema-file to a local canonical
// HCL file and returns its path plus the cleanup that removes it. A value that
// is not an oci:// reference returns ("", nil, nil), which is the caller's
// signal that nothing was materialized.
//
// # Why here, and not in the shared classifier
//
// Every sibling command that accepts an oci:// --schema-file resolves it
// through [go.5x5.cz/ptah/internal/schemaload], which parses the artifact into
// a desired schema directly. `schema inspect` cannot: it does not parse its
// source at all. It hands the source to
// [go.5x5.cz/ptah/internal/atlasschema.InspectSource], which materializes it on
// a destructively reset --dev-url database and introspects the result, so the
// output is normalized by a real database of the target dialect. That is the
// whole point of the verb, and it is why the refusal was not an oversight: the
// value goes to [go.5x5.cz/ptah/internal/atlassource.Classify], which names no
// oci source kind.
//
// Teaching Classify one would have been the smaller diff and the wrong change.
// That function is shared with the Atlas-compatible surface: thirteen non-test
// call sites live under cmd/atlas and internal/atlasschema, and
// cmd/atlas/compat_url_diagnostic.go re-words exactly its unsupported-scheme
// verdict — its own comment says it does so "so a scheme added to
// atlassource.Classify later is recognized here without this file being
// edited". Adding an oci branch would therefore stop `ptah-compat schema
// inspect --url oci://...` refusing, and the pinned community binary refuses
// that reference at exit 1 (`sql/sqlclient: unknown driver "oci"`). Widening
// the classifier would hand a compatibility-policy (a) violation to a surface
// this issue never mentioned.
//
// So the scheme is resolved before classification instead, on the one native
// verb that wants it, through the same schemaartifact.PullToFile that
// `ptah schema pull` uses. The bytes inspected are byte-for-byte the bytes
// `schema pull` would have written, because it is not a second implementation
// of the pull.
func materializeOCISchemaFile(cmd *cobra.Command, opts schemaInspectOptions) (string, func(), error) {
	reference := strings.TrimSpace(opts.schemaFile)
	if !strings.HasPrefix(reference, ociartifact.Scheme) {
		return "", nil, nil
	}
	dir, err := os.MkdirTemp("", "ptah-inspect-oci-")
	if err != nil {
		return "", nil, fmt.Errorf("create temporary directory for %s: %w", reference, err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	_, output, err := schemaartifact.PullToFile(
		cmd.Context(),
		reference,
		filepath.Join(dir, inspectOCIMaterializedName),
		opts.plainHTTP,
	)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	return output, cleanup, nil
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
