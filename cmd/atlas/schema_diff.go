package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

type atlasSchemaDiffOptions struct {
	fromURLs []string
	toURLs   []string
	devURL   string
	schemas  []string
	include  []string
	exclude  []string
	format   string
	policy   atlascompatpolicy.Policy
}

func newAtlasSchemaDiffCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	opts := atlasSchemaDiffOptions{policy: policy}
	long := `Atlas OSS ` + "`atlas schema diff`" + ` command path.

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
diagnostic. An --include selection that matches nothing on either side leaves
no plan to compare and is refused instead of reporting a synced schema to a CI
check.`
	if !policy.IsStrictCE() {
		long += `

Hosted report output is not implemented. --export is registered and refused:
it selects an exporter declared by an atlas.hcl ` + "`exporter`" + ` block,
which Ptah does not evaluate.`
	}
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Diff desired schema against another schema",
		Long:  long,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if policy.IsStrictCE() && cmd.Flags().Changed("include") {
				return failAtlasStrictCompatGate(cmd, "ptah-compat schema diff --include")
			}
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
	if !policy.IsStrictCE() {
		registerAtlasUIFlag(cmd, atlasSchemaExportFlag())
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgsHint("name the states with --from and --to"))
	return cmd
}

func runAtlasSchemaDiff(cmd *cobra.Command, opts atlasSchemaDiffOptions) error {
	if err := validateAtlasSchemaDiffSQLiteToggle(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	formatConfigured := cmd.Flags().Changed("format")
	policy := atlasschema.DiffPolicy{}
	mode := ignoreMissingEnvSelection
	if needsAtlasSchemaDiffConfig(cmd) {
		mode = reportMissingEnvSelection
	}
	projectCfg, loaded, err := loadAtlasProjectConfigForCommand(cmd, mode)
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
		opts.schemas = effectiveAtlasSchemas(cmd, opts.schemas, projectCfg)
		formatValue := projectCfg.StringValue(projectconfig.StringFormatSchemaDiff)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatConfigured = formatConfigured || formatValue.Present
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	// An exporter is a named format, so selecting one is choosing the template
	// this run renders through (stokaro/ptah#1620).
	exported, exportSelected, err := resolveAtlasExporter(cmd, atlasExportProject{config: projectCfg, loaded: loaded})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if exportSelected {
		opts.format = exported
		formatConfigured = true
	}
	if err := validateAtlasSchemaDiffSQLiteToggle(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// projectToURLs is the desired-state URL list this run took from the
	// project's schema sources, and stays nil when --to supplied it. See
	// [atlasProjectSourceURLs].
	var projectToURLs []string
	if loaded && !cmd.Flags().Changed("to") && len(projectCfg.SchemaSources) > 0 {
		opts.toURLs, err = atlasProjectConfigSchemaURLs(cmd, opts.toURLs)
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("atlas.hcl schema.src: %w", err))
		}
		projectToURLs = opts.toURLs
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
		projectEnv.ProjectSourceURLs = atlasProjectSourceURLs("--to", projectToURLs)
	}
	if err := validateAtlasSchemaDiffOptions(cmd, opts, projectEnv); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	schemaVars, err := atlasVarFlagValues(cmd)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	report, err := atlasschema.Diff(cmd.Context(), atlasschema.DiffOptions{
		FromURLs:    opts.fromURLs,
		ToURLs:      opts.toURLs,
		DevURL:      opts.devURL,
		Exclude:     opts.exclude,
		Schemas:     opts.schemas,
		Include:     opts.include,
		Policy:      policy,
		ProjectEnv:  projectEnv,
		Diagnostics: cmd.ErrOrStderr(),

		IgnoreUnknownHCLNames:     opts.policy.IgnoreUnknownHCLNames(),
		ValidateSchema:            opts.policy.ValidateDesiredSchema,
		ValidateInspectedSchema:   opts.policy.ValidateInspectedSchema,
		ValidateLiveObject:        atlasLiveSchemaObjectValidator(opts.policy),
		ValidateMigrationSource:   opts.policy.MigrationSourceValidator(opts.devURL),
		ValidateLocalSchemaSource: opts.policy.ValidateLocalSchemaSource,
		Vars:                      schemaVars,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := atlasreport.WriteSchemaDiff(cmd.OutOrStdout(), format, report); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return nil
}

func validateAtlasSchemaDiffSQLiteToggle(opts atlasSchemaDiffOptions) error {
	// --dev-url owns the comparison dialect when present. Preserve errors from
	// invalid URLs for the Atlas-compatible validation path below.
	if strings.TrimSpace(opts.devURL) != "" {
		if dialect, err := atlasurl.DialectFromURL(opts.devURL); err == nil {
			return sqlitevirtual.ValidateToggle(dialect)
		}
		return nil
	}
	for _, rawURL := range append(append(make([]string, 0), opts.fromURLs...), opts.toURLs...) {
		if dialect, err := atlasurl.DialectFromURL(rawURL); err == nil && dialect != "" {
			return sqlitevirtual.ValidateToggle(dialect)
		}
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
	// A `--dev-url` naming no driver is refused before the sources are
	// classified, which is where the pinned community binary v1.3.0 refuses it:
	// measured on 2026-08-13, this verb answers `sql/sqlclient: unknown driver
	// "notadriver"` for both database and local-file sources, so unlike `schema
	// inspect` the check is not scoped to what the sources turned out to be. An
	// empty value is left to [atlassource.Set.EnsureDevDatabase] below, whose
	// own refusal is scoped to the sources that need a dev database -- both
	// binaries exit 0 on two database sources with no `--dev-url` at all.
	if err := atlasDevURLDriverDiagnostic(opts.devURL); err != nil {
		return err
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
