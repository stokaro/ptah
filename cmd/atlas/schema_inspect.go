package atlas

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
)

type atlasSchemaInspectOptions struct {
	url     string
	devURL  string
	schemas []string
	include []string
	exclude []string
	format  string
	output  string
	policy  atlascompatpolicy.Policy
}

func newAtlasSchemaInspectCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	opts := atlasSchemaInspectOptions{policy: policy}
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

On PostgreSQL, HCL output omits an ` + "`extension`" + `, ` + "`sequence`" + ` or
` + "`policy`" + ` block that nothing else in the document depends on: Atlas CE
refuses a whole schema file that declares any one of them, so emitting one
would make the output unreadable to the tool this binary stands in for. A block
something still NEEDS — a sequence behind a column default, an extension
supplying a column's type — is kept, because a document that references an
object it did not declare is readable by nobody. For an extension the test is
what the catalog says it supplies, not its name: ` + "`isn`" + ` supplies the
type ` + "`isbn`" + `. Every decision is reported on standard error. Set
` + "`PTAH_ATLAS_INSPECT_ALL_BLOCKS=1`" + ` to emit every block Ptah models on
this surface. SQL output keeps them all, and native
` + "`ptah schema inspect`" + ` omits nothing.

The default output is HCL. Use --format '{{ hcl . }}' for explicit rendered
HCL, --format '{{ sql . }}' for rendered SQL, and --format '{{ json . }}' for
rendered JSON. Atlas CE treats the bare values --format hcl, --format sql, and
--format json as literal text; surrounding whitespace preserves those template
bodies byte for byte. Custom Go templates use the same --format flag.
Split/write exports support the documented
Atlas split strategies — per object (default),
` + "`split \"schema\"`" + `,
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
their parent and cannot be selected on their own; the
` + "`[type=column]`" + ` spelling is refused before the database is
contacted. A positional spelling such as ` + "`table.column`" + ` is not
refused on its shape, because it is indistinguishable from a table literally
named that; it is carried to the selection, where an identifier holding a dot
can also be named as ` + "`main.\"my.table\"`" + ` or
` + "`a\\.b\\.c`" + `. An --include selection that matches nothing renders no
objects and keeps exit status 0 — inspection is read-only, and an empty
description of an empty selection is a legitimate answer — but it reports the
empty selection on standard error. A selection that keeps an object whose
dependency it dropped is refused rather than rendered, so inspected output
never references an object it omitted. The flag is absent from Atlas CE, which rejects it as an
unknown flag on this command.

-o/--output writes the rendered schema to a file instead of stdout. The file is
staged beside its destination and published atomically, so a reader either sees
the previous contents or the complete new document, never a partial one. Nothing
is written to stdout on the --output path.

-w/--web is registered and refused: opening a viewer has no local counterpart,
and the ERD itself is available as data through --format '{{ mermaid . }}'.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaInspect(cmd, opts)
		},
	}
	if policy.IsStrictCE() {
		cmd.Long = strictAtlasSchemaInspectLong()
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL, schema file, migration directory, or env:// reference to inspect")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to evaluate non-database inspection sources")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schema to inspect")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from inspection")
	flags.StringVar(&opts.format, "format", "", "Output Go template: exact hcl/sql/json and whitespace-wrapped variants are literal text")
	if !policy.IsStrictCE() {
		flags.StringArrayVar(&opts.include, "include", nil, "Schema objects to include in inspection")
		flags.StringVarP(&opts.output, "output", "o", "", "Write the inspected schema to this file instead of stdout")
		registerAtlasUIFlag(cmd, atlasSchemaWebFlag())
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgsHint("name the database with -u/--url"))
	return cmd
}

func strictAtlasSchemaInspectLong() string {
	return `Atlas OSS ` + "`atlas schema inspect`" + ` command path.

Inspects the --url source and writes Atlas-compatible schema output to stdout
without Ptah status banners. The source is a live database URL, a local schema
file, a migration directory, or an env:// reference. Non-database sources
require --dev-url and are validated before that dev database is reset.

The default output is HCL. Atlas CE treats the bare values --format hcl,
--format sql, and --format json as literal text. Rendered SQL and JSON remain
available through explicit ` + "`{{ sql . }}`" + ` and ` + "`{{ json . }}`" + ` templates.
Strict compatibility refuses the Pro-only hcl, split, and write helpers before
source or database work. The default ptah-compat policy retains all three.`
}

func runAtlasSchemaInspect(cmd *cobra.Command, opts atlasSchemaInspectOptions) error {
	// The refusal lands before any config or database work: a flag Ptah does
	// not implement must not be answered with an inspection that ignored it.
	if err := refuseAtlasUIFlag(cmd, "schema", "inspect", atlasSchemaWebFlag()); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The variable this verb owns is resolved here, before the project file is
	// read and before anything is connected to, so a malformed value is refused
	// on every inspect rather than on the ones whose schema happens to contain a
	// block the default would omit. See stokaro/ptah#1334.
	omitRefusedBlocks, err := atlasInspectOmitsRefusedBlocks()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	formatConfigured := cmd.Flags().Changed("format")
	mode := ignoreMissingEnvSelection
	if needsAtlasSchemaInspectConfig(cmd) {
		mode = reportMissingEnvSelection
	}
	projectCfg, loaded, err := loadAtlasProjectConfigForCommand(cmd, mode)
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
	opts.format = atlasSchemaInspectCompatibilityFormat(opts.format)
	normalizedFormat, err := atlasschema.NormalizeInspectFormat(opts.format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := opts.policy.ValidateSchemaInspectFormat(normalizedFormat); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := validateAtlasSchemaInspectOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	schemaVars, err := atlasVarFlagValues(cmd)
	if err != nil {
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
		// Strict CE owns the pinned process output contract. Suppress only the
		// Ptah-specific role coverage note; selector and safety diagnostics keep
		// their ordinary writer.
		SuppressRoleCoverageNote: opts.policy.IsStrictCE(),

		// Atlas-compatible surface; see cmd/atlas/schema_apply.go.
		IgnoreUnknownHCLNames:     opts.policy.IgnoreUnknownHCLNames(),
		ValidateDesiredSchema:     opts.policy.ValidateDesiredSchema,
		PrepareInspectedSchema:    opts.policy.PrepareInspectedSchema,
		ValidateLiveObject:        atlasLiveSchemaObjectValidator(opts.policy),
		ValidateMigrationSource:   opts.policy.MigrationSourceValidator(opts.devURL),
		ValidateLocalSchemaSource: opts.policy.ValidateLocalSchemaSource,
		OmitAtlasRefusedBlocks:    omitRefusedBlocks,
		CompatibilityHCLFraming:   true,
		DevURLDiagnostic:          atlasDevURLDriverDiagnostic,
		Vars:                      schemaVars,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if path := strings.TrimSpace(opts.output); path != "" {
		if err := writeAtlasOutputFile(path, []byte(rendered)); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		return nil
	}
	fmt.Fprint(cmd.OutOrStdout(), rendered)
	return nil
}

// atlasSchemaInspectCompatibilityFormat preserves the pinned Atlas CE v1.3.0
// interpretation of shorthand-looking template text. Exact bare hcl/sql/json
// and their whitespace-wrapped forms remain literal text, byte for byte.
// Explicit helper calls still reach the shared renderer.
func atlasSchemaInspectCompatibilityFormat(format string) string {
	switch strings.TrimSpace(format) {
	case "hcl", "sql", "json":
		return "{{ " + strconv.Quote(format) + " }}"
	}
	return format
}

// atlasInspectOmitsRefusedBlocks is this surface's default for the top-level
// block types the pinned Atlas community binary refuses to read.
//
// It stands in for a binary that refuses a whole schema file for containing an
// extension, sequence or policy block, so by default it renders what that
// binary can read and reports what it left out on standard error
// (stokaro/ptah#1251). The capability is not deleted with it:
// PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 restores every block Ptah models on this same
// surface, because compatibility never removes a capability (AGENTS.md). It is
// an environment variable rather than a flag because the conformance
// cli-surface tier asserts flag parity with that binary, and a flag it does not
// register would break the promise this surface exists to keep.
//
// Native `ptah schema inspect` never calls this and always describes every
// construct Ptah models.
func atlasInspectOmitsRefusedBlocks() (bool, error) {
	keep, err := atlashclrender.KeepAtlasRefusedBlocks()
	if err != nil {
		return false, err
	}
	return !keep, nil
}

func needsAtlasSchemaInspectConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("url")
}

// validateAtlasSchemaInspectOptions settles this verb's --url before any source
// is resolved.
//
// Three measured answers, not one. On the pinned community binary v1.3.0 an
// absent --url is the plural required-flag refusal; an explicitly empty one
// gets past that check and is answered `missing scheme` by the desired-state
// layer -- this verb's URL names a source, not a connection, so it carries no
// `sql/sqlclient:` prefix; and a scheme that names no source kind at all is
// answered `sql/sqlclient: unknown driver`. A url supplied by atlas.hcl is
// already folded into opts.url by the caller, so it satisfies the first check
// exactly as a flag does. See cmd/atlas/compat_url_diagnostic.go.
func validateAtlasSchemaInspectOptions(cmd *cobra.Command, opts atlasSchemaInspectOptions) error {
	if !cmd.Flags().Changed("url") && strings.TrimSpace(opts.url) == "" {
		return atlasRequiredURLError(atlasRequiredURLPlural)
	}
	return atlasDesiredStateURLDiagnostic(opts.url)
}
