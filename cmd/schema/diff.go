package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/serverversion"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
)

const (
	diffFromFlag    = "from"
	diffToFlag      = "to"
	diffDevURLFlag  = "dev-url"
	diffIncludeFlag = "include"
	diffExcludeFlag = "exclude"
	diffFormatFlag  = "format"
)

type schemaDiffOptions struct {
	fromURLs       []string
	toURLs         []string
	devURL         string
	schemas        string
	include        []string
	exclude        []string
	format         string
	serverVersion  string
	connectTimeout string
	configPath     string
	envName        string
}

func newSchemaDiffCommand() *cobra.Command {
	opts := schemaDiffOptions{}
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Diff two arbitrary schema states",
		Long: `Calculate the SQL statements that migrate the --from schema state to the
--to schema state. Each side accepts local schema files (.hcl, .yaml, .yml,
or .sql; repeatable), one directly connectable database URL whose live schema
is introspected, or one Atlas-format migration directory replayed on the
required --dev-url dev database. All URLs of one flag must be one source
kind.

The SQL dialect is pinned by --dev-url first, then by --from and --to
database URLs; local schema files alone still require --dev-url. This makes
CI checks like "do these two schema files differ?" or "does this migration
directory converge to schema.hcl?" possible without touching a production
database. --schemas and --include positively select what both comparison
sides see; --exclude subtracts from the result. A selected object that
depends on an unselected object refuses the diff with an explicit diagnostic.
An --include selection that matches nothing on either side is refused instead
of reporting a synced schema to a CI check.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaDiff(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.fromURLs, diffFromFlag, nil, "Current schema state: file path, database URL, or migration directory (repeatable)")
	flags.StringArrayVar(&opts.toURLs, diffToFlag, nil, "Desired schema state: file path, database URL, or migration directory (repeatable)")
	flags.StringVar(&opts.devURL, diffDevURLFlag, "", "Dev database URL used to choose the SQL dialect and replay migration-directory sources")
	dbcli.RegisterURLScopedSchemasFlag(flags, &opts.schemas)
	flags.StringArrayVar(&opts.include, diffIncludeFlag, nil, "Schema objects to include in diffing (Atlas-style selectors)")
	flags.StringArrayVar(&opts.exclude, diffExcludeFlag, nil, "Schema objects to exclude from diffing (Atlas-style selectors)")
	flags.StringVar(&opts.format, diffFormatFlag, "sql", "Output format: sql or json")
	// The dialect this resolves against is not known here -- it comes from
	// --dev-url or from a source URL -- so the value travels to
	// atlasschema.Diff and is resolved where the dialect is.
	serverversion.Register(flags, &opts.serverVersion)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runSchemaDiff(cmd *cobra.Command, opts schemaDiffOptions) error {
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	opts.devURL = dbcli.EffectiveString(
		cmd,
		diffDevURLFlag,
		opts.devURL,
		projectCfg.StringValue(projectconfig.StringDevURL),
	)
	policy := nativeDiffPolicy(projectCfg)

	format := strings.ToLower(strings.TrimSpace(opts.format))
	switch format {
	case "", "sql":
		format = "sql"
	case "json":
	default:
		return cmdutil.Fail(cmd, fmt.Errorf("unsupported --%s %q: expected sql or json", diffFormatFlag, opts.format))
	}
	if err := validateSchemaDiffSources(opts); err != nil {
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

	report, err := atlasschema.Diff(cmd.Context(), atlasschema.DiffOptions{
		FromURLs:       opts.fromURLs,
		ToURLs:         opts.toURLs,
		DevURL:         opts.devURL,
		ServerVersion:  opts.serverVersion,
		Exclude:        opts.exclude,
		Schemas:        dbcli.ParseSchemas(opts.schemas),
		Include:        opts.include,
		Policy:         policy,
		ConnectTimeout: connectTimeout,
		Diagnostics:    cmd.ErrOrStderr(),
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if format == "json" {
		return writeSchemaDiffJSON(cmd, report)
	}
	if err := atlasreport.WriteSchemaDiff(cmd.OutOrStdout(), atlasreport.NormalizeSchemaDiffFormat(""), report); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return nil
}

func validateSchemaDiffSources(opts schemaDiffOptions) error {
	if len(opts.fromURLs) == 0 {
		return fmt.Errorf("--%s is required", diffFromFlag)
	}
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--%s is required", diffToFlag)
	}
	// Malformed or unsupported --include selectors fail before any database
	// is contacted.
	if err := atlasfilter.ValidateIncludeSelectors(opts.include); err != nil {
		return err
	}
	// Classification rejects unsupported schemes and source conflicts, and
	// migration-directory sources require a dev database, before any database
	// is contacted. --from is validated first, then --to.
	fromSet, err := atlassource.ClassifySet("--"+diffFromFlag, opts.fromURLs, atlassource.ProjectEnv{})
	if err != nil {
		return err
	}
	if err := fromSet.EnsureDevDatabase(opts.devURL); err != nil {
		return err
	}
	toSet, err := atlassource.ClassifySet("--"+diffToFlag, opts.toURLs, atlassource.ProjectEnv{})
	if err != nil {
		return err
	}
	return toSet.EnsureDevDatabase(opts.devURL)
}

// writeSchemaDiffJSON renders the diff as a stable JSON document: an array of
// migration statements under "statements" (empty when the schemas are
// synced), for machine consumption in CI.
func writeSchemaDiffJSON(cmd *cobra.Command, report atlasreport.SchemaDiff) error {
	statements := make([]string, 0, len(report.Changes))
	for _, change := range report.Changes {
		statements = append(statements, change.Cmd)
	}
	document, err := json.MarshalIndent(map[string][]string{"statements": statements}, "", "  ")
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("render schema diff JSON: %w", err))
	}
	document = append(document, '\n')
	if _, err := cmd.OutOrStdout().Write(document); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write schema diff JSON: %w", err))
	}
	return nil
}
