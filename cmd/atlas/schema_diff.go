package atlas

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

type atlasSchemaDiffOptions struct {
	fromURLs []string
	toURLs   []string
	devURL   string
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
with .hcl, .yaml, .yml, or .sql extensions. Database URLs, migration directory
URLs, Atlas project env:// URLs, exclusion filters, and custom output templates
are explicit follow-up gaps.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaDiff(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.fromURLs, "from", nil, "Current schema state URL")
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema state URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to choose the SQL dialect for local schema files")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.StringArray("schema", nil, "Schemas to inspect when a database URL is used")
	flags.StringArray("exclude", nil, "Schema objects to exclude from diffing")
	flags.StringArray("include", nil, "Schema objects to include in diffing")
	flags.BoolP("web", "w", false, "Visualize the schema diff on Atlas Cloud")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaDiff(cmd *cobra.Command, opts atlasSchemaDiffOptions) error {
	if err := validateAtlasSchemaDiffOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	dialect, err := atlasSchemaDiffDialect(opts.devURL)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	from, err := schemafile.LoadAll(opts.fromURLs, schemafile.Options{Dialect: dialect})
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("load --from schema: %w", err))
	}
	to, err := schemafile.LoadAll(opts.toURLs, schemafile.Options{Dialect: dialect})
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("load --to schema: %w", err))
	}

	diff := schemadiff.CompareWithDialect(to, schemafile.ToDBSchema(from), dialect)
	if !diff.HasChanges() {
		fmt.Fprintln(cmd.OutOrStdout(), "Schemas are synced, no changes to be made.")
		return nil
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, to, dialect)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("generate schema diff SQL: %w", err))
	}
	for _, stmt := range statements {
		fmt.Fprintf(cmd.OutOrStdout(), "%s;\n", strings.TrimSuffix(stmt, ";"))
	}
	return nil
}

func validateAtlasSchemaDiffOptions(cmd *cobra.Command, opts atlasSchemaDiffOptions) error {
	if len(opts.fromURLs) == 0 {
		return fmt.Errorf("--from is required")
	}
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	if strings.TrimSpace(opts.format) != "" {
		return fmt.Errorf("atlas schema diff accepts --format, but Ptah does not implement its behavior yet")
	}
	if web, err := cmd.Flags().GetBool("web"); err == nil && web {
		return fmt.Errorf("atlas schema diff accepts --web, but Ptah does not implement Atlas Cloud visualization")
	}
	for _, name := range []string{"schema", "exclude", "include"} {
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

func atlasSchemaDiffDialect(devURL string) (string, error) {
	dialect, err := dialectFromAtlasURL(devURL)
	if err != nil {
		return "", err
	}
	if dialect == "" {
		return "", fmt.Errorf("--dev-url is required for local schema file diffing")
	}
	return dialect, nil
}

func dialectFromAtlasURL(rawURL string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse --dev-url: %w", err)
	}
	switch parsed.Scheme {
	case "docker":
		return dialectFromDockerURL(parsed)
	case "sqlite", "mysql", "mariadb", "postgres", "postgresql", "sqlserver", "mssql", "clickhouse", "cockroach", "cockroachdb", "yugabyte", "yugabytedb":
		dialect := platform.NormalizeDialect(parsed.Scheme)
		if dialect != "" {
			return dialect, nil
		}
	}
	return "", fmt.Errorf("unsupported --dev-url dialect %q", rawURL)
}

func dialectFromDockerURL(parsed *url.URL) (string, error) {
	engine := parsed.Host
	if engine == "" {
		return "", errors.New("docker --dev-url is missing database engine")
	}
	if before, _, found := strings.Cut(engine, "/"); found {
		engine = before
	}
	if before, _, found := strings.Cut(engine, ":"); found {
		engine = before
	}
	dialect := platform.NormalizeDialect(engine)
	if dialect == "" {
		return "", fmt.Errorf("unsupported docker --dev-url engine %q", parsed.Host)
	}
	return dialect, nil
}
