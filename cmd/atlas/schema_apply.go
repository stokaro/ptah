package atlas

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdflags"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

type atlasSchemaApplyOptions struct {
	url         string
	toURLs      []string
	devURL      string
	dryRun      bool
	autoApprove bool
	format      string
	txMode      string
}

func newAtlasSchemaApplyCommand() *cobra.Command {
	opts := atlasSchemaApplyOptions{}
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a desired schema to a database",
		Long: `Atlas OSS ` + "`atlas schema apply`" + ` command path.

Compares a live database from --url with local --to schema files and applies the
generated schema changes directly to the target database. This implementation
currently supports local file:// schema files with .hcl, .yaml, .yml, or .sql
extensions. Database desired-state URLs, Atlas project env:// URLs, schema
filters, custom output templates, and transaction-mode control remain explicit
follow-up gaps.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaApply(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to apply to")
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema target URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used by Atlas for planning")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Show planned changes without applying them")
	flags.BoolVar(&opts.autoApprove, "auto-approve", false, "Skip interactive approval")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.StringVar(&opts.txMode, "tx-mode", "", "Transaction mode: all, file, or none")
	flags.StringArray("schema", nil, "Schemas to apply when database URLs are used")
	flags.StringArray("exclude", nil, "Schema objects to exclude from apply")
	flags.StringArray("include", nil, "Schema objects to include in apply")
	if err := cmdflags.DisableEnvBinding(flags, "auto-approve"); err != nil {
		panic(err)
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaApply(cmd *cobra.Command, opts atlasSchemaApplyOptions) error {
	if err := validateAtlasSchemaApplyOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	if err := validateAtlasDevURLDialect(opts.devURL, conn.Info().Dialect); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	current, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("read database schema: %w", err))
	}
	desired, err := schemafile.LoadAll(opts.toURLs, schemafile.Options{Dialect: conn.Info().Dialect})
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("load --to schema: %w", err))
	}

	diff := schemadiff.CompareWithDialect(desired, current, conn.Info().Dialect)
	if !diff.HasChanges() {
		fmt.Fprintln(cmd.OutOrStdout(), "Schema is synced, no changes to be made.")
		return nil
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, desired, conn.Info().Dialect)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("generate schema apply SQL: %w", err))
	}
	sqlText := atlasMigrationSQL(statements)
	printAtlasSchemaApplyPlan(cmd.OutOrStdout(), sqlText)
	if opts.dryRun {
		return nil
	}

	ok, err := confirmAtlasSchemaApply(opts, cmd.OutOrStdout(), cmd.InOrStdin())
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !ok {
		return nil
	}

	conn.SchemaWriter().SetDryRun(false)
	migration := migrator.CreateMigrationFromSQL(0, "atlas schema apply", sqlText, "")
	if err := migration.Up(cmd.Context(), conn); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema apply completed successfully.")
	return nil
}

func validateAtlasSchemaApplyOptions(cmd *cobra.Command, opts atlasSchemaApplyOptions) error {
	if strings.TrimSpace(opts.url) == "" {
		return fmt.Errorf("--url is required")
	}
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	if strings.TrimSpace(opts.format) != "" {
		return fmt.Errorf("atlas schema apply accepts --format, but Ptah does not implement its behavior yet")
	}
	if strings.TrimSpace(opts.txMode) != "" {
		return fmt.Errorf("atlas schema apply accepts --tx-mode, but Ptah does not implement its behavior yet")
	}
	for _, name := range []string{"schema", "exclude", "include"} {
		if values, err := cmd.Flags().GetStringArray(name); err == nil && len(values) > 0 {
			return fmt.Errorf("atlas schema apply accepts --%s, but Ptah only supports local schema files for this command yet", name)
		}
	}
	return ensureLocalSchemaURLs("--to", opts.toURLs)
}

func validateAtlasDevURLDialect(devURL, targetDialect string) error {
	devDialect, err := dialectFromAtlasURL(devURL)
	if err != nil {
		return err
	}
	if devDialect == "" {
		return nil
	}
	if devDialect != targetDialect {
		return fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", devDialect, targetDialect)
	}
	return nil
}

func printAtlasSchemaApplyPlan(out io.Writer, sqlText string) {
	fmt.Fprintln(out, "Planned schema changes:")
	fmt.Fprintln(out, strings.TrimSpace(sqlText))
}

func confirmAtlasSchemaApply(opts atlasSchemaApplyOptions, prompt io.Writer, input io.Reader) (bool, error) {
	if opts.autoApprove {
		fmt.Fprintln(prompt, "Auto-approval enabled; applying schema changes.")
		return true, nil
	}

	fmt.Fprint(prompt, "Apply these schema changes? Type 'YES' to confirm: ")
	var confirmation string
	if _, err := fmt.Fscan(input, &confirmation); err != nil {
		return false, fmt.Errorf("read schema apply confirmation: %w", err)
	}
	if confirmation != "YES" {
		fmt.Fprintln(prompt, "Schema apply canceled.")
		return false, nil
	}
	fmt.Fprintln(prompt)
	return true, nil
}
