package atlas

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdflags"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/migration/migrator"
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
filters, and Atlas Cloud planning remain explicit follow-up gaps.`,
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
	formatOutput := cmd.Flags().Changed("format")
	if formatOutput && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	if formatOutput {
		if err := atlasreport.ValidateSchemaApplyTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if err := validateAtlasSchemaApplyOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	txMode, err := migrator.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasschema.PrepareApply(conn, atlasschema.ApplyRuntimeOptions{
		DevURL: opts.devURL,
		ToURLs: opts.toURLs,
		TxMode: txMode,
		DryRun: opts.dryRun,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !plan.HasChanges() {
		if formatOutput {
			return writeAtlasSchemaApplyFormat(cmd, opts, plan.Statements())
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Schema is synced, no changes to be made.")
		return nil
	}

	formattedPlan := ""
	sqlText := plan.SQL()
	if formatOutput {
		var err error
		formattedPlan, err = renderAtlasSchemaApplyFormat(opts, plan.Statements())
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprint(cmd.OutOrStdout(), formattedPlan)
	} else {
		printAtlasSchemaApplyPlan(cmd.OutOrStdout(), sqlText)
	}
	if opts.dryRun {
		return nil
	}

	ok := true
	if opts.autoApprove {
		if !formatOutput {
			fmt.Fprintln(cmd.OutOrStdout(), "Auto-approval enabled; applying schema changes.")
		}
	} else {
		if formatOutput && !strings.HasSuffix(formattedPlan, "\n") {
			fmt.Fprintln(cmd.OutOrStdout())
		}
		var err error
		ok, err = promptAtlasSchemaApplyConfirmation(cmd.OutOrStdout(), cmd.InOrStdin())
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if !ok {
		return nil
	}

	if err := plan.Execute(cmd.Context()); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
	}
	if formatOutput {
		return nil
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
	for _, name := range []string{"schema", "exclude", "include"} {
		if values, err := cmd.Flags().GetStringArray(name); err == nil && len(values) > 0 {
			return fmt.Errorf("atlas schema apply accepts --%s, but Ptah only supports local schema files for this command yet", name)
		}
	}
	return ensureLocalSchemaURLs("--to", opts.toURLs)
}

func printAtlasSchemaApplyPlan(out io.Writer, sqlText string) {
	fmt.Fprintln(out, "Planned schema changes:")
	fmt.Fprintln(out, strings.TrimSpace(sqlText))
}

func writeAtlasSchemaApplyFormat(cmd *cobra.Command, opts atlasSchemaApplyOptions, statements []string) error {
	rendered, err := renderAtlasSchemaApplyFormat(opts, statements)
	if err != nil {
		return err
	}
	_, err = io.WriteString(cmd.OutOrStdout(), rendered)
	return err
}

func renderAtlasSchemaApplyFormat(opts atlasSchemaApplyOptions, statements []string) (string, error) {
	report := atlasreport.NewSchemaApply(statements)
	var out bytes.Buffer
	if err := atlasreport.WriteSchemaApply(&out, opts.format, report); err != nil {
		return "", err
	}
	return out.String(), nil
}

func promptAtlasSchemaApplyConfirmation(prompt io.Writer, input io.Reader) (bool, error) {
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
