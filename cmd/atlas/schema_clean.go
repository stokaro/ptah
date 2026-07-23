package atlas

import (
	"bufio"
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
	"github.com/stokaro/ptah/internal/schemaclean"
)

type atlasSchemaCleanOptions struct {
	url         string
	dryRun      bool
	format      string
	autoApprove bool
}

func newAtlasSchemaCleanCommand() *cobra.Command {
	opts := atlasSchemaCleanOptions{}
	cmd := &cobra.Command{
		Use:   "clean",
		Short: "Clean database schema objects",
		Long: `Atlas OSS ` + "`atlas schema clean`" + ` command path.

Cleans user-owned schema objects through Ptah's destructive database cleanup
runtime. The implementation supports direct database URLs, dry-run planning,
explicit auto-approval, and Atlas Go-template output over the cleanup plan.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaClean(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to clean")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Show planned cleanup without applying it")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.BoolVar(&opts.autoApprove, "auto-approve", false, "Skip interactive approval")
	if err := cmdflags.DisableEnvBinding(flags, "auto-approve"); err != nil {
		panic(err)
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaClean(cmd *cobra.Command, opts atlasSchemaCleanOptions) error {
	formatOutput := cmd.Flags().Changed("format")
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		opts.url = dbcli.EffectiveString(cmd, "url", opts.url, projectCfg.DatabaseURL)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, projectCfg.Format.Schema.Clean)
		formatOutput = formatOutput || projectCfg.Format.Schema.Clean != ""
	}
	if formatOutput && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	if formatOutput {
		if err := atlasreport.ValidateSchemaCleanTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if strings.TrimSpace(opts.url) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--url is required"))
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	cancel()
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	plan, err := schemaclean.Inspect(conn)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if formatOutput && !opts.dryRun {
		if err := validateAtlasSchemaCleanActualFormat(opts, conn, plan); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if formatOutput && opts.dryRun {
		rendered, err := renderAtlasSchemaCleanFormat(opts, conn, plan, false)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprint(cmd.OutOrStdout(), rendered)
	} else if !formatOutput {
		printAtlasSchemaCleanPlan(cmd.OutOrStdout(), opts, conn, plan)
	}
	if opts.dryRun {
		return nil
	}

	ok := true
	if opts.autoApprove {
		if !formatOutput {
			fmt.Fprintln(cmd.OutOrStdout(), "Auto-approval enabled; skipping interactive confirmation.")
			fmt.Fprintln(cmd.OutOrStdout())
		}
	} else {
		if formatOutput {
			rendered, err := renderAtlasSchemaCleanFormat(opts, conn, plan, false)
			if err != nil {
				return cmdutil.Fail(cmd, err)
			}
			fmt.Fprint(cmd.OutOrStdout(), rendered)
			fmt.Fprintln(cmd.OutOrStdout())
		}
		var err error
		ok, err = promptAtlasSchemaCleanConfirmation(cmd.OutOrStdout(), cmd.InOrStdin())
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if !ok {
		return nil
	}

	if err := schemaclean.Apply(conn); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if formatOutput {
		rendered, err := renderAtlasSchemaCleanFormat(opts, conn, plan, true)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprint(cmd.OutOrStdout(), rendered)
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema clean completed successfully.")
	return nil
}

func printAtlasSchemaCleanPlan(
	out io.Writer,
	opts atlasSchemaCleanOptions,
	conn *dbschema.DatabaseConnection,
	plan schemaclean.Plan,
) {
	if opts.dryRun {
		fmt.Fprintf(out, "[DRY RUN] Would clean schema objects from database %s\n", dbschema.FormatDatabaseURL(opts.url))
	} else {
		fmt.Fprintf(out, "Cleaning schema objects from database %s\n", dbschema.FormatDatabaseURL(opts.url))
	}
	fmt.Fprintf(out, "Connected to %s database successfully.\n", conn.Info().Dialect)
	fmt.Fprintf(out, "Planned cleanup changes: %d\n", len(plan.Changes))
	for _, change := range plan.Changes {
		fmt.Fprintf(out, "- %s\n", change.Cmd)
	}
	if opts.dryRun {
		fmt.Fprintln(out, "[DRY RUN] No changes were applied.")
	}
}

func renderAtlasSchemaCleanFormat(
	opts atlasSchemaCleanOptions,
	conn *dbschema.DatabaseConnection,
	plan schemaclean.Plan,
	applied bool,
) (string, error) {
	report := atlasreport.NewSchemaClean(atlasreport.SchemaCleanOptions{
		Driver:  conn.Info().Dialect,
		URL:     opts.url,
		DryRun:  opts.dryRun,
		Applied: applied,
		Plan:    plan,
	})
	var out bytes.Buffer
	if err := atlasreport.WriteSchemaClean(&out, opts.format, report); err != nil {
		return "", err
	}
	return out.String(), nil
}

func validateAtlasSchemaCleanActualFormat(
	opts atlasSchemaCleanOptions,
	conn *dbschema.DatabaseConnection,
	plan schemaclean.Plan,
) error {
	if _, err := renderAtlasSchemaCleanFormat(opts, conn, plan, false); err != nil {
		return err
	}
	_, err := renderAtlasSchemaCleanFormat(opts, conn, plan, true)
	return err
}

func promptAtlasSchemaCleanConfirmation(prompt io.Writer, input io.Reader) (bool, error) {
	reader := bufio.NewReader(input)
	fmt.Fprintln(prompt, "WARNING: This operation will permanently delete all supported schema objects.")
	fmt.Fprint(prompt, "Type 'DELETE EVERYTHING' to confirm this destructive operation: ")
	confirmation, err := reader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("read schema clean confirmation: %w", err)
	}
	if strings.TrimSpace(confirmation) != "DELETE EVERYTHING" {
		fmt.Fprintln(prompt, "Schema clean canceled.")
		return false, nil
	}
	fmt.Fprint(prompt, "Last chance. Type 'YES I AM SURE' to proceed: ")
	confirmation, err = reader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("read schema clean confirmation: %w", err)
	}
	if strings.TrimSpace(confirmation) != "YES I AM SURE" {
		fmt.Fprintln(prompt, "Schema clean canceled.")
		return false, nil
	}
	fmt.Fprintln(prompt)
	return true, nil
}
