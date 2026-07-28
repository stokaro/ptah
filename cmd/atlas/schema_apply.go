package atlas

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdflags"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/editor"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/atlassource"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/migrator"
)

type atlasSchemaApplyOptions struct {
	url         string
	filePaths   []string
	toURLs      []string
	devURL      string
	dryRun      bool
	autoApprove bool
	format      string
	exclude     []string
	txMode      string
	schemas     []string
	planURL     string
	lockTimeout string
	edit        bool
	// formatOutput is derived at run time: true when --format was passed or
	// atlas.hcl provides format.schema.apply.
	formatOutput bool
}

func newAtlasSchemaApplyCommand() *cobra.Command {
	opts := atlasSchemaApplyOptions{}
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a desired schema to a database",
		Long: `Atlas OSS ` + "`atlas schema apply`" + ` command path.

Compares a live database from --url with the --to desired state and applies the
generated schema changes directly to the target database. --to accepts local
file:// schema files with .hcl, .yaml, .yml, or .sql extensions, one directly
connectable database URL whose live schema becomes the desired state, one
migration directory (a file:// directory containing atlas.sum) replayed on the
required --dev-url dev database, or one env://<attribute> reference (src,
schema.src, url, dev, migration.dir) resolved through the evaluated atlas.hcl
env. All --to values must be one source kind; unsupported schemes such as
atlas:// fail before the target database is touched. When --env is set, the
selected atlas.hcl env can provide url, schema.src, dev, exclude, schema.mode,
format.schema.apply, and supported diff policy values. With --edit the planned
SQL opens in $VISUAL or $EDITOR before the plan is shown and approved, and the
edited SQL is what gets applied. With --plan file://<path>, a pre-approved
local plan file saved by ` + "`atlas schema plan`" + ` is executed instead of
re-planning, after verifying the database still matches the plan's source
fingerprint; registry plan URLs are not supported. Schema/include filters and
database lock waiting remain explicit follow-up gaps.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaApply(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to apply to")
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema target URL")
	flags.StringArrayVarP(&opts.filePaths, atlasFileFlagName, atlasFileFlagShorthand, nil, "File or directory containing HCL or SQL files")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used by Atlas for planning")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Show planned changes without applying them")
	flags.BoolVar(&opts.autoApprove, "auto-approve", false, "Skip interactive approval")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from apply")
	flags.StringVar(&opts.txMode, "tx-mode", "", "Transaction mode: all, file, or none")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schemas to apply when database URLs are used")
	flags.StringArray("include", nil, "Schema objects to include in apply")
	flags.StringVar(&opts.planURL, "plan", "", "URL to a pre-planned migration (e.g., file://<name>"+atlasschema.PlanFileSuffix+")")
	flags.BoolVar(&opts.edit, "edit", false, "Open the generated SQL in an editor")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring the database lock")
	if err := cmdflags.DisableEnvBinding(flags, "auto-approve"); err != nil {
		panic(err)
	}
	if err := flags.MarkHidden(atlasFileFlagName); err != nil {
		panic(err)
	}
	cmd.MarkFlagsMutuallyExclusive(atlasFileFlagName, "to")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaApply(cmd *cobra.Command, opts atlasSchemaApplyOptions) error {
	formatOutput := cmd.Flags().Changed("format")
	policy := atlasschema.DiffPolicy{}
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if needsAtlasSchemaApplyConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		opts.url = dbcli.EffectiveString(cmd, "url", opts.url, projectCfg.DatabaseURL)
		opts.devURL = dbcli.EffectiveString(cmd, "dev-url", opts.devURL, projectCfg.DevURL)
		opts.toURLs = effectiveStringArray(cmd, "to", opts.toURLs, projectCfg.SchemaSources)
		opts.exclude = effectiveAtlasExclude(cmd, opts.exclude, projectCfg)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, projectCfg.Format.Schema.Apply)
		formatOutput = formatOutput || projectCfg.Format.Schema.Apply != ""
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	opts.formatOutput = formatOutput
	if strings.TrimSpace(opts.planURL) != "" {
		return runAtlasSchemaApplyPlanFile(cmd, opts)
	}
	if loaded && !cmd.Flags().Changed("to") && !cmd.Flags().Changed(atlasFileFlagName) && len(projectCfg.SchemaSources) > 0 {
		opts.toURLs, err = atlasProjectConfigSchemaURLs(cmd, opts.toURLs)
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("atlas.hcl schema.src: %w", err))
		}
	}
	if cmd.Flags().Changed(atlasFileFlagName) {
		opts.toURLs = opts.filePaths
	}
	if formatOutput && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	if formatOutput {
		if err := atlasreport.ValidateSchemaApplyTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}

	projectEnv := atlassource.ProjectEnv{}
	if loaded {
		projectEnv, err = atlasSourceProjectEnv(cmd, projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if err := validateAtlasSchemaApplyOptions(cmd, opts, projectEnv); err != nil {
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

	plan, err := atlasschema.PrepareApply(cmd.Context(), conn, atlasschema.ApplyRuntimeOptions{
		DevURL:     opts.devURL,
		ToURLs:     opts.toURLs,
		Exclude:    opts.exclude,
		Policy:     policy,
		TxMode:     txMode,
		DryRun:     opts.dryRun,
		ProjectEnv: projectEnv,
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

	sqlText := plan.SQL()
	statements := plan.Statements()
	if opts.edit {
		edited, err := editAtlasSchemaApplySQL(sqlText)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		sqlText = edited
		statements = atlasschema.SplitApplyStatements(sqlText, conn.Info().Dialect)
	}
	formattedPlan := ""
	if formatOutput {
		var err error
		formattedPlan, err = renderAtlasSchemaApplyFormat(opts, statements)
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
	if err := validateAtlasSchemaApplyDiffPolicy(txMode, conn, statements); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	ok, err := confirmAtlasSchemaApply(cmd, opts, formattedPlan)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !ok {
		return nil
	}

	if opts.edit {
		// The edited SQL replaces the prepared plan as the executable payload.
		conn.SchemaWriter().SetDryRun(false)
		if err := atlasschema.ApplySQL(cmd.Context(), conn, txMode, sqlText); err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
		}
	} else if err := plan.Execute(cmd.Context()); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
	}
	if formatOutput {
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema apply completed successfully.")
	return nil
}

func needsAtlasSchemaApplyConfig(cmd *cobra.Command) bool {
	if !cmd.Flags().Changed("url") {
		return true
	}
	// A pre-approved plan file fixes the desired state, so no schema source is
	// needed from flags or atlas.hcl.
	if cmd.Flags().Changed("plan") {
		return false
	}
	return !cmd.Flags().Changed("to") && !cmd.Flags().Changed(atlasFileFlagName)
}

// runAtlasSchemaApplyPlanFile executes a pre-approved local plan file saved by
// `atlas schema plan` instead of re-planning. The plan's source fingerprint is
// verified against the live database first: a drifted target refuses to
// execute, which is the entire value of a pre-approved plan.
func runAtlasSchemaApplyPlanFile(cmd *cobra.Command, opts atlasSchemaApplyOptions) error {
	if opts.formatOutput && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	if opts.formatOutput {
		if err := atlasreport.ValidateSchemaApplyTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if err := validateAtlasSchemaApplyPlanOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	txMode, err := migrator.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	path, err := atlasSchemaApplyPlanFilePath(opts.planURL)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	plan, err := atlasschema.ReadPlanFile(path)
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

	if err := atlasschema.VerifyPlanTarget(conn, plan); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	statements := plan.StatementSQL()
	formattedPlan := ""
	if opts.formatOutput {
		formattedPlan, err = renderAtlasSchemaApplyFormat(opts, statements)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprint(cmd.OutOrStdout(), formattedPlan)
	} else {
		printAtlasSchemaApplyPlan(cmd.OutOrStdout(), plan.SQL())
	}
	if opts.dryRun {
		return nil
	}
	if err := validateAtlasSchemaApplyDiffPolicy(txMode, conn, statements); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	ok, err := confirmAtlasSchemaApply(cmd, opts, formattedPlan)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !ok {
		return nil
	}

	conn.SchemaWriter().SetDryRun(false)
	if err := atlasschema.ApplySQL(cmd.Context(), conn, txMode, plan.SQL()); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
	}
	if opts.formatOutput {
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema apply completed successfully.")
	return nil
}

// validateAtlasSchemaApplyPlanOptions rejects flags that would recompute or
// reshape the pre-approved plan: the plan file already fixes the desired
// state, the exclude patterns, and the exact SQL that was reviewed.
func validateAtlasSchemaApplyPlanOptions(cmd *cobra.Command, opts atlasSchemaApplyOptions) error {
	if strings.TrimSpace(opts.url) == "" {
		return fmt.Errorf("--url is required")
	}
	conflicts := []struct {
		flag   string
		reason string
	}{
		{"to", "the plan file already fixes the desired state"},
		{atlasFileFlagName, "the plan file already fixes the desired state"},
		{"dev-url", "the plan is already computed; there is nothing to re-plan on a dev database"},
		{"exclude", "the plan file records the exclude patterns it was computed with"},
		{"edit", "a pre-approved plan must execute exactly as reviewed; recompute the plan with `schema plan` instead"},
		{atlasSchemaFlagName, "the plan file already fixes the planned schema objects"},
		{"include", "the plan file already fixes the planned schema objects"},
	}
	for _, conflict := range conflicts {
		if cmd.Flags().Changed(conflict.flag) {
			return fmt.Errorf("atlas schema apply --plan cannot be combined with --%s: %s", conflict.flag, conflict.reason)
		}
	}
	if strings.TrimSpace(opts.lockTimeout) != "" {
		return fmt.Errorf("atlas schema apply accepts --lock-timeout, but Ptah does not implement database lock waiting yet")
	}
	return nil
}

// atlasSchemaApplyPlanFilePath resolves a --plan URL to a local plan-file
// path. Registry URLs (for example atlas://repo/plans/name) are rejected:
// Ptah has no plan registry, and the open replacement is a local plan file.
func atlasSchemaApplyPlanFilePath(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
		return "", fmt.Errorf("atlas schema apply accepts registry plan URLs like %q, but Ptah has no plan registry; "+
			"pass a local plan file saved by `schema plan` as --plan file://<path>", raw)
	}
	path, err := schemafile.LocalFilePath(trimmed)
	if err != nil {
		return "", fmt.Errorf("--plan %q: %w", raw, err)
	}
	return path, nil
}

func confirmAtlasSchemaApply(cmd *cobra.Command, opts atlasSchemaApplyOptions, formattedPlan string) (bool, error) {
	if opts.autoApprove {
		if !opts.formatOutput {
			fmt.Fprintln(cmd.OutOrStdout(), "Auto-approval enabled; applying schema changes.")
		}
		return true, nil
	}
	if opts.formatOutput && !strings.HasSuffix(formattedPlan, "\n") {
		fmt.Fprintln(cmd.OutOrStdout())
	}
	return promptAtlasSchemaApplyConfirmation(cmd.OutOrStdout(), cmd.InOrStdin())
}

func validateAtlasSchemaApplyDiffPolicy(
	txMode migrator.MigrationTxMode,
	conn *dbschema.DatabaseConnection,
	statements []string,
) error {
	if len(statements) == 0 {
		return nil
	}
	if conn.Info().Dialect != "postgres" && conn.Info().Dialect != "postgresql" {
		return nil
	}
	if txMode == migrator.MigrationTxModeNone {
		return nil
	}
	for _, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), "CREATE INDEX CONCURRENTLY") {
			return fmt.Errorf("atlas.hcl diff.concurrent_index.create requires --tx-mode none for schema apply")
		}
	}
	return nil
}

// editAtlasSchemaApplySQL round-trips the planned SQL through the operator's
// editor ($VISUAL, then $EDITOR) via a temporary file and returns the edited
// text, which replaces the prepared plan for display, policy validation, and
// execution.
func editAtlasSchemaApplySQL(sqlText string) (string, error) {
	file, err := os.CreateTemp("", "ptah-schema-apply-*.sql")
	if err != nil {
		return "", fmt.Errorf("create schema apply edit file: %w", err)
	}
	path := file.Name()
	defer os.Remove(path)
	if _, err := file.WriteString(sqlText); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("write schema apply edit file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close schema apply edit file: %w", err)
	}
	if err := editor.Open("", path); err != nil {
		return "", err
	}
	edited, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read edited schema apply SQL: %w", err)
	}
	return string(edited), nil
}

func effectiveStringArray(cmd *cobra.Command, flagName string, flagValues, configValues []string) []string {
	if cmd.Flags().Changed(flagName) || len(configValues) == 0 {
		return flagValues
	}
	return slices.Clone(configValues)
}

func validateAtlasSchemaApplyOptions(
	cmd *cobra.Command,
	opts atlasSchemaApplyOptions,
	projectEnv atlassource.ProjectEnv,
) error {
	if strings.TrimSpace(opts.url) == "" {
		return fmt.Errorf("--url is required")
	}
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	if len(opts.schemas) > 0 {
		return fmt.Errorf("atlas schema apply accepts --schema, but Ptah only supports local schema files for this command yet")
	}
	if values, err := cmd.Flags().GetStringArray("include"); err == nil && len(values) > 0 {
		return fmt.Errorf("atlas schema apply accepts --include, but Ptah only supports local schema files for this command yet")
	}
	if strings.TrimSpace(opts.lockTimeout) != "" {
		return fmt.Errorf("atlas schema apply accepts --lock-timeout, but Ptah does not implement database lock waiting yet")
	}
	// Classification rejects unsupported schemes and source conflicts, and the
	// dev-database requirement is checked here, before the target database is
	// contacted.
	set, err := atlassource.ClassifySet("--to", opts.toURLs, projectEnv)
	if err != nil {
		return err
	}
	return set.EnsureDevDatabase(opts.devURL)
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
