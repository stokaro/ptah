package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasschema"
)

type atlasSchemaPlanOptions struct {
	fromURLs []string
	toURLs   []string
	devURL   string
	exclude  []string
	schemas  []string
	name     string
	output   string
	save     bool
	dryRun   bool
}

func newAtlasSchemaPlanCommand() *cobra.Command {
	opts := atlasSchemaPlanOptions{}
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Plan a declarative migration for a schema transition",
		Long: `Atlas OSS ` + "`atlas schema plan`" + ` command path.

Atlas gates schema plan behind the Atlas Pro registry approval flow. Ptah
implements the open local replacement: the plan is computed from the --from
target database to the local --to schema files and saved as a local plan
file. The default format is the Atlas ` + "`.plan.hcl`" + ` shape (one plan block with
from/to fingerprints and the migration SQL), so the saved file is readable by
Atlas's own plan reader; an --output path ending in .json writes Ptah's
native fingerprinted JSON plan (format version 1) instead. The from/to
values in a ` + "`.plan.hcl`" + ` are Ptah's sha256 fingerprints — the official Atlas
binary parses the file but verifies its own hashes, which have no local
recipe.
` + "`atlas schema apply --plan file://<path>`" + ` executes the saved plan after
verifying it against the live database, so a reviewed plan is exactly what
runs. Pass --save or --output <path> to write the plan file; without either,
the plan document prints to stdout (--dry-run does the same explicitly).
--auto-approve is accepted for Atlas CLI compatibility: a locally saved plan
file is approved by operator review, so there is no prompt to skip. When
--env is set, the selected atlas.hcl env can provide url (the --from
target), schema.src, dev, exclude, schema.mode, and supported diff policy
values. Registry-bound planning (--push, --pending, --repo) and the plan
registry sub-verbs remain Atlas CE boundary stubs.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaPlan(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.fromURLs, atlasFromFlagName, nil, "Current schema state URL: the target database the plan applies to")
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema state URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used by Atlas for planning")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from planning")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schemas to plan when database URLs are used")
	flags.StringVar(&opts.name, "name", "", "Plan name recorded in the plan file")
	flags.StringVarP(&opts.output, "output", "o", "", "Plan file output path (default <name>"+atlasschema.PlanFileSuffixHCL+"; a .json path writes the native JSON plan format)")
	flags.BoolVar(&opts.save, "save", false, "Save the plan to a local plan file")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Print the plan file document without saving it")
	// Accepted for Atlas CLI compatibility: a locally saved plan file is
	// approved by operator review, so there is no approval prompt to skip.
	flags.Bool("auto-approve", false, "Approve the plan without asking for confirmation")
	// The remaining Atlas Pro plan flags are declared for CLI-surface parity
	// and rejected loudly in validateAtlasSchemaPlanOptions: their behavior is
	// either bound to the Atlas Registry or not implemented yet.
	flags.Bool("push", false, "Push the plan to the Atlas Registry")
	flags.Bool("pending", false, "Push the plan in a pending state")
	flags.String("repo", "", "URL to the schema repository")
	flags.Bool("edit", false, "Edit the plan in the terminal editor")
	flags.Bool("skip-lint", false, "Skip linting the migration plan")
	flags.String("format", "", "Atlas Go template output format")
	flags.String("name-format", "", "Go template used to compute the plan name")
	flags.StringArrayP("directive", "d", nil, "Directives for the migration plan")
	flags.StringArray("include", nil, "Schema objects to include in planning")
	flags.String("lock-timeout", "", "Timeout for acquiring the database lock")
	cmd.MarkFlagsMutuallyExclusive("save", "dry-run")
	cmd.MarkFlagsMutuallyExclusive("output", "dry-run")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	addAtlasUnsupportedCommunityCommands(cmd, "schema plan", []atlasUnsupportedCommunityVerb{
		{use: "approve", short: "Approve a plan in the Atlas Registry"},
		{use: "lint", short: "Lint a plan against the Atlas Registry"},
		{use: "list", short: "List plans in the Atlas Registry"},
		{use: "new", short: "Create a new plan in the Atlas Registry"},
		{use: "pull", short: "Pull a plan from the Atlas Registry"},
		{use: "push", short: "Push a plan to the Atlas Registry"},
		{use: "rm", short: "Remove a plan from the Atlas Registry"},
		{use: "test", short: "Test a plan through the Atlas Registry"},
		{use: "validate", short: "Validate a plan through the Atlas Registry"},
	})
	return cmd
}

func runAtlasSchemaPlan(cmd *cobra.Command, opts atlasSchemaPlanOptions) error {
	policy := atlasschema.DiffPolicy{}
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if needsAtlasSchemaPlanConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		databaseURL := projectCfg.StringValue(projectconfig.StringDatabaseURL)
		if !cmd.Flags().Changed(atlasFromFlagName) && databaseURL.Present {
			opts.fromURLs = []string{databaseURL.Value}
		}
		opts.toURLs = effectiveStringArray(cmd, "to", opts.toURLs, projectCfg.SchemaSources)
		opts.devURL = dbcli.EffectiveString(
			cmd,
			"dev-url",
			opts.devURL,
			projectCfg.StringValue(projectconfig.StringDevURL),
		)
		opts.exclude = effectiveAtlasExclude(cmd, opts.exclude, projectCfg)
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if loaded && !cmd.Flags().Changed("to") && len(projectCfg.SchemaSources) > 0 {
		opts.toURLs, err = atlasProjectConfigSchemaURLs(cmd, opts.toURLs)
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("atlas.hcl schema.src: %w", err))
		}
	}
	// Schema plan resolves local schema files only (LocalFilesOnly), so an env
	// whose desired state is an external schema program cannot feed it yet.
	if loaded && !cmd.Flags().Changed("to") && atlasExternalSchemaConfigured(projectCfg) {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"atlas schema plan does not support atlas.hcl data.external_schema desired state yet; pass --to explicitly",
		))
	}
	if err := validateAtlasSchemaPlanOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.fromURLs[0])
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --from: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasschema.PreparePlanFile(cmd.Context(), conn, atlasschema.PlanFileOptions{
		Name:    opts.name,
		DevURL:  opts.devURL,
		ToURLs:  opts.toURLs,
		Exclude: opts.exclude,
		Policy:  policy,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !plan.HasChanges() {
		fmt.Fprintln(cmd.OutOrStdout(), "Schema is synced, no changes to be made.")
		return nil
	}
	outputPath := strings.TrimSpace(opts.output)
	format := atlasSchemaPlanFormat(outputPath)
	// The Atlas plan format names plans with a UTC timestamp; keep the
	// deterministic fingerprint-derived default for the native JSON format.
	if format == atlasschema.PlanFormatHCL && strings.TrimSpace(opts.name) == "" {
		plan.Name = atlasschema.TimestampPlanName(time.Now())
	}
	document, err := atlasschema.MarshalPlanFileAs(plan, format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// Without a save destination the plan document prints to stdout, like
	// Atlas printing the computed plan; --dry-run does the same explicitly.
	if opts.dryRun || (!opts.save && outputPath == "") {
		if _, err := cmd.OutOrStdout().Write(document); err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("write plan preview: %w", err))
		}
		return nil
	}

	printAtlasSchemaApplyPlan(cmd.OutOrStdout(), plan.SQL())
	path := outputPath
	if path == "" {
		path = plan.Name + atlasschema.PlanFileSuffixFor(format)
		// Default plan names are timestamps with one-second granularity, so
		// two plans computed in the same second would silently overwrite each
		// other's reviewed SQL. An explicit --output path stays overwritable.
		if _, err := os.Stat(path); err == nil {
			return cmdutil.Fail(cmd, fmt.Errorf(
				"plan file %s already exists; pass --name or --output to choose a distinct plan file", path))
		} else if !errors.Is(err, fs.ErrNotExist) {
			return cmdutil.Fail(cmd, fmt.Errorf("check plan file %s: %w", path, err))
		}
	}
	if err := os.WriteFile(path, document, 0o644); err != nil { //nolint:gosec // plan files are meant to be reviewed and shared, 0644 like migration files
		return cmdutil.Fail(cmd, fmt.Errorf("write plan file: %w", err))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Plan saved to file://%s\n", path)
	return nil
}

// atlasSchemaPlanFormat selects the plan-file encoding from the output path:
// the Atlas-compatible tree defaults to the Atlas `.plan.hcl` format, and an
// explicit .json output path keeps the native JSON plan format reachable.
func atlasSchemaPlanFormat(outputPath string) atlasschema.PlanFormat {
	if strings.HasSuffix(strings.ToLower(outputPath), ".json") {
		return atlasschema.PlanFormatJSON
	}
	return atlasschema.PlanFormatHCL
}

func needsAtlasSchemaPlanConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed(atlasFromFlagName) || !cmd.Flags().Changed("to")
}

func validateAtlasSchemaPlanOptions(cmd *cobra.Command, opts atlasSchemaPlanOptions) error {
	if err := rejectUnimplementedAtlasSchemaPlanFlags(cmd, opts); err != nil {
		return err
	}
	if len(opts.fromURLs) == 0 {
		return fmt.Errorf("--from is required")
	}
	if len(opts.fromURLs) > 1 {
		return fmt.Errorf("atlas schema plan accepts multiple --from URLs, but Ptah plans against one target database URL")
	}
	if err := ensureAtlasPlanDatabaseURL(opts.fromURLs[0]); err != nil {
		return err
	}
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	if err := ensureLocalSchemaURLs("--to", opts.toURLs); err != nil {
		return err
	}
	if strings.ContainsAny(opts.name, `/\`) {
		return fmt.Errorf("--name must not contain path separators; use --output to choose the plan file location")
	}
	return nil
}

// ensureAtlasPlanDatabaseURL requires the plan source to be a database URL:
// the plan's whole value is binding its statements to the fingerprint of the
// live database it will later be applied to.
func ensureAtlasPlanDatabaseURL(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !strings.Contains(trimmed, "://") || strings.HasPrefix(trimmed, "file://") {
		return fmt.Errorf("atlas schema plan requires --from to be the target database URL the plan will be applied to "+
			"(got %q); local desired-state schema files belong in --to", raw)
	}
	return nil
}

func rejectUnimplementedAtlasSchemaPlanFlags(cmd *cobra.Command, opts atlasSchemaPlanOptions) error {
	rejections := []struct {
		flag   string
		reason string
	}{
		{"push", "plan push targets the Atlas Registry (Atlas Cloud); Ptah's local plan workflow saves plan files with --save or --output instead"},
		{"pending", "pending plans are an Atlas Registry approval state; a locally saved plan file is approved by operator review"},
		{"repo", "schema repositories exist only in the Atlas Registry (Atlas Cloud); Ptah plans are local files"},
		{"edit", "Ptah does not implement editing the plan before saving yet; review the saved plan file instead"},
		{"skip-lint", "Ptah does not lint declarative plans yet, so there is no lint step to skip"},
		{"format", "Ptah does not implement --format for schema plan yet"},
		{"name-format", "Ptah does not implement Go-template plan naming yet; use --name"},
		{"directive", "Ptah does not implement Atlas plan directives yet"},
		{"lock-timeout", "Ptah does not implement database lock waiting yet"},
	}
	for _, rejection := range rejections {
		if cmd.Flags().Changed(rejection.flag) {
			return fmt.Errorf("atlas schema plan accepts --%s, but %s", rejection.flag, rejection.reason)
		}
	}
	if len(opts.schemas) > 0 {
		return fmt.Errorf("atlas schema plan accepts --schema, but Ptah only supports local schema files for this command yet")
	}
	if values, err := cmd.Flags().GetStringArray("include"); err == nil && len(values) > 0 {
		return fmt.Errorf("atlas schema plan accepts --include, but Ptah only supports local schema files for this command yet")
	}
	return nil
}
