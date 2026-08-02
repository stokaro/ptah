package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasschema"
)

type atlasSchemaPlanOptions struct {
	fromURLs   []string
	toURLs     []string
	devURL     string
	exclude    []string
	schemas    []string
	name       string
	nameFormat string
	output     string
	save       bool
	dryRun     bool
	edit       bool
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
file is approved by operator review, so there is no prompt to skip.
--skip-lint is accepted as an explicit no-op: this command runs no lint step,
so there is nothing to skip.
--edit opens the planned SQL in $VISUAL, then $EDITOR, and saves the plan
rebuilt from the edited text; severity and the destructive marker are
re-derived from what you wrote, and an edit that leaves no statement is
refused. Statement text round-trips verbatim, comments included, so quitting
the editor without changing anything yields the same plan document as no
--edit at all. The recorded to-fingerprint still describes the schema the plan
was computed against, which edited SQL may no longer reach — ` + "`schema apply`" + `
replays an Atlas-format plan on a dev database and requires it to converge on
--to, but a native .json plan carries no such check.
--name-format computes the plan name from a Go template over .FromHash and
.ToHash, which are this plan's own sha256 fingerprints; it cannot be combined
with --name. Because those fingerprints are "sha256:"-prefixed, Atlas's own
documented example ({{ slice .ToHash 0 8 }}) yields a name containing a colon
and is refused; slice from 7 to skip the prefix. When --env is set, the
selected atlas.hcl env can provide url
(the --from target), schema.src, dev, exclude, schema.mode, and supported
diff policy values. Registry-bound planning (--push, --pending, --repo),
--format, --directive, and the plan registry sub-verbs remain unimplemented.`,
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
	// Atlas's own help advertises 'plan_{{ slice .ToHash 0 8 }}'. That example
	// cannot be copied verbatim here: Ptah fingerprints are "sha256:<hex>", so
	// slicing from 0 keeps the prefix and yields "plan_sha256:", whose colon is
	// illegal in a Windows file name and is refused. Slicing from 7 skips the
	// prefix and gives the digest characters the Atlas example is after.
	flags.StringVar(&opts.nameFormat, "name-format", "", "Go template used to compute the plan name (e.g. 'plan_{{ slice .ToHash 7 15 }}')")
	flags.StringVarP(&opts.output, "output", "o", "", "Plan file output path (default <name>"+atlasschema.PlanFileSuffixHCL+"; a .json path writes the native JSON plan format)")
	flags.BoolVar(&opts.save, "save", false, "Save the plan to a local plan file")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Print the plan file document without saving it")
	flags.BoolVar(&opts.edit, "edit", false, "Edit the plan in the terminal editor")
	// Accepted for Atlas CLI compatibility: a locally saved plan file is
	// approved by operator review, so there is no approval prompt to skip.
	flags.Bool("auto-approve", false, "Approve the plan without asking for confirmation")
	// Accepted as an explicit no-op: `schema plan` runs no lint step, so there
	// is nothing for --skip-lint to skip. Refusing it would break a Pro
	// pipeline that passes it, and honoring it cannot loosen a check that does
	// not exist. Pinned by TestSchemaPlanSkipLintIsANoOp — if a plan linter is
	// ever added, that test goes red and this comment stops being true.
	flags.Bool("skip-lint", false, "Skip linting the migration plan")
	// The remaining Atlas Pro plan flags are declared for CLI-surface parity
	// and rejected loudly in validateAtlasSchemaPlanOptions: their behavior is
	// either bound to the Atlas Registry or not implemented yet.
	flags.Bool("push", false, "Push the plan to the Atlas Registry")
	flags.Bool("pending", false, "Push the plan in a pending state")
	flags.String("repo", "", "URL to the schema repository")
	flags.String("format", "", "Atlas Go template output format")
	flags.StringArrayP("directive", "d", nil, "Directives for the migration plan")
	flags.StringArray("include", nil, "Schema objects to include in planning")
	flags.String("lock-timeout", "", "Timeout for acquiring the database lock")
	cmd.MarkFlagsMutuallyExclusive("save", "dry-run")
	cmd.MarkFlagsMutuallyExclusive("output", "dry-run")
	// Both spell the same field. Atlas registers both and its precedence is
	// unmeasured (the licensed capture is help text only), so refusing the
	// combination is preferred over silently picking a winner and writing a
	// differently named plan file than the operator asked for.
	cmd.MarkFlagsMutuallyExclusive("name", "name-format")
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
	// Everything that can refuse the plan without looking at its statements
	// runs before the editor. An edit is operator work, and throwing it away
	// over a format or naming problem that was decidable beforehand loses it
	// for nothing: the editor's buffer is a temporary file this command
	// deletes. Nothing below depends on the statement text — the name template
	// reads only fingerprints, which an edit cannot change.
	outputPath := strings.TrimSpace(opts.output)
	format := atlasSchemaPlanFormat(outputPath)
	if err := atlasschema.CheckPlanFormatSupported(plan, format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	nameSource := "--name"
	switch {
	case opts.nameFormat != "":
		nameSource = "--name-format"
		plan.Name, err = renderAtlasSchemaPlanName(opts.nameFormat, plan)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	// The Atlas plan format names plans with a UTC timestamp; keep the
	// deterministic fingerprint-derived default for the native JSON format.
	case format == atlasschema.PlanFormatHCL && strings.TrimSpace(opts.name) == "":
		plan.Name = atlasschema.TimestampPlanName(time.Now())
	}
	if opts.edit {
		plan, err = editAtlasSchemaPlan(plan)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
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
			// The remediation names whichever flag produced this name:
			// --name and --name-format are mutually exclusive, so suggesting
			// --name to someone who used --name-format sends them at a
			// combination the command refuses.
			return cmdutil.Fail(cmd, fmt.Errorf(
				"plan file %s already exists; pass %s or --output to choose a distinct plan file", path, nameSource))
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

// editAtlasSchemaPlan opens the planned SQL in the operator's editor and
// returns the plan rebuilt from the edited text. Severity and the destructive
// marker are re-derived so the saved plan describes the SQL it actually
// carries; the fingerprints are not, for the reasons documented on
// [atlasschema.PlanFile.WithStatementsFromSQL].
//
// An edit that leaves no executable statement is refused rather than saved:
// an empty plan file would apply cleanly and change nothing, which reads as
// success. Note that the statement splitter strips SQL comments, so comments
// added in the editor do not survive into the plan file.
func editAtlasSchemaPlan(plan atlasschema.PlanFile) (atlasschema.PlanFile, error) {
	edited, err := editAtlasSQL("schema plan", plan.SQL())
	if err != nil {
		return atlasschema.PlanFile{}, err
	}
	plan = plan.WithStatementsFromSQL(edited)
	if !plan.HasChanges() {
		return atlasschema.PlanFile{}, fmt.Errorf(
			"the edited plan contains no SQL statement; nothing was saved")
	}
	return plan, nil
}

// renderAtlasSchemaPlanName computes the plan name from the --name-format Go
// template. The template sees the plan's own fingerprints under the field
// names the licensed Atlas binary documents.
func renderAtlasSchemaPlanName(nameFormat string, plan atlasschema.PlanFile) (string, error) {
	name, err := atlasreport.RenderSchemaPlanName(nameFormat, atlasreport.SchemaPlanName{
		FromHash: plan.FromFingerprint,
		ToHash:   plan.ToFingerprint,
	})
	if err != nil {
		return "", err
	}
	if err := validateAtlasSchemaPlanName("--name-format", name); err != nil {
		return "", err
	}
	return name, nil
}

// atlasPlanNameIllegalWindowsChars are the characters Windows forbids in a file
// name and POSIX does not. They are refused on every platform so a plan file
// written on Linux stays readable on Windows, which goreleaser builds for.
// `:` is the one that bites in practice: it is NTFS's alternate-data-stream
// separator, so `plan_sha256:9.plan.hcl` would create an empty `plan_sha256`
// with the document hidden in a stream, under a cheerful success message.
const atlasPlanNameIllegalWindowsChars = `:*?"<>|`

// validateAtlasSchemaPlanName rejects plan names that cannot safely become a
// plan file name or a plan block label. source names the flag the name came
// from, so the same rules can back --name and --name-format with a message
// that points at the right flag.
//
// Control characters are refused because a template emitting a stray newline
// would otherwise produce both an unusable file name and a plan block label the
// HCL writer has to reject later, with a message pointing at the writer rather
// than at the template. `.` and `..` are refused because they name a directory
// rather than a plan.
func validateAtlasSchemaPlanName(source, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%s: the plan name is empty", source)
	case name == "." || name == "..":
		return fmt.Errorf("%s: the plan name %q is a directory reference, not a name", source, name)
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf(
			"%s: the plan name %q contains a path separator; use --output to choose the plan file location",
			source, name)
	case strings.ContainsAny(name, atlasPlanNameIllegalWindowsChars):
		return fmt.Errorf(
			"%s: the plan name %q contains one of %s, which cannot appear in a file name on Windows",
			source, name, atlasPlanNameIllegalWindowsChars)
	case strings.ContainsFunc(name, unicode.IsControl):
		return fmt.Errorf("%s: the plan name %q contains a control character", source, name)
	}
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
	// An empty --name means "use the default name", so only a supplied one is
	// checked. Everything else that makes a name unusable applies to both
	// flags, so they share one validator.
	if opts.name != "" {
		if err := validateAtlasSchemaPlanName("--name", opts.name); err != nil {
			return err
		}
	}
	// Parse the name template before any database work, so a malformed
	// template fails with no connection opened and nothing written.
	if cmd.Flags().Changed("name-format") {
		if opts.nameFormat == "" {
			return fmt.Errorf("--name-format must not be empty")
		}
		if err := atlasreport.ValidateSchemaPlanNameTemplate(opts.nameFormat); err != nil {
			return err
		}
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
		{"format", "Ptah does not implement --format for schema plan yet"},
		{"directive", "Ptah does not implement Atlas plan directives yet; the plan file records only the migration SQL"},
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
