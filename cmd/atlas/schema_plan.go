package atlas

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"
	"unicode"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
)

type atlasSchemaPlanOptions struct {
	atlasSchemaPlanTransitionFlags
	// verb is the Atlas command the operator invoked. `schema plan new`
	// delegates to the same run function, and a diagnostic naming the wrong
	// command is a diagnostic the operator cannot act on.
	verb       string
	name       string
	nameFormat string
	output     string
	save       bool
	dryRun     bool
	edit       bool
}

type atlasSchemaPlanOutputMode uint8

const (
	atlasSchemaPlanDefaultOutput atlasSchemaPlanOutputMode = iota
	atlasSchemaPlanExplicitOutput
)

func newAtlasSchemaPlanCommand() *cobra.Command {
	opts := atlasSchemaPlanOptions{verb: atlasSchemaPlanVerb}
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
.ToHash, which expose this plan's own digest bytes in Atlas's untagged Base64
representation; it cannot be combined with --name. Standard Base64 can contain
/, so a rendered path separator requires an explicit --output. When --env is set, the
selected atlas.hcl env can provide url
(the --from target), schema.src, dev, exclude, schema.mode, and supported
diff policy values. Registry-bound planning (--push, --pending, --repo),
--format and --directive remain unimplemented.

Two local sub-verbs are implemented: ` + "`new`" + ` creates a plan file for the
transition and ` + "`validate`" + ` checks an existing plan file against it. The
registry sub-verbs (approve, list, pull, push, rm) and the ` + "`lint`" + ` and
` + "`test`" + ` sub-verbs remain unimplemented.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaPlan(cmd, opts)
		},
	}
	flags := cmd.Flags()
	registerAtlasSchemaPlanTransitionFlags(flags, &opts.atlasSchemaPlanTransitionFlags)
	flags.StringVar(&opts.name, "name", "", "Plan name recorded in the plan file")
	flags.StringVar(&opts.nameFormat, "name-format", "", "Go template used to compute the plan name (standard Base64 may render '/', which requires --output)")
	flags.StringVarP(&opts.output, "output", "o", "", "Plan file output path (default <name>"+atlasschema.PlanFileSuffixHCL+"; a .json path writes the native JSON plan format)")
	flags.BoolVar(&opts.save, "save", false, "Save the plan to a local plan file")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Print the plan file document without saving it")
	flags.BoolVar(&opts.edit, "edit", false, "Edit the plan in the terminal editor")
	// Accepted as an explicit no-op: `schema plan` runs no lint step, so there
	// is nothing for --skip-lint to skip. Refusing it would break a Pro
	// pipeline that passes it, and honoring it cannot loosen a check that does
	// not exist. Pinned by TestSchemaPlanSkipLintIsANoOp — if a plan linter is
	// ever added, that test goes red and this comment stops being true.
	flags.Bool("skip-lint", false, "Skip linting the migration plan")
	// The remaining hosted plan flags are declared for CLI-surface parity
	// and rejected loudly in validateAtlasSchemaPlanOptions: their behavior is
	// either bound to a remote registry or not implemented yet. The rest of
	// the rejected set is registered by registerAtlasSchemaPlanTransitionFlags,
	// which the local sub-verbs share; --push, --pending and --directive are
	// registered here because the Atlas help capture shows them only on the
	// parent verb.
	flags.Bool("push", false, "Push the plan to a remote registry")
	flags.Bool("pending", false, "Push the plan in a pending state")
	flags.StringArrayP("directive", "d", nil, "Directives for the migration plan")
	cmd.MarkFlagsMutuallyExclusive("save", "dry-run")
	cmd.MarkFlagsMutuallyExclusive("output", "dry-run")
	// Both spell the same field. Atlas registers both and its precedence is
	// unmeasured (Atlas publishes help text only), so refusing the
	// combination is preferred over silently picking a winner and writing a
	// differently named plan file than the operator asked for.
	cmd.MarkFlagsMutuallyExclusive("name", "name-format")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	cmd.AddCommand(newAtlasSchemaPlanNewCommand())
	cmd.AddCommand(newAtlasSchemaPlanValidateCommand())
	// The remaining sub-verbs stay unsupported-boundary stubs, for two
	// different reasons, and their descriptions must not blur them: approve,
	// list, pull, push and rm take --url and arbitrate plan state in a remote
	// registry, which the local plan-file workflow replaces; lint and test take
	// no --url and are local, deferred only because neither has a measured
	// contract. Describing lint or test as registry work would assert a
	// dependency they do not have, so they keep Atlas's own descriptions. The
	// reasons they are deferred rather than guessed are recorded on
	// unsupportedCommandTests.
	addAtlasUnsupportedCommands(cmd, []atlasUnsupportedVerb{
		{use: "approve", short: "Approve a plan in a remote registry"},
		{use: "lint", short: "Run analysis (migration linting) on a plan file"},
		{use: "list", short: "List plans in a remote registry"},
		{use: "pull", short: "Pull a plan from a remote registry"},
		{use: "push", short: "Push a plan to a remote registry"},
		{use: "rm", short: "Remove a plan from a remote registry"},
		{use: "test", short: "Run schema plan tests"},
	})
	return cmd
}

func runAtlasSchemaPlan(cmd *cobra.Command, opts atlasSchemaPlanOptions) error {
	transition, policy, err := resolveAtlasSchemaPlanTransitionConfig(
		cmd, opts.verb, opts.atlasSchemaPlanTransitionFlags)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	opts.atlasSchemaPlanTransitionFlags = transition
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
	outputMode := atlasSchemaPlanOutputModeFor(outputPath)
	format := atlasSchemaPlanFormat(outputPath)
	if err := atlasschema.CheckPlanFormatSupported(plan, format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	nameSource := "--name"
	switch {
	case opts.nameFormat != "":
		nameSource = "--name-format"
		plan.Name, err = renderAtlasSchemaPlanName(opts.nameFormat, plan, outputMode)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	// The Atlas plan format names plans with a UTC timestamp; keep the
	// deterministic fingerprint-derived default for the native JSON format.
	case format == atlasschema.PlanFormatHCL && strings.TrimSpace(opts.name) == "":
		plan.Name = atlasschema.TimestampPlanName(time.Now())
	}
	if opts.edit {
		plan, err = editAtlasSchemaPlan(cmd.Context(), plan)
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
	}
	if err := writeAtlasPlanDocument(path, document, outputMode); err != nil {
		if outputMode == atlasSchemaPlanDefaultOutput && errors.Is(err, fs.ErrExist) {
			return cmdutil.Fail(cmd, fmt.Errorf(
				"plan file %s already exists; pass %s or --output to choose a distinct plan file", path, nameSource))
		}
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
// success. Statement text, including comments, round-trips through the editor.
func editAtlasSchemaPlan(ctx context.Context, plan atlasschema.PlanFile) (atlasschema.PlanFile, error) {
	edited, err := editAtlasSQL(ctx, "schema plan", plan.SQL())
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
// names Atlas documents.
func renderAtlasSchemaPlanName(
	nameFormat string,
	plan atlasschema.PlanFile,
	outputMode atlasSchemaPlanOutputMode,
) (string, error) {
	data, err := atlasreport.NewSchemaPlanName(plan.FromFingerprint, plan.ToFingerprint)
	if err != nil {
		return "", err
	}
	name, err := atlasreport.RenderSchemaPlanName(nameFormat, data)
	if err != nil {
		return "", err
	}
	if err := validateAtlasSchemaPlanName("--name-format", name, outputMode); err != nil {
		return "", err
	}
	return name, nil
}

// atlasPlanNameIllegalWindowsChars are the characters Windows forbids in a file
// name and POSIX does not. They are refused on every platform so a plan file
// written on Linux stays readable on Windows, which goreleaser builds for.
// In particular, `:` is NTFS's alternate-data-stream separator: accepting it
// could create an empty visible file with the plan hidden in a named stream.
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
func validateAtlasSchemaPlanName(source, name string, outputMode atlasSchemaPlanOutputMode) error {
	switch {
	case name == "":
		return fmt.Errorf("%s: the plan name is empty", source)
	case name == "." || name == "..":
		return fmt.Errorf("%s: the plan name %q is a directory reference, not a name", source, name)
	case outputMode == atlasSchemaPlanDefaultOutput && strings.ContainsAny(name, `/\`):
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

func atlasSchemaPlanOutputModeFor(outputPath string) atlasSchemaPlanOutputMode {
	if outputPath != "" {
		return atlasSchemaPlanExplicitOutput
	}
	return atlasSchemaPlanDefaultOutput
}

func needsAtlasSchemaPlanConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed(atlasFromFlagName) || !cmd.Flags().Changed("to")
}

func validateAtlasSchemaPlanOptions(cmd *cobra.Command, opts atlasSchemaPlanOptions) error {
	if err := validateAtlasSchemaPlanTransition(
		cmd, opts.verb, opts.atlasSchemaPlanTransitionFlags); err != nil {
		return err
	}
	// An empty --name means "use the default name", so only a supplied one is
	// checked. Everything else that makes a name unusable applies to both
	// flags, so they share one validator.
	if opts.name != "" {
		if err := validateAtlasSchemaPlanName(
			"--name",
			opts.name,
			atlasSchemaPlanOutputModeFor(strings.TrimSpace(opts.output)),
		); err != nil {
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
//
// Atlas documents `--from file://schema.hcl` on `schema plan validate`
// (https://atlasgo.io/cli-reference, retrieved 2026-08-02), so this is a
// divergence, not parity: a fingerprint over a file has nothing to be stale
// against.
func ensureAtlasPlanDatabaseURL(verb, raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !strings.Contains(trimmed, "://") || strings.HasPrefix(trimmed, "file://") {
		return fmt.Errorf("%s requires --from to be the target database URL the plan will be applied to "+
			"(got %q); local desired-state schema files belong in --to", verb, raw)
	}
	return nil
}

// rejectUnimplementedAtlasSchemaPlanFlags refuses the transition flags that
// `schema plan` and its local sub-verbs declare for CLI-surface parity but do
// not implement. verb names the invoked command so a sub-verb's diagnostic
// points at the command the operator actually ran.
//
// Flags absent from a sub-verb's Atlas flag set are simply not registered
// there; pflag reports an unregistered flag as unchanged, so the same table
// serves every verb without claiming a flag exists where Atlas has none.
func rejectUnimplementedAtlasSchemaPlanFlags(
	cmd *cobra.Command,
	verb string,
	transition atlasSchemaPlanTransitionFlags,
) error {
	rejections := []struct {
		flag   string
		reason string
	}{
		{"push", "plan push requires a hosted registry; Ptah's local plan workflow saves plan files with --save or --output instead"},
		{"pending", "pending plans require a hosted approval state; a locally saved plan file is approved by operator review"},
		{"repo", "schema repositories require a hosted registry; Ptah plans are local files"},
		{"format", "Ptah does not implement --format for schema plan yet"},
		{"directive", "Ptah does not implement Atlas plan directives yet; the plan file records only the migration SQL"},
		{"lock-timeout", "Ptah does not implement database lock waiting yet"},
	}
	for _, rejection := range rejections {
		if cmd.Flags().Changed(rejection.flag) {
			return fmt.Errorf("%s accepts --%s, but %s", verb, rejection.flag, rejection.reason)
		}
	}
	if len(transition.schemas) > 0 {
		return fmt.Errorf("%s accepts --schema, but Ptah only supports local schema files for this command yet", verb)
	}
	if values, err := cmd.Flags().GetStringArray("include"); err == nil && len(values) > 0 {
		return fmt.Errorf("%s accepts --include, but Ptah only supports local schema files for this command yet", verb)
	}
	return nil
}
