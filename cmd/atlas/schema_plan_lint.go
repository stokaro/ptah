package atlas

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/planlint"
)

// atlasSchemaPlanLintFailOnErrorEnvVar turns the plan report into a gate: an
// error-severity finding makes the command exit 1 instead of 0.
//
// It exists because this verb reports from a position pipelines gate on, and
// the report alone cannot be gated on without parsing it. The default is off,
// because a report is what the verb produces: a plan carrying a destructive
// change is a plan an operator is expected to read and approve, not one the
// tool refuses on their behalf, and turning that into a failure by default
// would break a working pipeline the first time it upgraded.
//
// It is an environment variable rather than a flag because the conformance
// cli-surface tier asserts that ptah-compat registers exactly the flags of the
// surface it stands in for; precedent and spelling:
// [go.5x5.cz/ptah/internal/atlashclrender.KeepAtlasRefusedBlocksEnvVar].
const atlasSchemaPlanLintFailOnErrorEnvVar = "PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR"

// atlasPlanLintFailOnError is the declaration of the variable, made once, on
// the verb that owns it. See [go.5x5.cz/ptah/internal/envbool].
// It is [go.5x5.cz/ptah/internal/envbool.Gated]: the pinned community binary
// v1.3.0 refuses the whole `schema plan` path, so failing a plan review on a
// lint finding is behavior it does not have at all.
var atlasPlanLintFailOnError = envbool.New(atlasSchemaPlanLintFailOnErrorEnvVar, false, envbool.Gated)

// atlasSchemaPlanLintGatesOnFindings reports whether the opt-in threshold is
// on. Unset keeps the reporting default and a valid false spelling keeps it
// too; an empty or unparsable value is a configuration error.
func atlasSchemaPlanLintGatesOnFindings() (bool, error) {
	return atlasPlanLintFailOnError.Resolve()
}

// atlasSchemaPlanLintFindingError is what a gated run fails with. It names the
// threshold rather than the finding, because the findings are already on
// stdout and a second copy in the error would print one of them twice.
const atlasSchemaPlanLintFindingError = "plan lint findings reached the failure threshold"

type atlasSchemaPlanLintOptions struct {
	atlasSchemaPlanTransitionFlags
	file string
}

// newAtlasSchemaPlanLintCommand implements `atlas schema plan lint`.
//
// # Surface
//
// Flag set, per the published Atlas CLI reference
// (https://atlasgo.io/cli-reference, entry "atlas schema plan lint"): the
// transition set shared by every `schema plan` verb plus -f/--file, and no
// --url. That is the same set `schema plan validate` carries, and the absence
// of --url is what makes this sub-verb local rather than registry-bound.
//
// # What it does, and what it deliberately does not do
//
// The verb answers one question — "what would this plan do to a database?" —
// and it answers it in two parts.
//
// First the plan file has to describe the transition it is being read against.
// That is [verifyAtlasSchemaPlanFile], the same verification `schema plan
// validate` and `schema apply --plan` run, and it is not optional here: a lint
// report about a plan that does not describe this transition would be an
// accurate report about a change nobody is going to make, which is worse than
// no report. A plan that fails it is refused, and no report is printed.
//
// Then the plan's statements are analyzed by Ptah's migration lint rules and
// the findings are printed. Analysis is the same analysis `ptah-compat migrate
// lint` runs over a migration file holding the same SQL — same rules, same
// codes, same `atlas:nolint` directives — so a hazard reported in one place is
// reported in the other, and neither surface has a private rule set.
//
// **Findings do not decide the exit code.** A plan carrying a destructive
// change exits 0 with the change described on stdout. That is the shape this
// verb is for: a plan is a document an operator reviews and approves, the lint
// report is what they review it with, and a report that refuses on their behalf
// is a report they cannot use to approve anything. The gate is available and
// off by default; see [atlasSchemaPlanLintFailOnErrorEnvVar].
//
// That places a duty on the report itself. Ptah's rule set is its own and does
// not name every hazard a schema change can carry, so "no diagnostics found"
// has to mean "these rules found nothing", not "this plan is safe". The verb
// says exactly that on stderr, on every run, precisely because it cannot be
// inferred from an exit code of 0 — see [writeAtlasSchemaPlanLintCoverage].
//
// --format is refused rather than implemented. Rendering the analysis through a
// caller's template is a second output contract, and the one this verb has is
// the report on stdout; the refusal is shared with the sibling plan verbs.
func newAtlasSchemaPlanLintCommand() *cobra.Command {
	opts := atlasSchemaPlanLintOptions{}
	cmd := &cobra.Command{
		Use:   "lint",
		Short: "Run analysis (migration linting) on a plan file",
		Long: `Atlas ` + "`atlas schema plan lint`" + ` command path.

Analyzes the SQL of the plan file named by --file and prints what Ptah's
migration lint rules find, without changing the target database.

The plan is verified before it is analyzed, with the same two checks
` + "`schema plan validate`" + ` runs:

  - the plan's recorded from-fingerprint must match the live --from database,
    for plans carrying Ptah's own sha256 fingerprints; and
  - the plan's statements are replayed on a dev database seeded from the
    target's current schema, and the state they reach must equal --to.

A plan that fails either check is refused and nothing is analyzed: a report
about a plan that does not describe this transition would describe a change
nobody is about to make. A SQLite target gets a throwaway dev database; every
other dialect requires --dev-url.

The analysis is the one ` + "`ptah-compat migrate lint`" + ` runs over a migration file
holding the same SQL: the same rules, the same codes, and the same
` + "`atlas:nolint`" + ` directives silence it.

Findings are reported and do not change the exit code — a plan carrying a
destructive change is a plan to review, not one this command refuses on your
behalf. Set ` + "`PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR=1`" + ` to exit 1 when an
error-severity finding is reported. Because the rules are Ptah's own and name
fewer hazards than exist, a clean report is a statement about the rules and not
a guarantee about the plan; every run says so on stderr.

--repo, --format, --lock-timeout, --schema, --include and --exclude are
declared for CLI-surface parity and refused; --auto-approve is accepted and
does nothing, since a local plan file is approved by operator review.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaPlanLint(cmd, opts)
		},
	}
	flags := cmd.Flags()
	registerAtlasSchemaPlanTransitionFlags(flags, &opts.atlasSchemaPlanTransitionFlags)
	flags.StringVarP(&opts.file, atlasFileFlagName, atlasFileFlagShorthand, "",
		"URL to the plan file to analyze (file://path/to/file.plan.hcl)")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaPlanLint(cmd *cobra.Command, opts atlasSchemaPlanLintOptions) error {
	// Resolved first, before the project file is opened and before any database
	// is touched: a malformed value must not stay dormant on the runs that
	// report nothing, because those are the whole of a healthy pipeline and the
	// typo would survive every one of them.
	gateOnFindings, err := atlasSchemaPlanLintGatesOnFindings()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	transition, _, err := resolveAtlasSchemaPlanTransitionConfig(
		cmd, atlasSchemaPlanLintVerb, opts.atlasSchemaPlanTransitionFlags)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	opts.atlasSchemaPlanTransitionFlags = transition
	if err := validateAtlasSchemaPlanLintOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	verified, err := verifyAtlasSchemaPlanFile(
		cmd, atlasSchemaPlanLintVerb, opts.file, opts.atlasSchemaPlanTransitionFlags)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	analysis, err := planlint.Analyze(verified.plan.SQL(), verified.dialect)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := atlasreport.WriteSchemaPlanLintText(cmd.OutOrStdout(), atlasreport.SchemaPlanLintOptions{
		Analysis: &analysis,
	}); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	threshold := atlasSchemaPlanLintReportingNote
	if gateOnFindings {
		threshold = atlasSchemaPlanLintGatingNote
	}
	if err := writeAtlasSchemaPlanLintCoverage(cmd, threshold); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if gateOnFindings && planlint.HasErrorSeverity(analysis) {
		return exitcode.New(1, errors.New(atlasSchemaPlanLintFindingError))
	}
	return nil
}

// The stderr note every run prints, in two parts: what the report covers, and
// what the exit code means for it.
//
// It is unconditional rather than printed only alongside findings, because the
// run it matters most on is the clean one: an operator reading "no diagnostics
// found" and an exit code of 0 has every reason to read that as "this plan is
// safe", and the rules that produced it do not support that reading. Saying so
// only when something was found would leave the silent case — the one that
// looks like an all-clear — as the one case with no caveat on it.
//
// stderr rather than stdout so the report stays exactly the report: a pipeline
// capturing stdout keeps a document of findings and nothing else.
const (
	atlasSchemaPlanLintCoverageNote = "Analyzed with Ptah's migration lint rules. They do not name" +
		" every hazard a schema\nchange can carry, so a report without findings describes the rules" +
		" and not the plan."
	atlasSchemaPlanLintReportingNote = "Findings do not change the exit code; set " +
		atlasSchemaPlanLintFailOnErrorEnvVar + "=1 to exit 1\non an error-severity finding."
	atlasSchemaPlanLintGatingNote = atlasSchemaPlanLintFailOnErrorEnvVar +
		" is set: an error-severity finding exits 1."
)

// writeAtlasSchemaPlanLintCoverage states, on stderr, what the report on stdout
// is and is not. threshold is the sentence describing what the exit code means
// on this run.
func writeAtlasSchemaPlanLintCoverage(cmd *cobra.Command, threshold string) error {
	_, err := fmt.Fprintf(cmd.ErrOrStderr(), "%s\n%s\n", atlasSchemaPlanLintCoverageNote, threshold)
	return err
}

// validateAtlasSchemaPlanLintOptions checks the inputs `lint` needs on top of
// the shared transition, and refuses the flags whose meaning here would have to
// be invented.
func validateAtlasSchemaPlanLintOptions(cmd *cobra.Command, opts atlasSchemaPlanLintOptions) error {
	if err := validateAtlasSchemaPlanTransition(
		cmd, atlasSchemaPlanLintVerb, opts.atlasSchemaPlanTransitionFlags); err != nil {
		return err
	}
	if err := requireAtlasSchemaPlanFile(atlasSchemaPlanLintVerb, opts.file); err != nil {
		return err
	}
	return rejectAtlasSchemaPlanFileExclude(cmd, atlasSchemaPlanLintVerb)
}
