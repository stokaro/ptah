package atlas

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
)

type atlasSchemaPlanValidateOptions struct {
	atlasSchemaPlanTransitionFlags
	file string
}

// newAtlasSchemaPlanValidateCommand implements
// `atlas schema plan validate`.
//
// # Evidence
//
// Flag set, per the published Atlas CLI reference
// (https://atlasgo.io/cli-reference, entry "atlas schema plan validate"): the
// transition set plus -f/--file, and no --url, which is what makes this
// sub-verb local rather than registry-bound.
//
// Description and examples: DOCUMENTED —
// "Validate a plan file against the schema transition", with
// `atlas schema plan validate --file file://plan.hcl` and
// `--from … --to … --file …` (https://atlasgo.io/cli-reference).
//
// The rule being checked: DOCUMENTED for the plan-test case — a plan's `from`
// must match the schema state it is validated against, or the case fails
// (https://atlasgo.io/hcl/testing, https://atlasgo.io/testing/plan). Atlas
// applies the same requirement at apply time: `schema apply --plan` on an
// Atlas-format plan refuses without --to with
// `the flag "to" is required to verify the provided plan`, which this tree
// reproduces verbatim (#965).
//
// Which checks run, their order, the exit code and the output: INFERRED. None
// of it is established — CE aborts the entire `schema plan` path (reconfirmed
// on the pinned CE v1.3.0 binary, 2026-08-02), so it settles nothing here
// either.
// This provenance belongs in source and compatibility documentation, not in
// normal operator output.
//
// # Implementation
//
// `validate` runs exactly the verification `schema apply --plan` runs before it
// touches the target, and then stops:
//
//  1. the plan's `from` fingerprint must match the live --from database, for
//     the plans whose fingerprints Ptah can recompute; and
//  2. the plan's statements, replayed on a dev database from the target's
//     current schema, must reach the --to desired state.
//
// Reusing the apply-path gate rather than writing a second one is the point: a
// `validate` that verified something weaker than `apply` would report a plan as
// valid that `apply` then refuses. [verifyAtlasSchemaPlanFile] is where both
// checks live, shared with every other verb that reads a plan file.
//
// One deliberate difference: `apply` may skip (2) for a native JSON plan whose
// fingerprint already matched, because the operator is applying their own plan
// to their own database; `validate` passes rehearseAlways, because skipping the
// only semantic check would leave the verb answering a narrower question than
// it was asked.
//
// Statement equality with a freshly computed plan is deliberately NOT checked.
// Atlas documents three edit workflows that rewrite the `migration` attribute
// and leave `from`/`to` alone (https://atlasgo.io/declarative/plan), so an
// edited plan whose SQL reaches the desired state by a different route is
// valid, and check (2) is what says so.
func newAtlasSchemaPlanValidateCommand() *cobra.Command {
	opts := atlasSchemaPlanValidateOptions{}
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate a plan file against the schema transition",
		Long: `Atlas ` + "`atlas schema plan validate`" + ` command path.

Checks that the plan file named by --file describes the transition from the
--from target database to the local --to schema files, without changing the
target database.

Two checks run, and they are the same two ` + "`schema apply --plan`" + ` runs before it
touches anything:

  - the plan's recorded from-fingerprint must match the live --from database,
    for plans carrying Ptah's own sha256 fingerprints; an Atlas-written plan
    file carries Atlas hashes that have no local recipe, so this check is
    skipped for those and the replay below is the only from-state gate; and
  - the plan's statements are replayed on a dev database seeded from the
    target's current schema, and the state they reach must equal --to.

The replay always runs, in both plan formats. A matching from-fingerprint says
the plan was computed against this database, not that its statements reach
--to, and the second question is the one this command exists to answer — so
unlike ` + "`schema apply --plan`" + `, which may skip the replay for a fingerprint-verified
native plan, validate never skips it. A SQLite target gets a throwaway dev
database; every other dialect requires --dev-url.

The plan's SQL is not required to equal a freshly computed plan's: Atlas
documents editing the migration attribute of a saved plan, so what is checked
is where the statements arrive, not how they are spelled.

On success nothing is written to stdout and the exit code is 0. --repo,
--format, --lock-timeout, --schema, --include and --exclude are declared for
CLI-surface parity and refused; --auto-approve is accepted and does nothing,
since a local plan file is approved by operator review.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasSchemaPlanValidate(cmd, opts)
		},
	}
	flags := cmd.Flags()
	registerAtlasSchemaPlanTransitionFlags(flags, &opts.atlasSchemaPlanTransitionFlags)
	flags.StringVarP(&opts.file, atlasFileFlagName, atlasFileFlagShorthand, "",
		"URL to the plan file to validate (file://path/to/file.plan.hcl)")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasSchemaPlanValidate(cmd *cobra.Command, opts atlasSchemaPlanValidateOptions) error {
	transition, _, err := resolveAtlasSchemaPlanTransitionConfig(
		cmd, atlasSchemaPlanValidateVerb, opts.atlasSchemaPlanTransitionFlags)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	opts.atlasSchemaPlanTransitionFlags = transition
	if err := validateAtlasSchemaPlanValidateOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if _, err := verifyAtlasSchemaPlanFile(
		cmd, atlasSchemaPlanValidateVerb, opts.file, opts.atlasSchemaPlanTransitionFlags); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// Success is silent on stdout. The shape of a successful plan validation is
	// unmeasured, and an empty stdout cannot be the wrong shape; the documented
	// sibling `atlas schema validate` also exits successfully with no output
	// (https://atlasgo.io/cli-reference).
	return nil
}

// validateAtlasSchemaPlanValidateOptions checks the inputs `validate` needs on
// top of the shared transition, and refuses the flags whose meaning here would
// have to be invented.
func validateAtlasSchemaPlanValidateOptions(cmd *cobra.Command, opts atlasSchemaPlanValidateOptions) error {
	if err := validateAtlasSchemaPlanTransition(
		cmd, atlasSchemaPlanValidateVerb, opts.atlasSchemaPlanTransitionFlags); err != nil {
		// --to is what the plan is validated AGAINST, so its absence is the
		// same situation `schema apply --plan` reports, in Atlas's own words.
		// Only that one case is substituted: a missing --from is reported
		// first and is the more useful diagnostic.
		if errors.Is(err, errAtlasSchemaPlanMissingTo) {
			return fmt.Errorf("the flag %q is required to verify the provided plan", "to")
		}
		return err
	}
	if err := requireAtlasSchemaPlanFile(atlasSchemaPlanValidateVerb, opts.file); err != nil {
		return err
	}
	return rejectAtlasSchemaPlanFileExclude(cmd, atlasSchemaPlanValidateVerb)
}
