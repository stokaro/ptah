package atlas

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/migrator"
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
// valid that `apply` then refuses.
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

	planPath, err := atlasSchemaPlanValidateFilePath(opts.file)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	plan, planFormat, err := atlasschema.ReadPlanDocument(planPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.fromURLs[0])
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --from: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	desired, err := schemafile.LoadAll(opts.toURLs, schemafile.Options{
		Dialect:               conn.Info().Dialect,
		IgnoreUnknownHCLNames: true,
	})
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("load --to schema: %w", err))
	}

	// The from-state gate, on the same terms as `schema apply --plan`: the
	// fingerprint SHAPE never decides whether a check runs, because the
	// `sha256:<hex>` derivation is public and forgeable. A JSON plan is always
	// checked; an HCL plan only when its recorded fingerprint is one Ptah can
	// recompute, and the replay below covers the rest either way.
	if planFormat == atlasschema.PlanFormatJSON || atlasschema.IsNativeFingerprint(plan.FromFingerprint) {
		if err := atlasschema.VerifyPlanTarget(conn, plan); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}

	// One dialect-aware statement list, split the way the apply path splits it,
	// so what validate verifies is what apply would run.
	statements := atlasschema.SplitApplyStatements(plan.SQL(), conn.Info().Dialect)
	if err := rehearseAtlasSchemaApplyPlan(cmd, conn, rehearsePlanParams{
		// Never skip the replay: a matching from-fingerprint says the plan was
		// computed against this database, not that its statements reach --to,
		// and the second question is the one this verb exists to answer.
		policy:     rehearseAlways,
		format:     planFormat,
		statements: statements,
		desired:    desired,
		exclude:    plan.Exclude,
		// The replay answers "do these statements reach --to", not "do they
		// apply under a particular transaction mode": Atlas registers no
		// --tx-mode on this sub-verb, so there is no operator input to honor,
		// and wrapping the replay in a transaction would refuse plans that
		// apply cleanly (PostgreSQL CREATE INDEX CONCURRENTLY is the standing
		// example). A plan that needs a specific transaction mode still gets
		// that verdict, loudly, from `schema apply --plan --tx-mode`.
		txMode: migrator.MigrationTxModeNone,
		devURL: opts.devURL,
		// targetURL and desiredURLs are what let the simulation refuse a
		// --dev-url that resolves to something it must not destroy: a dev
		// database is reset before the plan is replayed on it. Dropping
		// targetURL turns this verb into one that silently migrates and empties
		// the target and still exits 0 — measured, so it is pinned by
		// TestSchemaPlanValidateLeavesTheTargetDatabaseUnchanged.
		//
		// desiredURLs cannot collide today, because --to is restricted to local
		// schema files here and a dev URL is a database URL. It is passed anyway
		// so the guard already holds if --to ever accepts a database URL; that
		// is why there is no mutation for it — nothing could kill one.
		targetURL:   opts.fromURLs[0],
		desiredURLs: opts.toURLs,
	}); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// Success is silent on stdout. The shape of a successful Atlas plan
	// validation is unmeasured, and an empty stdout cannot be the wrong shape;
	// the documented sibling `atlas schema validate` also exits successfully
	// with no output (https://atlasgo.io/cli-reference).
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
	if opts.file == "" {
		return fmt.Errorf("--file is required: %s validates an existing plan file", atlasSchemaPlanValidateVerb)
	}
	// A JSON plan records the exclude patterns it was computed with and the
	// Atlas `.plan.hcl` shape cannot record them at all, so honoring
	// flag-supplied patterns would compare the plan against a state it never
	// described. Refusing beats quietly validating the wrong transition.
	if cmd.Flags().Changed("exclude") {
		return fmt.Errorf(
			"%s accepts --exclude, but a plan file records the exclude patterns it was computed with "+
				"(and the Atlas .plan.hcl format records none at all), so flag-supplied patterns would verify "+
				"a different transition than the plan describes", atlasSchemaPlanValidateVerb)
	}
	return nil
}

// atlasSchemaPlanValidateFilePath resolves a --file URL to a local plan-file
// path. Registry URLs are rejected the same way `schema apply --plan` rejects
// them: Ptah has no plan registry, and the open replacement is a local file.
func atlasSchemaPlanValidateFilePath(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
		return "", fmt.Errorf("%s accepts registry plan URLs like %q, but Ptah has no plan registry; "+
			"pass a local plan file saved by `schema plan` as --file file://<path>", atlasSchemaPlanValidateVerb, raw)
	}
	path, err := schemafile.LocalFilePath(trimmed)
	if err != nil {
		return "", fmt.Errorf("--file %q: %w", raw, err)
	}
	return path, nil
}
