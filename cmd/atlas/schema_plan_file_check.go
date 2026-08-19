package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/migrator"
)

// verifiedAtlasSchemaPlan is a plan file that has been read and checked against
// the transition it claims to describe.
type verifiedAtlasSchemaPlan struct {
	// plan is the plan document exactly as read.
	plan atlasschema.PlanFile
	// dialect is the --from target's dialect, established while the plan was
	// being checked against that database. A caller that needs to split or
	// analyze the plan's SQL needs it and must not re-derive it from the URL:
	// what the connection reports is what the checks above ran under.
	dialect string
}

// verifyAtlasSchemaPlanFile reads the plan file named by file and runs the two
// checks `schema apply --plan` runs before it touches the target:
//
//  1. the plan's recorded from-fingerprint must match the live --from database,
//     for the plans whose fingerprints Ptah can recompute; and
//  2. the plan's statements, replayed on a dev database seeded from the
//     target's current schema, must reach the --to desired state.
//
// It is one function rather than one per verb because every verb that reads a
// plan file has to answer the same question before it may say anything about
// that plan. `schema plan validate` exists to answer it; `schema plan lint`
// reports on statements whose meaning depends on it, and a lint report about a
// plan that does not describe this transition would describe a change nobody
// is about to make. A second copy of these checks would be free to be weaker
// than this one, which is the failure mode worth designing out.
//
// The target database is never written: the replay happens on the dev database,
// and the simulation refuses a dev URL that resolves to the target or to a
// --to source.
//
// Call it after [validateAtlasSchemaPlanTransition] has accepted the same
// transition. It reads transition.fromURLs[0] directly, because "exactly one
// --from database URL" is that validator's answer and re-deriving it here would
// be a second sentence with the same job.
func verifyAtlasSchemaPlanFile(
	cmd *cobra.Command,
	verb string,
	file string,
	transition atlasSchemaPlanTransitionFlags,
) (verifiedAtlasSchemaPlan, error) {
	planPath, err := atlasSchemaPlanFilePath(verb, file)
	if err != nil {
		return verifiedAtlasSchemaPlan{}, err
	}
	plan, planFormat, err := atlasschema.ReadPlanDocument(planPath)
	if err != nil {
		return verifiedAtlasSchemaPlan{}, err
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, transition.fromURLs[0])
	if err != nil {
		return verifiedAtlasSchemaPlan{}, fmt.Errorf("connect to --from: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	schemaVars, err := atlasVarFlagValues(cmd)
	if err != nil {
		return verifiedAtlasSchemaPlan{}, err
	}
	schemaScope, schemaScopeFlag := schemafile.ScopeFromURLs(transition.devURL, transition.fromURLs[0], "from")
	// The sources rather than the URLs: a desired state the env selected
	// through `data "hcl_schema"` carries that block's vars, and the plan verbs
	// load local files directly instead of classifying them.
	desired, err := schemafile.LoadSources(atlasSchemaPlanSources(transition), schemafile.Options{
		Dialect:               conn.Info().Dialect,
		IgnoreUnknownHCLNames: true,
		SchemaScope:           schemaScope,
		SchemaScopeFlag:       schemaScopeFlag,
		Vars:                  schemaVars,
	})
	if err != nil {
		return verifiedAtlasSchemaPlan{}, fmt.Errorf("load --to schema: %w", err)
	}

	// The from-state gate, on the same terms as `schema apply --plan`: the
	// fingerprint SHAPE never decides whether a check runs, because the
	// `sha256:<hex>` derivation is public and forgeable. A JSON plan is always
	// checked; an HCL plan only when its recorded fingerprint is one Ptah can
	// recompute, and the replay below covers the rest either way.
	if planFormat == atlasschema.PlanFormatJSON || atlasschema.IsNativeFingerprint(plan.FromFingerprint) {
		if err := atlasschema.VerifyPlanTarget(conn, plan); err != nil {
			return verifiedAtlasSchemaPlan{}, err
		}
	}

	// One dialect-aware statement list, split the way the apply path splits it,
	// so what is verified here is what apply would run.
	statements := atlasschema.SplitApplyStatements(plan.SQL(), conn.Info().Dialect)
	if err := rehearseAtlasSchemaApplyPlan(cmd, conn, rehearsePlanParams{
		// Never skip the replay: a matching from-fingerprint says the plan was
		// computed against this database, not that its statements reach --to,
		// and the second question is the one these verbs exist to answer.
		policy:     rehearseAlways,
		format:     planFormat,
		statements: statements,
		desired:    desired,
		exclude:    plan.Exclude,
		// The replay answers "do these statements reach --to", not "do they
		// apply under a particular transaction mode": Atlas registers no
		// --tx-mode on these sub-verbs, so there is no operator input to honor,
		// and wrapping the replay in a transaction would refuse plans that
		// apply cleanly (PostgreSQL CREATE INDEX CONCURRENTLY is the standing
		// example). A plan that needs a specific transaction mode still gets
		// that verdict, loudly, from `schema apply --plan --tx-mode`.
		txMode: migrator.MigrationTxModeNone,
		devURL: transition.devURL,
		// targetURL and desiredURLs are what let the simulation refuse a
		// --dev-url that resolves to something it must not destroy: a dev
		// database is reset before the plan is replayed on it. Dropping
		// targetURL turns these verbs into ones that silently migrate and empty
		// the target and still exit 0 — measured, so it is pinned by
		// TestSchemaPlanValidateLeavesTheTargetDatabaseUnchanged.
		//
		// desiredURLs cannot collide today, because --to is restricted to local
		// schema files here and a dev URL is a database URL. It is passed anyway
		// so the guard already holds if --to ever accepts a database URL; that
		// is why there is no mutation for it — nothing could kill one.
		targetURL:   transition.fromURLs[0],
		desiredURLs: transition.toURLs,
	}); err != nil {
		return verifiedAtlasSchemaPlan{}, err
	}
	return verifiedAtlasSchemaPlan{plan: plan, dialect: conn.Info().Dialect}, nil
}

// atlasSchemaPlanFilePath resolves a --file URL to a local plan-file path.
// Registry URLs are rejected the same way `schema apply --plan` rejects them:
// Ptah has no plan registry, and the open replacement is a local file. verb
// names the invoked command so a sub-verb's diagnostic points at the command
// the operator actually ran.
func atlasSchemaPlanFilePath(verb, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
		return "", fmt.Errorf("%s accepts registry plan URLs like %q, but Ptah has no plan registry; "+
			"pass a local plan file saved by `schema plan` as --file file://<path>", verb, raw)
	}
	path, err := schemafile.LocalFilePath(trimmed)
	if err != nil {
		return "", fmt.Errorf("--file %q: %w", raw, err)
	}
	return path, nil
}

// requireAtlasSchemaPlanFile refuses a plan-file verb invoked without one.
func requireAtlasSchemaPlanFile(verb, file string) error {
	if strings.TrimSpace(file) != "" {
		return nil
	}
	return fmt.Errorf("--file is required: %s reads an existing plan file", verb)
}

// rejectAtlasSchemaPlanFileExclude refuses flag-supplied --exclude on a verb
// that reads a plan file.
//
// A JSON plan records the exclude patterns it was computed with and the Atlas
// `.plan.hcl` shape cannot record them at all, so honoring flag-supplied
// patterns would compare the plan against a state it never described. Refusing
// beats quietly answering for the wrong transition.
func rejectAtlasSchemaPlanFileExclude(cmd *cobra.Command, verb string) error {
	if !cmd.Flags().Changed("exclude") {
		return nil
	}
	return fmt.Errorf(
		"%s accepts --exclude, but a plan file records the exclude patterns it was computed with "+
			"(and the Atlas .plan.hcl format records none at all), so flag-supplied patterns would verify "+
			"a different transition than the plan describes", verb)
}
