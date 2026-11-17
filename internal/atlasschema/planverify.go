package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/plangraph"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/readscope"
	"ptah.run/migration/migrator"
)

// PlanDesiredStateError reports that the plan does not converge the
// database to the --to desired state. Phase "rehearsal" means the mismatch
// was caught on the dev database before the target was touched; phase
// "post-apply" means the plan executed on the target but the resulting schema
// does not match the desired state.
type PlanDesiredStateError struct {
	// Phase is "rehearsal" or "post-apply".
	Phase string
	// Drift contains the typed operations for the remaining differences between
	// the reached state and the desired state.
	Drift []renderer.Fragment
}

func (e *PlanDesiredStateError) Error() string {
	drift := describeDrift(e.Drift)
	if e.Phase == "rehearsal" {
		// The claim is deliberately narrow: the plan was not applied to the
		// target by this command. Statements that could reach outside the dev
		// database are refused before the replay (see PlanEscapeError), but
		// this message does not promise anything about what a replayed
		// statement did elsewhere.
		return fmt.Sprintf(
			"pre-planned migration does not converge to the desired state: replaying the plan on the dev database, "+
				"starting from the target's current schema, left the following schema drift against --to "+
				"(the plan was not applied to the target):\n%s\n"+
				"either the target database changed since the plan was computed or the plan was computed for a "+
				"different desired state; re-run `schema plan` against the current database and review the fresh plan",
			drift)
	}
	return fmt.Sprintf(
		"schema apply --plan end-state verification failed: after applying the plan, the database does not match "+
			"the --to desired state; the remaining schema drift is:\n%s",
		drift)
}

// describeDrift is diagnostic output, never executable SQL. Native operations
// are named by owner kind instead of being encoded in comments or dropped.
func describeDrift(units []renderer.Fragment) string {
	var lines []string
	for _, unit := range units {
		if unit.Native != nil {
			lines = append(lines, "Native operation: "+string(unit.Native.Kind()))
		} else {
			lines = append(lines, strings.TrimSpace(unit.SQL))
		}
	}
	return strings.Join(lines, "\n")
}

// IsPlanDesiredStateFailure reports whether err wraps a desired-state
// verification failure.
func IsPlanDesiredStateFailure(err error) bool {
	var target *PlanDesiredStateError
	return errors.As(err, &target)
}

// PlanRehearsalOptions configures RehearsePlanStatements.
type PlanRehearsalOptions struct {
	// Runtime selects the feature services used by rehearsal and verification.
	Runtime engine.SchemaRuntime
	// DevURL is the dev database the plan is replayed on. Required; the dev
	// database is reset destructively.
	DevURL string
	// TargetURL guards against pointing the rehearsal at the target itself.
	TargetURL string
	// DesiredURLs are the raw --to values, guarded like TargetURL.
	DesiredURLs []string
	// Exclude filters the introspected current schema and both sides of the
	// end-state comparison with the plan's recorded exclude patterns.
	Exclude []string
	// TxMode is the transaction mode the target apply will use.
	TxMode migrator.MigrationTxMode
	// OwnerTransactions contains requirements in statement order. Nil means no
	// owner assessments are available; a non-nil slice must match statements.
	OwnerTransactions [][]plangraph.Transaction
	// DevServerDisposable is the operator's declaration that the server
	// DevURL names is the run's own; see [SimulateOptions.DevServerDisposable].
	DevServerDisposable bool
	// PlacedSchema is the plan's [PlanFile.PlacedSchema]. The comparison of the
	// rehearsed state places the desired schema's unqualified names there. Empty
	// asks the dev database the rehearsal runs on, which is what a plan that
	// records no schema, such as an Atlas .plan.hcl document, leaves to decide.
	PlacedSchema string
}

// RehearsePlanStatements verifies a pre-planned migration semantically, the
// way Atlas verifies a plan file whose hashes it can check and Ptah cannot:
// the dev database is reset, the target's introspected current schema is
// recreated on it, the plan's ordered statements execute there, and the
// reached state must equal the --to desired state under Ptah's own schema
// diff. A replay failure or a non-empty diff refuses the target apply; the
// target database has not been modified.
func RehearsePlanStatements(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statements []string,
	desired *schemamodel.Database,
	opts PlanRehearsalOptions,
) error {
	if err := schemaext.RequireRuntime(ctx, opts.Runtime); err != nil {
		return err
	}
	if opts.OwnerTransactions != nil && len(opts.OwnerTransactions) != len(statements) {
		return errors.New("plan rehearsal transaction requirements do not match the statements")
	}
	if err := checkOwnerTransactions(opts.OwnerTransactions, opts.TxMode, conn); err != nil {
		return err
	}
	if conn == nil {
		return errors.New("plan rehearsal requires database connection")
	}
	if desired == nil {
		return errors.New("plan rehearsal requires the desired schema state")
	}
	if strings.TrimSpace(opts.DevURL) == "" {
		return errors.New("plan rehearsal requires a dev database URL")
	}
	// The escape lint runs again inside rehearseStatementsOnDev, which is the
	// gate every dev-database caller passes through. Running it here too
	// refuses a plan before the target database is even inspected, and lets
	// the refusal name the target dialect rather than the dev one.
	if err := CheckPlanStatementsSandboxable(statements, conn.Info().Dialect); err != nil {
		return err
	}

	// The scope computeApplyPlan read the plan's target at, which on a realm URL
	// takes in the schemas the desired state names. Read at the URL's scope
	// alone, the target's current schema, the baseline leaves out every other
	// schema the plan alters: measured on PostgreSQL 18, a saved plan altering
	// "app"."t" on a target whose current schema is public failed on the dev
	// database with 3F000, schema "app" does not exist.
	read, err := applyCurrentState(
		readscope.Without(ctx, atlasfilter.KindsLeftOut(opts.Runtime.Selections(), opts.Exclude, nil)...), conn, nil, desired,
	)
	if err != nil {
		return err
	}
	current := read.current
	// Same default schema computeApplyPlan uses, so the rehearsal subtracts
	// exactly the objects the plan was computed without.
	current, err = excludeCurrentWithBindings(ctx, current, opts.Exclude, conn.Info(), opts.Runtime)
	if err != nil {
		return fmt.Errorf("apply plan exclude patterns to current schema: %w", err)
	}

	dev, err := connectSimulationDev(ctx, opts.DevURL, opts.DevServerDisposable, conn, opts.TargetURL, opts.DesiredURLs)
	if err != nil {
		return err
	}
	devConn := dev.conn
	// Registered before the close so it runs after it: a provisioned container
	// is removed only once the connection to it is gone.
	defer dev.release()
	defer dbschema.CloseAndWarn(devConn)
	// Registered after the close so it runs before it, and after the
	// end-state comparison below, which reads the rehearsed state it drops.
	defer discardDevRehearsalArtifacts(ctx, devConn, dev.baseline)

	seed := newRehearsalSeed(conn.Info(), opts.Runtime.Selections(), nil, nil, opts.Exclude)
	if err := rehearseStatementsOnDev(
		ctx, conn, devConn, dev.baseline, current, seed.target(ctx, conn), opts.TxMode, statements, opts.Runtime,
	); err != nil {
		return err
	}

	// A plan that records where it placed an unqualified desired name is
	// compared with the names placed there. Otherwise the rehearsal has its dev
	// database, so it asks it rather than asking its URL again.
	devInfo := devConn.Info()
	naming := namingFor(conn.Info(), &devInfo)
	if recorded, ok := recordedNaming(conn.Info(), opts.PlacedSchema); ok {
		naming = recorded
	}
	computation, err := compareRehearsedState(ctx, devConn, conn.Info(), ApplyOptions{
		Runtime:  opts.Runtime,
		Desired:  desired,
		Exclude:  opts.Exclude,
		baseline: dev.baseline,
		naming:   naming,
	})
	if err != nil {
		return fmt.Errorf("compare rehearsed plan state with the desired state: %w", err)
	}
	if err := computation.undecided.Err(); err != nil {
		return err
	}
	// Convergence includes the declared rows. A rehearsal whose schema matches
	// and whose reference table does not is not a rehearsal of the desired
	// state.
	if drift := computation.executionUnits(); len(drift) > 0 {
		return &PlanDesiredStateError{Phase: "rehearsal", Drift: drift}
	}
	return nil
}

// compareRehearsedState computes the plan from the rehearsed dev database to
// the desired state, reading the dev database as the target names its objects.
//
// The rehearsal built the target's state on a dev session that resolves names
// as the target does; see [resolveNamesAsTarget]. The desired schema's own names
// are placed in a schema, so they read the same in any session; see
// [namingFor]. What still depends on the session is SQL text the server prints
// back, such as a view's body, which names a relation without its schema only
// when the session's search path reaches it. Read through a session of the
// dev database's own, a view the rehearsal built over a table of the target's
// current schema reads back naming that schema, where a read of the target
// names the table bare, and the comparison would report a rehearsal that
// reached the desired schema as drift.
func compareRehearsedState(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	target catalog.ServerInfo,
	opts ApplyOptions,
) (applyComputation, error) {
	if !realmPairResolvesApart(devConn.Info(), target) {
		return computeApplyPlan(ctx, devConn, opts)
	}
	var computation applyComputation
	err := devConn.WithSession(ctx, func(session *dbschema.DatabaseConnection) error {
		moved, err := session.ResolveNamesAs(ctx, target)
		if err != nil {
			return err
		}
		computation, err = computeApplyPlan(ctx, moved, opts)
		return err
	})
	return computation, err
}

// VerifyAppliedPlanState performs the post-apply end-state verification:
// after the plan executed on the target, the introspected database schema
// must match the --to desired state under Ptah's own schema diff. Atlas
// performs the equivalent verification on every `schema apply --plan`; the
// check is always on and has no opt-out flag.
//
// The verification compares with the plan's exclude patterns, and places a
// name the desired schema writes without a schema where the plan placed it
// ([PlanFile.PlacedSchema]). A plan that records no schema, such as an Atlas
// .plan.hcl document, places it where devURL, the dev database the apply
// names, resolves it, or where the target does when devURL is empty; see
// [namingFor].
func VerifyAppliedPlanState(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	plan PlanFile,
	devURL string,
	runtime engine.SchemaRuntime,
) error {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return err
	}
	if conn == nil {
		return errors.New("plan end-state verification requires database connection")
	}
	if desired == nil {
		return errors.New("plan end-state verification requires the desired schema state")
	}
	naming, recorded := recordedNaming(conn.Info(), plan.PlacedSchema)
	if !recorded {
		var err error
		if naming, err = resolveDesiredNaming(ctx, conn.Info(), devURL); err != nil {
			return fmt.Errorf(
				"the plan was applied successfully, but the end-state verification could not be completed: %w", err)
		}
	}
	computation, err := computeApplyPlan(ctx, conn, ApplyOptions{
		Runtime: runtime,
		Desired: desired,
		Exclude: plan.Exclude,
		naming:  naming,
	})
	if err != nil {
		// The plan already executed successfully; this is the verification
		// itself failing to run, which must not read as a schema mismatch.
		return fmt.Errorf(
			"the plan was applied successfully, but the end-state verification could not be completed: %w", err)
	}
	if err := computation.undecided.Err(); err != nil {
		return fmt.Errorf("the plan was applied successfully, but the end-state verification has incomplete evidence: %w", err)
	}
	if drift := computation.executionUnits(); len(drift) > 0 {
		return &PlanDesiredStateError{Phase: "post-apply", Drift: drift}
	}
	return nil
}

// NewEphemeralSQLiteDev creates a throwaway SQLite dev database for plan
// rehearsal when no --dev-url was given: a SQLite dev database is just a
// temporary file, so the rehearsal needs no operator-provided server. The
// returned cleanup removes the database; it is safe to call once.
func NewEphemeralSQLiteDev() (devURL string, cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "ptah-plan-dev-*")
	if err != nil {
		return "", nil, fmt.Errorf("create ephemeral dev database directory: %w", err)
	}
	return atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db")), func() { _ = os.RemoveAll(dir) }, nil
}
