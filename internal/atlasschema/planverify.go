package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasfilter"
	"github.com/stokaro/ptah/migration/migrator"
)

// PlanDesiredStateError reports that the plan's SQL does not converge the
// database to the --to desired state. Phase "rehearsal" means the mismatch
// was caught on the dev database before the target was touched; phase
// "post-apply" means the plan executed on the target but the resulting schema
// does not match the desired state.
type PlanDesiredStateError struct {
	// Phase is "rehearsal" or "post-apply".
	Phase string
	// Drift is Ptah's own DDL describing the remaining differences between
	// the reached state and the desired state.
	Drift []string
}

func (e *PlanDesiredStateError) Error() string {
	drift := strings.TrimSpace(FormatMigrationSQL(e.Drift))
	if e.Phase == "rehearsal" {
		return fmt.Sprintf(
			"pre-planned migration does not converge to the desired state: replaying the plan on the dev database, "+
				"starting from the target's current schema, left the following schema drift against --to "+
				"(the target database was left unchanged):\n%s\n"+
				"either the target database changed since the plan was computed or the plan was computed for a "+
				"different desired state; re-run `schema plan` against the current database and review the fresh plan",
			drift)
	}
	return fmt.Sprintf(
		"schema apply --plan end-state verification failed: after applying the plan, the database does not match "+
			"the --to desired state; the remaining schema drift is:\n%s",
		drift)
}

// IsPlanDesiredStateFailure reports whether err wraps a desired-state
// verification failure.
func IsPlanDesiredStateFailure(err error) bool {
	var target *PlanDesiredStateError
	return errors.As(err, &target)
}

// PlanRehearsalOptions configures RehearsePlanStatements.
type PlanRehearsalOptions struct {
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
	desired *goschema.Database,
	opts PlanRehearsalOptions,
) error {
	if conn == nil {
		return errors.New("plan rehearsal requires database connection")
	}
	if desired == nil {
		return errors.New("plan rehearsal requires the desired schema state")
	}
	devURL := strings.TrimSpace(opts.DevURL)
	if devURL == "" {
		return errors.New("plan rehearsal requires a dev database URL")
	}

	current, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	if err != nil {
		return fmt.Errorf("read database schema: %w", err)
	}
	current, err = atlasfilter.ExcludeDatabase(current, opts.Exclude)
	if err != nil {
		return fmt.Errorf("apply plan exclude patterns to current schema: %w", err)
	}

	devConn, err := connectSimulationDev(ctx, devURL, conn.Info(), opts.TargetURL, opts.DesiredURLs)
	if err != nil {
		return err
	}
	defer dbschema.CloseAndWarn(devConn)

	if err := rehearseStatementsOnDev(ctx, devConn, current, opts.TxMode, statements); err != nil {
		return err
	}

	computation, err := computeApplyPlan(ctx, devConn, ApplyOptions{
		Desired: desired,
		Exclude: opts.Exclude,
	})
	if err != nil {
		return fmt.Errorf("compare rehearsed plan state with the desired state: %w", err)
	}
	if len(computation.statements) > 0 {
		return &PlanDesiredStateError{Phase: "rehearsal", Drift: computation.statements}
	}
	return nil
}

// VerifyAppliedPlanState performs the post-apply end-state verification:
// after the plan executed on the target, the introspected database schema
// must match the --to desired state under Ptah's own schema diff. Atlas
// performs the equivalent verification on every `schema apply --plan`; the
// check is always on and has no opt-out flag.
func VerifyAppliedPlanState(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *goschema.Database,
	exclude []string,
) error {
	if conn == nil {
		return errors.New("plan end-state verification requires database connection")
	}
	if desired == nil {
		return errors.New("plan end-state verification requires the desired schema state")
	}
	computation, err := computeApplyPlan(ctx, conn, ApplyOptions{
		Desired: desired,
		Exclude: exclude,
	})
	if err != nil {
		return fmt.Errorf("verify applied plan end state: %w", err)
	}
	if len(computation.statements) > 0 {
		return &PlanDesiredStateError{Phase: "post-apply", Drift: computation.statements}
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
	return "sqlite://" + filepath.Join(dir, "dev.db"), func() { _ = os.RemoveAll(dir) }, nil
}
