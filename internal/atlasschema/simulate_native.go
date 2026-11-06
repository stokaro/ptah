package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/devclean"
	"ptah.run/migration/safety"
)

// nativeRehearsal is what a rehearsal of a plan that holds a native operation
// needs to plan the change again on its dev database.
//
// A native operation is bound to the database it was planned for: the session
// that executes it refuses one planned for another database or realm, and the
// owner reads the subjects it names in that database. So the rehearsal does
// not send the target's operations to the dev database. It recreates the
// target's current schema there, plans the same desired state against it,
// holds that plan to the target's operation by operation, and executes it the
// way the apply executes the target's.
type nativeRehearsal struct {
	// planning is what the target's plan was computed with.
	planning ApplyOptions
	// desiredFor is [ApplyRuntimeOptions.DesiredFor].
	desiredFor func(context.Context, string) (*schemamodel.Database, error)
	// schema is the target's plan without its declared rows.
	schema ApplyPlan
	// data are the target plan's data statements, which the rehearsal runs
	// after the schema: the dev database holds none of the target's rows, so
	// a data stage planned there would insert every declared row.
	data []dataStatement
}

// hasNativeExecution reports whether the plan must run through its owner's
// executor: a native operation cannot be written as SQL, and an approved input
// is bound to the unit it was approved with.
func (p ApplyRuntimePlan) hasNativeExecution() bool {
	return p.HasNativeUnits() || p.HasInputReferences()
}

// simulateNativeOnDev is [ApplyRuntimePlan.SimulateOnDev] for a plan that
// holds a native operation or an approved input. The dev database is claimed,
// reset and given the target's current schema as for a plan of SQL text; the
// plan is then made again for the dev database and executed there through
// the same executor the apply uses.
func (p ApplyRuntimePlan) simulateNativeOnDev(ctx context.Context, opts SimulateOptions) error {
	if len(opts.Statements) != 0 {
		return fmt.Errorf("%w: edited SQL cannot stand in for a plan that holds native operations or approved inputs", ptaherr.ErrUnsupportedFeature)
	}
	if err := checkOwnerTransactions(p.plan.transactions, p.txMode, p.conn); err != nil {
		return err
	}
	if p.conn == nil {
		return errors.New("schema apply simulation requires database connection")
	}
	assessments, ok := p.runtime.(safety.Runtime)
	if !ok {
		return errors.New("schema apply simulation of native operations requires their assessment")
	}
	dev, err := connectSimulationDev(ctx, opts.DevURL, opts.DevServerDisposable, p.conn, opts.TargetURL, opts.DesiredURLs)
	if err != nil {
		return err
	}
	// The cleanups run in the order a rehearsal of SQL text runs them: the dev
	// database is emptied, its connection closed, and its server released.
	defer dev.release()
	defer dbschema.CloseAndWarn(dev.conn)
	defer discardDevRehearsalArtifacts(ctx, dev.conn, dev.baseline)

	return dev.conn.WithUntrustedSQLSession(ctx, func(session *dbschema.DatabaseConnection) error {
		return p.rehearseNativeOnPreparedDev(ctx, session, dev.baseline, assessments)
	})
}

func (p ApplyRuntimePlan) rehearseNativeOnPreparedDev(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	baseline devclean.Baseline,
	assessments safety.Runtime,
) error {
	devConn.SchemaWriter().SetDryRun(false)
	if err := devclean.Reset(ctx, devConn, baseline); err != nil {
		return &SimulationError{Stage: "reset", Err: err}
	}
	if err := recreateCurrentSchema(ctx, devConn, p.current, p.conn.Info().URL, baseline, p.runtime); err != nil {
		return &SimulationError{Stage: "baseline", Err: err}
	}
	plan, err := p.planOnDev(ctx, devConn, baseline, assessments)
	if err != nil {
		return &SimulationError{Stage: "plan", Err: err}
	}
	if err := plan.Execute(ctx, devConn, p.txMode); err != nil {
		return &SimulationError{Stage: "plan", Err: err}
	}
	return nil
}

// planOnDev makes the target's plan again for the dev database, from the same
// desired state bound to the dev database, and returns it with the target
// plan's data statements. A plan that is not the target's, operation by
// operation, is refused: the rehearsal would pass on changes the apply does
// not make.
func (p ApplyRuntimePlan) planOnDev(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	baseline devclean.Baseline,
	assessments safety.Runtime,
) (ApplyPlan, error) {
	options := p.rehearsal.planning
	// The target's planning already reported what these write, and the
	// operator reads it once.
	options.Diagnostics = io.Discard
	options.WarnUndescribedSchemas = false
	options.baseline = baseline
	options.withoutData = true
	if p.rehearsal.desiredFor != nil {
		desired, err := p.rehearsal.desiredFor(ctx, devConn.Info().URL)
		if err != nil {
			return ApplyPlan{}, fmt.Errorf("load the desired schema for the dev database: %w", err)
		}
		options.Desired = desired
	}
	computation, err := computeApplyPlan(ctx, devConn, options)
	if err != nil {
		return ApplyPlan{}, err
	}
	if err := sameOperations(ctx, assessments, devConn.Info().Dialect, p.rehearsal.schema, computation.ApplyPlan); err != nil {
		return ApplyPlan{}, err
	}
	computation.dataStatements = slices.Clone(p.rehearsal.data)
	plan := computation.executionPlan()
	if err := guardRehearsedUnits(plan.units, devclean.NewDevReplayGuardFor(devConn), devConn); err != nil {
		return ApplyPlan{}, err
	}
	return plan, nil
}

// guardRehearsedUnits holds the SQL units of a rehearsed plan to the escape
// lint and the replay guard a plan of SQL text is held to. The owner confines
// its native operations to the dev database itself.
func guardRehearsedUnits(units []renderer.Fragment, guard *devclean.ReplayGuard, devConn *dbschema.DatabaseConnection) error {
	var statements []string
	for _, unit := range units {
		if unit.Native == nil && strings.TrimSpace(unit.SQL) != "" {
			statements = append(statements, unit.SQL)
		}
	}
	if err := checkPlanStatements(statements, devConn.Info().Dialect); err != nil {
		return err
	}
	return guardPlanRehearsal(statements, guard, devConn.Info())
}

// operationSignature is what the operator is shown of one planned unit, and
// what a rehearsal holds the dev database's plan to: the kind of operation,
// the object it changes, how severe the change is, and the inputs it reads.
// It leaves the bytes out, because a plan for another database may spell an
// address in them differently.
type operationSignature struct {
	native   schemaext.Kind
	node     string
	subject  string
	severity safety.Severity
	inputs   string
}

func (s operationSignature) String() string {
	kind := string(s.native)
	if kind == "" {
		kind = s.node
	}
	if s.subject == "" {
		return kind
	}
	return kind + " of " + s.subject
}

// sameOperations refuses a dev plan whose units are not the target's.
func sameOperations(ctx context.Context, runtime safety.Runtime, dialect string, target, dev ApplyPlan) error {
	want, err := operationSignatures(ctx, runtime, dialect, target)
	if err != nil {
		return err
	}
	got, err := operationSignatures(ctx, runtime, dialect, dev)
	if err != nil {
		return err
	}
	for i := range max(len(want), len(got)) {
		if i < len(want) && i < len(got) && want[i] == got[i] {
			continue
		}
		return fmt.Errorf("the plan made for the dev database is not the plan for the target: %s", divergence(want, got, i))
	}
	return nil
}

// divergence names the first unit at which two plans part.
func divergence(want, got []operationSignature, at int) string {
	switch {
	case at >= len(got):
		return fmt.Sprintf("the target's plan has %d units and the dev database's %d; unit %d is %s on the target", len(want), len(got), at+1, want[at])
	case at >= len(want):
		return fmt.Sprintf("the target's plan has %d units and the dev database's %d; unit %d is %s on the dev database", len(want), len(got), at+1, got[at])
	default:
		return fmt.Sprintf("unit %d is %s on the target and %s on the dev database", at+1, want[at], got[at])
	}
}

func operationSignatures(ctx context.Context, runtime safety.Runtime, dialect string, plan ApplyPlan) ([]operationSignature, error) {
	assessed, err := safety.AssessUnits(ctx, runtime, dialect, plan.units, plan.owned)
	if err != nil {
		return nil, err
	}
	signatures := make([]operationSignature, len(plan.units))
	for i, unit := range plan.units {
		names := make([]string, 0, len(unit.Inputs))
		for _, input := range unit.Inputs {
			names = append(names, input.Name)
		}
		signatures[i] = operationSignature{
			native:   assessed[i].NativeKind,
			node:     assessed[i].NodeType,
			subject:  assessed[i].Subject,
			severity: assessed[i].Severity,
			inputs:   strings.Join(names, ","),
		}
	}
	return signatures, nil
}
