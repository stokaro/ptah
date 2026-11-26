package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devseed"
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
	if err := checkDevNaming(p.rehearsal.planning.naming, p.conn.Info(), dev.conn.Info()); err != nil {
		return err
	}

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
	outside := rehearsalOutside{target: p.seed.target(ctx, p.conn), executed: p.plan.executedSQL()}
	devConn, added, err := rebuildTargetOnDev(ctx, devConn, baseline, p.current, outside, p.conn.Info(), p.runtime)
	if err != nil {
		return err
	}
	plan, err := p.planOnDev(ctx, devConn, baseline, assessments)
	if err != nil {
		return &SimulationError{Stage: "plan", Err: devseed.WithAdded(err, added)}
	}
	if err := plan.Execute(ctx, devConn, p.txMode); err != nil {
		return &SimulationError{Stage: "plan", Err: devseed.WithAdded(err, added)}
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
	target := plannedOn{plan: p.rehearsal.schema, info: p.conn.Info()}
	if err := sameOperations(ctx, assessments, target, plannedOn{plan: computation.ApplyPlan, info: devConn.Info()}); err != nil {
		return ApplyPlan{}, err
	}
	computation.dataStatements = slices.Clone(p.rehearsal.data)
	plan := computation.executionPlan()
	guard, err := devclean.NewDevReplayGuardFor(ctx, devConn)
	if err != nil {
		return ApplyPlan{}, err
	}
	if err := guardRehearsedUnits(plan.units, guard.WithPublications(baseline.Publications).WithSurvivors(baseline.Survivors), devConn); err != nil {
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
// what an SQL unit's statements do, the object it changes, how severe the
// change is, and the inputs it reads. It leaves the bytes out, because a plan
// for another database may spell an address in them differently.
//
// The assessment names an SQL unit by NodeType "sql" alone, so action, the
// leading words of its statements, says whether it creates, alters or drops
// ([safety.StatementTargets]), with the head of an ALTER's clause, and subject
// the objects they act on. Without them two plans that do different things
// with the same number of units, such as an ALTER TABLE on the target where
// the dev database creates the table, or an ADD COLUMN of another column,
// compare equal.
type operationSignature struct {
	native   schemaext.Kind
	node     string
	action   string
	subject  string
	severity safety.Severity
	inputs   string
}

func (s operationSignature) String() string {
	kind := string(s.native)
	if kind == "" {
		kind = s.node
	}
	if s.action != "" {
		kind = s.action
	}
	if s.subject == "" {
		return kind
	}
	return kind + " of " + s.subject
}

// plannedOn is a plan and the connection it was made on.
type plannedOn struct {
	plan ApplyPlan
	info catalog.ServerInfo
}

// sameOperations refuses a dev plan whose units are not the target's.
func sameOperations(ctx context.Context, runtime safety.Runtime, target, dev plannedOn) error {
	want, err := operationSignatures(ctx, runtime, target)
	if err != nil {
		return err
	}
	got, err := operationSignatures(ctx, runtime, dev)
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

func operationSignatures(ctx context.Context, runtime safety.Runtime, planned plannedOn) ([]operationSignature, error) {
	dialect := planned.info.Dialect
	assessed, err := safety.AssessUnits(ctx, runtime, dialect, planned.plan.units, planned.plan.owned)
	if err != nil {
		return nil, err
	}
	signatures := make([]operationSignature, len(planned.plan.units))
	for i, unit := range planned.plan.units {
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
		if unit.Native == nil {
			signatures[i].action, signatures[i].subject = sqlUnitSignature(unit.SQL, planned.info)
		}
	}
	return signatures, nil
}

// sqlUnitSignature is what the statements of an SQL unit do and the objects
// they act on, as [safety.StatementTargets] reads them: each statement's
// action followed by the head of an ALTER's clause, and its objects, each
// statement's joined with "; ".
//
// Where a schema is a database, a name the plan's own database qualifies is
// read without that part: a MySQL-family plan writes `app`.`notes` on the
// target where the dev database's plan writes `app_dev`.`notes`, and both act
// on the table notes of the database they run in. A name of another database
// is read as written, and so is every name where info names no database, as on
// a URL for a whole server, whose plan names each table by its database.
func sqlUnitSignature(sql string, info catalog.ServerInfo) (action, subject string) {
	own := ""
	if schemaScopeNamesDatabase(info.Dialect) {
		own = info.Schema
	}
	var actions, subjects []string
	for _, statement := range safety.StatementTargets(sql, info.Dialect) {
		actions = append(actions, strings.TrimSpace(statement.Action+" "+statement.Clause))
		names := make([]string, 0, len(statement.Objects))
		for _, parts := range statement.Objects {
			if own != "" && len(parts) > 1 && parts[0] == own {
				parts = parts[1:]
			}
			names = append(names, qualifiedName(parts))
		}
		subjects = append(subjects, strings.Join(names, ", "))
	}
	return strings.Join(actions, "; "), strings.Join(subjects, "; ")
}

// qualifiedName joins the parts of a name with dots, quoting a part that holds
// a dot, so the table "app.notes" and the table notes of app read apart.
func qualifiedName(parts []string) string {
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = part
		if strings.Contains(part, ".") {
			quoted[i] = strconv.Quote(part)
		}
	}
	return strings.Join(quoted, ".")
}
