package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/renderer"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
)

func captureApplyTarget(info catalog.ServerInfo) (ApplyPlan, error) {
	binding, err := planProviderBinding(info)
	if err != nil {
		return ApplyPlan{}, err
	}
	return ApplyPlan{target: info.Dialect, capabilities: info.Capabilities, provider: binding}, nil
}

// CaptureApplyPlan snapshots rendered operations, provenance, and owner facts.
// info is the target observed before planning. Rendering may additionally use
// capabilities provided by changes in the plan, so those effective capabilities
// do not replace the captured connection facts. Callers approve omissions and
// verify target state before execution.
func CaptureApplyPlan(info catalog.ServerInfo, rendered planner.RenderedPlan) (ApplyPlan, error) {
	if rendered.Request.Target != info.Dialect {
		return ApplyPlan{}, fmt.Errorf("%w: rendered plan target differs from the connection", renderer.ErrInvalidResult)
	}
	for key := range info.Capabilities.All() {
		if !rendered.Request.Capabilities.Established(key) {
			return ApplyPlan{}, fmt.Errorf("%w: rendered plan lost established target capability %q", renderer.ErrInvalidResult, key)
		}
	}
	plan, err := captureApplyTarget(info)
	if err != nil {
		return ApplyPlan{}, err
	}
	units, err := rendered.Units()
	if err != nil {
		return ApplyPlan{}, err
	}
	annotations, err := rendered.Annotations()
	if err != nil {
		return ApplyPlan{}, err
	}
	positions := make([]int, len(units)+1)
	var indexes []int
	for i, unit := range units {
		positions[i] = len(plan.units)
		if !rendered.Result.ExecutableUnits && strings.TrimSpace(sqlutil.StripCommentsForDialect(unit.Fragment.SQL, rendered.Dialect)) == "" {
			// A comment-only unit is never executed alone. It trails the
			// statements, so it stays readable in the plan by riding on the
			// SQL unit before it, which is where a skipped object's note lands.
			if last := len(plan.units) - 1; last >= 0 && plan.units[last].Native == nil {
				plan.units[last].SQL += "\n" + unit.Fragment.SQL
			}
			continue
		}
		plan.units = append(plan.units, unit.Fragment)
		indexes = append(indexes, unit.Node)
	}
	positions[len(units)] = len(plan.units)
	for _, annotation := range annotations {
		annotation.Before = positions[annotation.Before]
		plan.annotations = append(plan.annotations, annotation)
	}
	plan.transactions, err = txrequire.OwnerRequirements(rendered.Request.Nodes, indexes)
	if err != nil {
		return ApplyPlan{}, err
	}
	plan.owned, err = safety.OwnerVerdicts(rendered.Request.Nodes, indexes)
	if err != nil {
		return ApplyPlan{}, err
	}
	return plan, nil
}

// Preflight verifies the selected provider, captured base capabilities, complete
// batch, and transaction requirements without writing or opening a transaction.
func (p ApplyPlan) Preflight(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrator.MigrationTxMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if conn == nil {
		return errors.New("schema apply execution requires database connection")
	}
	if err := renderer.ValidateAnnotations(p.annotations, len(p.units)); err != nil {
		return err
	}
	info := conn.Info()
	if p.target != info.Dialect || !maps.Equal(p.capabilities.Map(), info.Capabilities.Map()) {
		return fmt.Errorf("%w: captured apply target capabilities changed", renderer.ErrInvalidResult)
	}
	if err := verifyPlanProvider(p.provider, info); err != nil {
		return err
	}
	if !p.HasChanges() {
		return nil
	}
	if err := checkOwnerTransactions(p.transactions, mode, conn); err != nil {
		return err
	}
	units, statements, err := cloneApplyUnits(p.units)
	if err != nil {
		return err
	}
	if err := preflightUnitTransaction(info.Dialect, info.Capabilities, mode, units); err != nil {
		return err
	}
	return validateApplyUnits(ctx, conn.Writer(), units, statements)
}

// Execute applies the captured units after repeating preflight. Callers own
// approval, target-state verification, and workflow lock acquisition.
func (p ApplyPlan) Execute(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrator.MigrationTxMode) error {
	if err := p.Preflight(ctx, conn, mode); err != nil {
		return err
	}
	if !p.HasChanges() {
		return nil
	}
	return applyUnits(ctx, conn, mode, p.units)
}
