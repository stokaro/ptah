package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/atlaschange"
	"ptah.run/internal/mysqldefiner"
	"ptah.run/internal/planeffect"
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
	return captureApplyPlan(info, rendered, renderer.Result{}, nil)
}

// captureApplyPlan is CaptureApplyPlan with text, the plan's nodes rendered in
// [renderer.FormText], or the zero Result when there is none: a native unit
// whose node text renders as SQL keeps that SQL for display (see
// [ApplyPlan.texts]).
//
// effects is what describing each unit reads, or nil to describe none; see
// describeUnits.
func captureApplyPlan(
	info catalog.ServerInfo,
	rendered planner.RenderedPlan,
	text renderer.Result,
	effects *planeffect.Target,
) (ApplyPlan, error) {
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
	names := atlaschange.For(info.Dialect).Forward(rendered.Request.Nodes)
	named := -1
	var indexes []int
	for i, unit := range units {
		positions[i] = len(plan.units)
		if !rendered.Result.ExecutableUnits && strings.TrimSpace(sqlutil.StripCommentsForDialect(unit.Fragment.SQL, rendered.Dialect)) == "" {
			// A comment-only unit is never executed alone. It trails the
			// statements, so it stays readable in the plan by riding on the
			// SQL unit before it, which is where a skipped object's note lands.
			if last := len(plan.units) - 1; last >= 0 && plan.units[last].Native == nil {
				plan.units[last].SQL += "\n" + unit.Fragment.SQL
				continue
			}
			// With no SQL unit before it, the note is all the plan says about
			// a difference it plans no statement for, such as a generated
			// column whose expression the target cannot change. Dropped, the
			// plan reads as empty and apply reports the schema synced. Such a
			// unit is the plan's trailing comment piece, which
			// RenderedPlan.Annotations positions ahead of every rendered
			// annotation, so the rendered ones follow it.
			plan.annotations = append(plan.annotations, renderer.Annotation{
				Before: len(plan.units),
				Text:   commentProse(unit.Fragment.SQL),
			})
			continue
		}
		plan.units = append(plan.units, unit.Fragment)
		plan.texts = append(plan.texts, nodeText(unit, text))
		plan.changes = append(plan.changes, "")
		if unit.Node != named {
			plan.changes[len(plan.changes)-1] = names[unit.Node]
			named = unit.Node
		}
		indexes = append(indexes, unit.Node)
	}
	if !slices.ContainsFunc(plan.texts, func(text string) bool { return text != "" }) {
		plan.texts = nil
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
	statements := make([]string, len(plan.units))
	for i, unit := range plan.units {
		statements[i] = unit.SQL
	}
	plan.columns, err = safety.ColumnVerdicts(rendered.Request.Nodes, indexes, statements)
	if err != nil {
		return ApplyPlan{}, err
	}
	plan.restores, err = safety.OwnerRestores(rendered.Request.Nodes, indexes)
	if err != nil {
		return ApplyPlan{}, err
	}
	plan.effects = describeUnits(effects, plan.provider, rendered.Request.Nodes, indexes)
	return plan, nil
}

// commentProse is the text of a comment-only SQL unit as an annotation holds
// it: each line comment loses its marker and the space after it, so writing the
// annotation back as SQL comments gives the lines the planner wrote. A line
// that is not a line comment, such as a block comment, is kept as it reads.
func commentProse(sql string) string {
	var lines []string
	for line := range strings.SplitSeq(strings.TrimSpace(sql), "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "--"); ok {
			line = strings.TrimPrefix(rest, " ")
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// describeUnits is [planeffect.Describe] of the captured units, where units
// holds the index in nodes of each unit's node. It is nil without a target to
// read, and for a plan an executable provider renders: the description follows
// the statements Ptah's own renderers write for a node.
func describeUnits(target *planeffect.Target, provider *PlanProviderBinding, nodes []ast.Node, units []int) [][]planeffect.Effect {
	if target == nil || provider != nil {
		return nil
	}
	return planeffect.Describe(*target, nodes, units)
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
	if err := checkTransactionAvailable(mode, conn); err != nil {
		return err
	}
	units, statements, err := cloneApplyUnits(p.units)
	if err != nil {
		return err
	}
	if err := preflightUnitTransaction(info.Dialect, info.Capabilities, mode, units); err != nil {
		return err
	}
	// The SQL units of a plan that also holds a provider-native operation are
	// asked too: the native units do not change which definers the SQL ones
	// name, and a refusal there would otherwise come from the server, after
	// the units before it ran.
	if err := mysqldefiner.Check(ctx, conn, "", sqlUnitText(units)); err != nil {
		return err
	}
	return validateApplyUnits(ctx, conn.Writer(), units, statements)
}

// sqlUnitText returns the SQL text of each unit that is not a provider-native
// operation, in plan order.
func sqlUnitText(units []renderer.Fragment) []string {
	var text []string
	for _, unit := range units {
		if unit.Native == nil {
			text = append(text, unit.SQL)
		}
	}
	return text
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

// nativeTextForm renders a plan that holds a native operation again in
// [renderer.FormText], for display, as `schema compare` and a migration file
// write it. A plan without one, and one whose owner has no text form for some
// operation (a provider that did not negotiate text units, or an operation it
// performs only through a native call), gets the zero Result, so its native
// units are shown by kind and subject.
func nativeTextForm(ctx context.Context, service renderer.Service, plan planner.RenderedPlan) (renderer.Result, error) {
	if !slices.ContainsFunc(plan.Result.Fragments, func(fragment renderer.Fragment) bool { return fragment.Native != nil }) {
		return renderer.Result{}, nil
	}
	request := plan.Request
	request.Form = renderer.FormText
	text, err := renderer.Render(ctx, service, request)
	if errors.Is(err, renderer.ErrNativeSQL) {
		return renderer.Result{}, nil
	}
	return text, err
}

// nodeText is the text of a native unit's node in text, or "" for an SQL unit
// and for a node text has no SQL for.
func nodeText(unit planner.PlannedUnit, text renderer.Result) string {
	if unit.Fragment.Native == nil || unit.Node < 0 || unit.Node >= len(text.Fragments) {
		return ""
	}
	return strings.TrimSpace(text.Fragments[unit.Node].SQL)
}
