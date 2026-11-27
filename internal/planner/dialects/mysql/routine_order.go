package mysql

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/deporder"
	"ptah.run/internal/planner/featurehost"
)

// routineCreationGroup keeps a replacement's drop and create indivisible.
// Other statements keep their order relative to one another, while created
// relations and routines may move around them to satisfy their dependencies.
type routineCreationGroup struct {
	nodes  []ast.Node
	object deporder.ViewLike
	move   bool
}

func (p *Planner) createViewsAndOrderRoutines(ctx context.Context, nodes []ast.Node, start int, diff *difftypes.SchemaDiff, windows *featurehost.Windows) ([]ast.Node, error) {
	nodes, err := p.createViews(nodes, diff)
	if err != nil {
		return nil, err
	}
	if err := p.rejectMaterializedViews(diff); err != nil {
		return nil, err
	}
	nodes = p.addNewMaterializedViews(nodes, diff)
	nodes = p.modifyExistingMaterializedViews(nodes, diff)
	creations, err := p.orderRoutineCreations(ctx, nodes[start:], diff)
	if err != nil {
		return nil, err
	}
	nodes = append(nodes[:start], creations...)
	if len(diff.FunctionsAdded) > 0 || len(diff.FunctionsModified) > 0 {
		windows.Creation = len(nodes)
	}
	return nodes, nil
}

// orderRoutineCreations orders the common creation span, after the type
// window and before triggers, grants and removals. Routines otherwise follow
// all tables and views, including when a dynamic body hides its reads. A table
// default, CHECK or generated column can instead require a routine first.
// The graph carries both directions, so source table -> view -> routine ->
// dependent table is one ordering rather than competing phase choices.
func (p *Planner) orderRoutineCreations(ctx context.Context, nodes []ast.Node, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	groups := routineCreationGroups(nodes, diff)
	if !slices.ContainsFunc(groups, func(group routineCreationGroup) bool { return group.object.Routine }) {
		return nodes, nil
	}
	// A routine with no recognized reader waits for every relation. Stable
	// scheduling moves it earlier only where another statement needs it.
	slices.SortStableFunc(groups, func(a, b routineCreationGroup) int {
		return a.rank() - b.rank()
	})
	objects := make([]deporder.ViewLike, len(groups))
	contribution := plangraph.Contribution[[]ast.Node]{Owner: "ptah.run/routine-order"}
	var previousFixed *plangraph.StepID
	for i, group := range groups {
		objects[i] = group.object
		id := routineCreationID(i, group.object.Name)
		contribution.Steps = append(contribution.Steps, plangraph.Step[[]ast.Node]{ID: id, Payload: group.nodes})
		if !group.move {
			previousFixed = preserveCreationOrder(&contribution, previousFixed, id)
		}
	}
	cyclicViews := p.hasReplacedViewCycle(groups, diff)
	for after, before := range deporder.CreationDependenciesIn(objects, p.targetDialect(), diff.EffectiveIdentifierSemantics(p.targetDialect())) {
		for _, dependency := range before {
			// A view replacement can close a ring through a view already
			// present. createViews chooses its executable order; retain those
			// edges while still allowing routines between independent views.
			if cyclicViews && dependency > after && groups[after].isView() && groups[dependency].isView() {
				continue
			}
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{
				Before: contribution.Steps[dependency].ID, After: contribution.Steps[after].ID,
			})
		}
	}
	preferNeededCreations(&contribution, groups)
	ordered, err := plangraph.Schedule(ctx, contribution)
	if err != nil {
		return nil, fmt.Errorf("%w: routine creation dependency order: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	result := make([]ast.Node, 0, len(nodes))
	for _, step := range ordered.Steps {
		result = append(result, step.Payload...)
	}
	return result, nil
}

// Ready relation prerequisites take priority over unrelated routines. Carry
// that priority through every prerequisite, so a helper routine can unlock a
// view before a dynamic routine, whose body hides a read of that view, runs.
func preferNeededCreations(contribution *plangraph.Contribution[[]ast.Node], groups []routineCreationGroup) {
	before := make(map[plangraph.StepID][]plangraph.StepID)
	for _, edge := range contribution.Dependencies {
		before[edge.After] = append(before[edge.After], edge.Before)
	}
	var pending []plangraph.StepID
	for i, group := range groups {
		if !group.object.Routine {
			pending = append(pending, contribution.Steps[i].ID)
		}
	}
	needed := make(map[plangraph.StepID]bool)
	for len(pending) > 0 {
		id := pending[0]
		pending = pending[1:]
		if needed[id] {
			continue
		}
		needed[id] = true
		pending = append(pending, before[id]...)
	}
	for i, step := range contribution.Steps {
		if needed[step.ID] {
			contribution.Steps[i].Placement = plangraph.PlacementEarly
		}
	}
}

func (p *Planner) hasReplacedViewCycle(groups []routineCreationGroup, diff *difftypes.SchemaDiff) bool {
	if len(diff.ViewsModified) == 0 {
		return false
	}
	var views []deporder.ViewLike
	for _, group := range groups {
		if group.isView() {
			views = append(views, group.object)
		}
	}
	return len(deporder.ViewLikeCycleIn(views, p.targetDialect(), diff.EffectiveIdentifierSemantics(p.targetDialect()))) > 0
}

func (g routineCreationGroup) rank() int {
	if g.object.Routine {
		return 1
	}
	return 0
}

func routineCreationID(index int, name string) plangraph.StepID {
	return plangraph.StepID{Owner: "ptah.run/routine-order", Name: fmt.Sprintf("%06d %s", index, name)}
}

func preserveCreationOrder(contribution *plangraph.Contribution[[]ast.Node], previous *plangraph.StepID, next plangraph.StepID) *plangraph.StepID {
	if previous != nil {
		contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: *previous, After: next})
	}
	return &next
}

func (g routineCreationGroup) isView() bool {
	switch g.nodes[len(g.nodes)-1].(type) {
	case *ast.CreateViewNode, *ast.CreateMaterializedViewNode:
		return true
	default:
		return false
	}
}

func routineCreationGroups(nodes []ast.Node, diff *difftypes.SchemaDiff) []routineCreationGroup {
	var groups []routineCreationGroup
	for i := 0; i < len(nodes); i++ {
		group := routineCreationGroup{nodes: []ast.Node{nodes[i]}}
		if i+1 < len(nodes) && creationPair(nodes[i], nodes[i+1]) {
			i++
			group.nodes = append(group.nodes, nodes[i])
		}
		group.object, group.move = routineCreationObject(nodes[i], diff)
		groups = append(groups, group)
	}
	return groups
}

func creationPair(first, second ast.Node) bool {
	switch drop := first.(type) {
	case *ast.DropFunctionNode:
		create, ok := second.(*ast.CreateFunctionNode)
		return ok && drop.Name == create.Name && drop.IsProcedure() == create.IsProcedure()
	case *ast.DropMaterializedViewNode:
		create, ok := second.(*ast.CreateMaterializedViewNode)
		return ok && drop.Name == create.Name
	default:
		return false
	}
}

func routineCreationObject(node ast.Node, diff *difftypes.SchemaDiff) (deporder.ViewLike, bool) {
	switch typed := node.(type) {
	case *ast.CreateFunctionNode:
		return deporder.ViewLike{Name: typed.Name, Routine: true,
			Body:      typed.Parameters + "\n" + typed.Returns + "\n" + typed.Body,
			DependsOn: routineOrderFunctionDependencies(typed.Name, diff)}, true
	case *ast.CreateTableNode:
		var expressions []string
		for _, column := range typed.Columns {
			expressions = append(expressions, creationColumnExpressions(column))
		}
		for _, constraint := range typed.Constraints {
			expressions = append(expressions, constraint.Expression)
		}
		return deporder.ViewLike{Name: typed.Name, Body: strings.Join(expressions, "\n"), DependsOn: diff.DeclaredTableDependencies[typed.Name]}, true
	case *ast.CreateViewNode:
		return deporder.ViewLike{Name: typed.Name, Body: typed.Body, DependsOn: routineOrderViewDependencies(typed.Name, diff)}, true
	case *ast.CreateMaterializedViewNode:
		return deporder.ViewLike{Name: typed.Name, Body: typed.Body, Materialized: true}, true
	case *ast.AlterTableNode:
		return deporder.ViewLike{Body: creationAlterExpressions(typed)}, false
	default:
		return deporder.ViewLike{}, false
	}
}

// A replacement's AST uses the schema the read found, while its dependency
// map remains keyed by the declaration's spelling.
func routineOrderFunctionDependencies(name string, diff *difftypes.SchemaDiff) []string {
	for _, change := range diff.FunctionsModified {
		replacement, _ := inHeldSchema(change.Desired, change)
		if replacement.Name != name {
			continue
		}
		if dependencies, ok := diff.DeclaredFunctions.Dependencies[change.Desired.Name]; ok {
			return dependencies
		}
		break
	}
	return diff.DeclaredFunctions.Dependencies[name]
}

func routineOrderViewDependencies(name string, diff *difftypes.SchemaDiff) []string {
	for _, view := range diff.ViewsAdded {
		if view.Name == name {
			return view.DependsOn
		}
	}
	for _, change := range diff.ViewsModified {
		if change.Desired.Name == name {
			return change.Desired.DependsOn
		}
	}
	return nil
}

func creationColumnExpressions(column *ast.ColumnNode) string {
	if column == nil {
		return ""
	}
	expressions := column.Check + "\n" + column.GeneratedExpression
	if column.Default != nil {
		expressions += "\n" + column.Default.Expression
	}
	return expressions
}

func creationAlterExpressions(table *ast.AlterTableNode) string {
	var expressions []string
	for _, operation := range table.Operations {
		switch typed := operation.(type) {
		case *ast.AddColumnOperation:
			expressions = append(expressions, creationColumnExpressions(typed.Column))
		case *ast.ModifyColumnOperation:
			expressions = append(expressions, creationColumnExpressions(typed.Column))
		case *ast.AlterColumnOperation:
			if typed.Default != nil {
				expressions = append(expressions, typed.Default.Expression)
			}
		case *ast.AddConstraintOperation:
			if typed.Constraint != nil {
				expressions = append(expressions, typed.Constraint.Expression)
			}
		}
	}
	return strings.Join(expressions, "\n")
}
