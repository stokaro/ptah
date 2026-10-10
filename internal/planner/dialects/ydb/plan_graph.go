package ydb

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) scheduleFeatureChanges(
	ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff,
	rebuilds map[string]*tableRebuild, semantics identifier.Semantics, before, after []ast.Node,
) ([]ast.Node, error) {
	graph, err := commonGraph(semantics, diff.CurrentDatabasePath, before, after)
	if err != nil {
		return nil, err
	}
	features, err := p.planFeatureChanges(ctx, runtime, diff, rebuilds, semantics, graph.steps)
	if err != nil {
		return nil, err
	}
	return scheduleFeatures(ctx, graph.contribution, graph.beforeFeatures, graph.afterFeatures, features)
}

type commonPlan struct {
	contribution   plangraph.Contribution[[]ast.Node]
	steps          []featureplan.CommonStep
	beforeFeatures plangraph.StepID
	afterFeatures  plangraph.StepID
	root           string
}

// commonGraph gives each accepted statement an identity before owner planning.
// The existing common phases retain their order. Only recognized scheme
// operations supply footprints; other operations retain unknown metadata.
// root is the database the plan runs in, which an absolute secret path is read
// against; see [ydbscheme.CommonEffects].
func commonGraph(semantics identifier.Semantics, root string, before, after []ast.Node) (commonPlan, error) {
	graph := commonPlan{contribution: plangraph.Contribution[[]ast.Node]{Owner: "ptah.run/ydb"}, root: root}
	builder := objectidentity.NewBuilder(semantics)
	if err := graph.appendNodes(builder, "before", before); err != nil {
		return commonPlan{}, err
	}
	graph.beforeFeatures = plangraph.StepID{Owner: graph.contribution.Owner, Name: "common/before-table-features"}
	graph.afterFeatures = plangraph.StepID{Owner: graph.contribution.Owner, Name: "common/after-table-features"}
	graph.appendStep(plangraph.Step[[]ast.Node]{ID: graph.beforeFeatures})
	graph.appendStep(plangraph.Step[[]ast.Node]{ID: graph.afterFeatures})
	if err := graph.appendNodes(builder, "after", after); err != nil {
		return commonPlan{}, err
	}
	return graph, nil
}

func (g *commonPlan) appendNodes(builder objectidentity.Builder, phase string, nodes []ast.Node) error {
	for i, node := range nodes {
		effects, err := ydbscheme.CommonEffects(builder, g.root, node)
		if err != nil {
			return err
		}
		step := plangraph.Step[[]ast.Node]{ID: plangraph.StepID{Owner: g.contribution.Owner, Name: fmt.Sprintf("common/%s/%06d", phase, i)}, Payload: []ast.Node{node}, Effects: effects}
		if len(effects) > 0 {
			step.Transaction = plangraph.TransactionForbidden
		}
		g.appendStep(step)
	}
	return nil
}

func (g *commonPlan) appendStep(step plangraph.Step[[]ast.Node]) {
	if count := len(g.contribution.Steps); count > 0 {
		g.contribution.Dependencies = append(g.contribution.Dependencies, plangraph.Dependency{Before: g.contribution.Steps[count-1].ID, After: step.ID})
	}
	g.contribution.Steps = append(g.contribution.Steps, step)
	g.steps = append(g.steps, featureplan.CommonStep{ID: step.ID, Effects: slices.Clone(step.Effects), Transaction: step.Transaction})
}

// Table-bound features remain between column changes and table drops.
// Standalone owners choose their placement from common resource footprints.
// Scheduling validates both sets together before any SQL nodes are returned.
func scheduleFeatures(ctx context.Context, common plangraph.Contribution[[]ast.Node], before, after plangraph.StepID, features featurehost.Result) ([]ast.Node, error) {
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			if slices.ContainsFunc(step.Payload, func(node ast.Node) bool { _, table := node.(*ast.AlterTableNode); return table }) {
				common.Dependencies = append(common.Dependencies,
					plangraph.Dependency{Before: before, After: step.ID},
					plangraph.Dependency{Before: step.ID, After: after},
				)
			}
		}
	}
	common.Dependencies = append(common.Dependencies, ancestorEdges(common, features.Contributions)...)
	lifecycle, err := plangraph.LifecycleDependencies(ctx, append([]plangraph.Contribution[[]ast.Node]{common}, features.Contributions...)...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	common.Dependencies = append(common.Dependencies, lifecycle...)
	plan, err := plangraph.ScheduleRewritten(ctx, common, features.Rewrites, features.Contributions...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	var nodes []ast.Node
	for _, step := range plan.Steps {
		nodes = append(nodes, step.Payload...)
	}
	return nodes, nil
}

// ancestorEdges orders statements of different owners along one chain of
// scheme paths: an object created below a path follows every statement that
// drops what is at the path, and an object dropped below a path precedes every
// statement that creates an object there, since YDB needs each directory above
// an object to be a directory. Each standalone owner orders its own statements
// against the common ones; these edges order owners against each other, which
// neither can see. A handoff of one path between two owners is ordered by
// [plangraph.LifecycleDependencies], as every host's is; this rule is YDB's,
// since only a scheme path has the directories above it.
func ancestorEdges(common plangraph.Contribution[[]ast.Node], contributions []plangraph.Contribution[[]ast.Node]) []plangraph.Dependency {
	uses, changed := schemePathUses(common, contributions)
	var edges []plangraph.Dependency
	for _, child := range changed {
		for _, directory := range ydbscheme.DirectoriesAbove(child.path) {
			for _, use := range uses[directory.Key()] {
				switch {
				case use.contribution == child.contribution:
				case child.action == plangraph.Create && use.action == plangraph.Drop:
					edges = append(edges, plangraph.Dependency{Before: use.step, After: child.step})
				case child.action == plangraph.Drop && use.action == plangraph.Create:
					edges = append(edges, plangraph.Dependency{Before: child.step, After: use.step})
				}
			}
		}
	}
	return edges
}

// pathUse is one statement's effect on a scheme path, with the contribution
// it belongs to: 0 for the common statements, and each owner's after them.
type pathUse struct {
	contribution int
	step         plangraph.StepID
	action       plangraph.Action
	path         objectidentity.ID
}

// schemePathUses indexes every effect on a scheme path by the path, and lists
// the ones that create or drop what is at a path.
func schemePathUses(common plangraph.Contribution[[]ast.Node], contributions []plangraph.Contribution[[]ast.Node]) (map[objectidentity.Key][]pathUse, []pathUse) {
	uses := make(map[objectidentity.Key][]pathUse)
	var changed []pathUse
	for index, contribution := range append([]plangraph.Contribution[[]ast.Node]{common}, contributions...) {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				if effect.Subject.Kind != ydbscheme.PathKind {
					continue
				}
				use := pathUse{contribution: index, step: step.ID, action: effect.Action, path: effect.Subject}
				uses[effect.Subject.Key()] = append(uses[effect.Subject.Key()], use)
				if use.action == plangraph.Create || use.action == plangraph.Drop {
					changed = append(changed, use)
				}
			}
		}
	}
	return uses, changed
}
