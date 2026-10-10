package ydb

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) scheduleFeatureChanges(
	ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff,
	rebuilds map[string]*tableRebuild, semantics identifier.Semantics, reads commonReads, before, after []ast.Node,
) ([]ast.Node, error) {
	graph, err := commonGraph(semantics, diff.CurrentDatabasePath, reads, before, after)
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
	reads          commonReads
}

// commonReads records what a host statement reads that
// [ydbscheme.CommonEffects] cannot see in it, a raw SQL statement's above all,
// and whether it reads as an early reader: one that uses the object as it was
// and runs before another owner alters it (see
// [plangraph.LifecycleDependencies]).
type commonReads map[ast.Node]commonRead

type commonRead struct {
	effects []plangraph.Effect
	early   bool
}

// add records what node reads. A node that reads nothing is left out.
func (r commonReads) add(node ast.Node, effects []plangraph.Effect, early bool) {
	if len(effects) > 0 {
		r[node] = commonRead{effects: effects, early: early}
	}
}

// commonGraph gives each accepted statement an identity before owner planning.
// The existing common phases retain their order. Only recognized scheme
// operations supply footprints; other operations retain unknown metadata.
// root is the database the plan runs in, which an absolute secret path is read
// against; see [ydbscheme.CommonEffects]. reads adds what a statement reads
// that its node does not say.
func commonGraph(semantics identifier.Semantics, root string, reads commonReads, before, after []ast.Node) (commonPlan, error) {
	graph := commonPlan{contribution: plangraph.Contribution[[]ast.Node]{Owner: "ptah.run/ydb"}, root: root, reads: reads}
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
		read := g.reads[node]
		effects = append(effects, read.effects...)
		step := plangraph.Step[[]ast.Node]{ID: plangraph.StepID{Owner: g.contribution.Owner, Name: fmt.Sprintf("common/%s/%06d", phase, i)}, Payload: []ast.Node{node}, Effects: effects}
		if read.early {
			step.Placement = plangraph.PlacementEarly
		}
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
	if err := refuseDroppedSecretReads(append([]plangraph.Contribution[[]ast.Node]{common}, features.Contributions...)); err != nil {
		return nil, err
	}
	if err := refuseCreationOverDirectory(append([]plangraph.Contribution[[]ast.Node]{common}, features.Contributions...)); err != nil {
		return nil, err
	}
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

// refuseDroppedSecretReads refuses a plan in which one contribution drops a
// secret that a statement of another reads by its path, such as an external
// data source the plan creates naming a secret the plan drops. YDB records no
// dependency on a secret (DROP SECRET succeeds while a data source names it,
// measured on 26.2.1.14), so the plan would apply and leave the reader naming
// a secret that is gone. The secret's owner refuses a common statement that
// reads a secret it drops; only the host sees what other owners read.
func refuseDroppedSecretReads(contributions []plangraph.Contribution[[]ast.Node]) error {
	dropped := make(map[objectidentity.Key]int)
	for index, contribution := range contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				if schemaext.Kind(effect.Subject.Kind) == ydbsecret.Kind && effect.Action == plangraph.Drop {
					dropped[effect.Subject.Key()] = index
				}
			}
		}
	}
	for index, contribution := range contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				owner, gone := dropped[effect.Subject.Key()]
				if !gone || owner == index || effect.Action != plangraph.Read {
					continue
				}
				name := ydbsecret.Display(effect.Subject.Schema.Source, effect.Subject.Name.Source)
				return (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
					Code: schemavalidation.InvalidSchema, Kind: "secret", Object: name,
					Message: "secret " + name + " is dropped while a statement of this plan reads it by its path",
				}}}).Err(platform.YDB)
			}
		}
	}
	return nil
}

// refuseCreationOverDirectory refuses a plan that drops an object below a
// scheme path and creates an object at that path.
//
// The path is a directory in the database, and YDB keeps a directory after its
// last object is dropped: measured on 26.2.1.14, CREATE TABLE at the path of a
// directory a plan had just emptied fails with "Path is not a table or topic",
// and CREATE TOPIC and CREATE SECRET with "unexpected path type". YQL has no
// statement that removes a directory, so no order of this plan's statements
// can apply it, and the next plan would plan the same creation again
// (stokaro/ptah#4282). Refusing names the path before anything runs.
func refuseCreationOverDirectory(contributions []plangraph.Contribution[[]ast.Node]) error {
	created := make(map[objectidentity.Key]bool)
	var dropped []objectidentity.ID
	for _, contribution := range contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				switch {
				case effect.Subject.Kind != ydbscheme.PathKind:
				case effect.Action == plangraph.Create:
					created[effect.Subject.Key()] = true
				case effect.Action == plangraph.Drop:
					dropped = append(dropped, effect.Subject)
				}
			}
		}
	}
	for _, drop := range dropped {
		for _, directory := range ydbscheme.DirectoriesAbove(drop) {
			if !created[directory.Key()] {
				continue
			}
			name := schemePathDisplay(directory)
			return (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
				Code: schemavalidation.InvalidSchema, Kind: "scheme path", Object: name,
				Message: "this plan drops " + schemePathDisplay(drop) + " and creates an object at " + name +
					", which is a directory in the database. YDB keeps a directory after its last object is dropped, " +
					"and YQL has no statement that removes one: drop the objects below " + name +
					" and remove the directory first (for example with `ydb scheme rmdir`), or declare the object at another path",
			}}}).Err(platform.YDB)
		}
	}
	return nil
}

// schemePathDisplay writes a scheme path the way YQL names the object at it.
func schemePathDisplay(id objectidentity.ID) string {
	if id.Schema.Source == "" {
		return id.Name.Source
	}
	return id.Schema.Source + "/" + id.Name.Source
}
