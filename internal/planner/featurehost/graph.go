package featurehost

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Windows records where a host's common sequence leaves room for feature
// operations, as indexes into its node list: the operation joins the window
// before the node at that index.
//
// Creation and Removal are the windows of [featureplan.PhaseDefault]: an
// object created after the tables and columns it may read and before the
// views that may read it, and dropped after those views and before the
// tables. DependentCreation and DependentRemoval are the windows of
// [featureplan.PhaseDependent]: an object that names objects of every family
// and that nothing common reads, created after the host creates and changes
// them and dropped before the host removes any of them. The indexes are
// nondecreasing in that order, so windows at one index nest in that order and
// a dependent object replaced under a new name exists throughout.
type Windows struct {
	Creation, DependentCreation, DependentRemoval, Removal int
}

// Graph is a host's common sequence as graph units, one per node, with an
// empty marker pair around each window. Build it with [NewGraph], hand
// [Graph.CommonSteps] to the owners in the planning request, and join their
// reply with [Graph.Schedule].
type Graph struct {
	contribution plangraph.Contribution[[]ast.Node]
	steps        []featureplan.CommonStep
	windows      [4][2]plangraph.StepID
}

const (
	creationWindow = iota
	dependentCreationWindow
	dependentRemovalWindow
	removalWindow
)

// NewGraph builds the graph of nodes under owner. effects holds each node's
// effects, or is nil when the host knows none; a node with an unknown
// footprint orders nothing beyond its position. Windows outside the sequence
// or out of order are refused.
func NewGraph(owner string, nodes []ast.Node, effects [][]plangraph.Effect, windows Windows) (*Graph, error) {
	positions := []int{windows.Creation, windows.DependentCreation, windows.DependentRemoval, windows.Removal}
	if !slices.IsSorted(positions) || positions[0] < 0 || positions[3] > len(nodes) {
		return nil, fmt.Errorf("%w: feature windows %v are out of order or outside a sequence of %d nodes", schemaext.ErrInvalidValue, positions, len(nodes))
	}
	if effects != nil && len(effects) != len(nodes) {
		return nil, fmt.Errorf("%w: %d effect lists for %d common nodes", schemaext.ErrInvalidValue, len(effects), len(nodes))
	}
	graph := &Graph{contribution: plangraph.Contribution[[]ast.Node]{Owner: owner}}
	names := [4]string{"feature-creations", "dependent-creations", "dependent-removals", "feature-removals"}
	for i := 0; i <= len(nodes); i++ {
		for window, position := range positions {
			if position == i {
				graph.windows[window] = [2]plangraph.StepID{graph.marker("common/" + names[window]), graph.marker("common/after-" + names[window])}
			}
		}
		if i == len(nodes) {
			break
		}
		step := plangraph.Step[[]ast.Node]{ID: plangraph.StepID{Owner: owner, Name: fmt.Sprintf("common/%06d", i)}, Payload: []ast.Node{nodes[i]}}
		if effects != nil {
			step.Effects = slices.Clone(effects[i])
		}
		graph.append(step)
	}
	return graph, nil
}

func (g *Graph) marker(name string) plangraph.StepID {
	id := plangraph.StepID{Owner: g.contribution.Owner, Name: name}
	g.append(plangraph.Step[[]ast.Node]{ID: id})
	return id
}

func (g *Graph) append(step plangraph.Step[[]ast.Node]) {
	if count := len(g.contribution.Steps); count > 0 {
		g.contribution.Dependencies = append(g.contribution.Dependencies, plangraph.Dependency{Before: g.contribution.Steps[count-1].ID, After: step.ID})
	}
	g.contribution.Steps = append(g.contribution.Steps, step)
	g.steps = append(g.steps, featureplan.CommonStep{ID: step.ID, Effects: slices.Clone(step.Effects)})
}

// CommonSteps returns the steps an owner orders itself against, markers
// included, as independent copies.
func (g *Graph) CommonSteps() []featureplan.CommonStep {
	steps := make([]featureplan.CommonStep, len(g.steps))
	for i, step := range g.steps {
		steps[i] = step.Clone()
	}
	return steps
}

// Schedule joins the owners' contributions to the common sequence and returns
// the nodes in plan order. Each feature step joins one window: a step that
// drops an object joins a removal window and every other step a creation
// window, the dependent ones when its operation asked for
// [featureplan.PhaseDependent]. The effects of different contributions are
// then ordered by [plangraph.LifecycleDependencies], so an owner that creates
// an object precedes another owner that reads it. The complete graph is
// validated before any node is returned, so a conflict leaves no prefix; it
// is refused with [ptaherr.ErrInvalidSchemaDiff]. g is not changed.
func (g *Graph) Schedule(ctx context.Context, features Result) ([]ast.Node, error) {
	common := plangraph.Contribution[[]ast.Node]{Owner: g.contribution.Owner,
		Steps: slices.Clone(g.contribution.Steps), Dependencies: slices.Clone(g.contribution.Dependencies)}
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			window := g.window(features.Phases[step.ID], slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool {
				return effect.Action == plangraph.Drop
			}))
			common.Dependencies = append(common.Dependencies,
				plangraph.Dependency{Before: window[0], After: step.ID}, plangraph.Dependency{Before: step.ID, After: window[1]})
		}
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
	return nodes, ctx.Err()
}

func (g *Graph) window(phase featureplan.Phase, drops bool) [2]plangraph.StepID {
	switch {
	case phase == featureplan.PhaseDependent && drops:
		return g.windows[dependentRemovalWindow]
	case phase == featureplan.PhaseDependent:
		return g.windows[dependentCreationWindow]
	case drops:
		return g.windows[removalWindow]
	default:
		return g.windows[creationWindow]
	}
}
