package featurehost

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
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
// tables. The two are apart, so a default operation that replaces an object
// is one step.
//
// Dependent is the window of [featureplan.PhaseDependent]: an object that
// names objects of every family and that nothing common reads, such as a
// row-security policy. It lies after the host creates and changes those
// objects and before the host removes any of them, and creations and drops
// share it. Which of a replacement's halves runs first is the owner's
// decision: a policy replaced under its own name is dropped first, and
// whether one renamed keeps both for a moment or neither depends on what it
// admits, which only its owner knows.
//
// The indexes are nondecreasing in the order Creation, Dependent, Removal, so
// windows at one index nest in that order.
//
// Prerequisites holds the windows of [featureplan.PhasePrerequisite], and
// Types those of [featureplan.PhaseType]; each is nil for a host that has
// none.
type Windows struct {
	Creation, Dependent, Removal int
	Prerequisites                *PrerequisiteWindows
	Types                        *TypeWindows
}

// PrerequisiteWindows are the two windows of [featureplan.PhasePrerequisite],
// for an object every other object may use, such as a PostgreSQL extension.
// Creation lies after the host creates schemas and before it creates or
// changes any other object, so it is at or before [Windows.Creation]; Removal
// lies after the host removes every other object, so it is at or after
// [Windows.Removal]. A creation or a change joins the first and a drop the
// second.
type PrerequisiteWindows struct {
	Creation, Removal int
}

// TypeWindows are the two windows of [featureplan.PhaseType], for a data type
// columns, routine signatures and other types may name, such as a PostgreSQL
// domain. Creation lies after the host creates the enums and the routines
// whose signatures name no type the plan creates, and before it creates those
// routines that do and the tables, so it is at or after
// [PrerequisiteWindows.Creation] and at or before [Windows.Creation]. Removal
// lies after the host removes tables, routines and roles and before it
// removes enums, so it is at or after [Windows.Removal] and at or before
// [PrerequisiteWindows.Removal].
//
// A step that drops a type no step of the phase creates again joins Removal.
// Every other step joins Creation: a creation, a change in place, and both
// halves of a type dropped and created again, which its owner orders. A
// rebuilt type is back before the tables, as it must be for a column to name
// it. Each span includes common type operations; owned operations may precede
// or follow them when their definitions require that order.
type TypeWindows struct {
	Creation, Removal Span
}

// Span encloses common operations between Start (inclusive) and End
// (exclusive). Feature steps may run anywhere inside it, subject to their
// dependencies on those operations. Equal bounds describe an insertion point.
type Span struct{ Start, End int }

// Graph is a host's common sequence as graph units, one per node, with an
// empty marker pair around each window. Build it with [NewGraph], hand
// [Graph.CommonSteps] to the owners in the planning request, and join their
// reply with [Graph.Schedule].
type Graph struct {
	contribution  plangraph.Contribution[[]ast.Node]
	steps         []featureplan.CommonStep
	windows       [windowCount][2]plangraph.StepID
	prerequisites bool
	types         bool
}

// The windows in the order their positions must keep.
const (
	prerequisiteCreationWindow = iota
	typeCreationWindow
	creationWindow
	dependentWindow
	removalWindow
	typeRemovalWindow
	prerequisiteRemovalWindow
	windowCount
)

var windowNames = [windowCount]string{"prerequisite-creations", "type-creations", "feature-creations", "dependent-operations",
	"feature-removals", "type-removals", "prerequisite-removals"}

// NewGraph builds the graph of nodes under owner. Metadata describes each node,
// or is nil when the host knows none. IDs must be empty; the graph assigns them.
// Unknown footprints order nothing beyond position. Common operands and parent
// identities reach feature owners without exposing mutable graph state. Windows
// outside the sequence or out of order are refused.
func NewGraph(owner string, nodes []ast.Node, metadata []featureplan.CommonStep, windows Windows) (*Graph, error) {
	positions := map[int]Span{creationWindow: {windows.Creation, windows.Creation}, dependentWindow: {windows.Dependent, windows.Dependent}, removalWindow: {windows.Removal, windows.Removal}}
	if windows.Prerequisites != nil {
		positions[prerequisiteCreationWindow] = Span{windows.Prerequisites.Creation, windows.Prerequisites.Creation}
		positions[prerequisiteRemovalWindow] = Span{windows.Prerequisites.Removal, windows.Prerequisites.Removal}
	}
	if windows.Types != nil {
		positions[typeCreationWindow] = windows.Types.Creation
		positions[typeRemovalWindow] = windows.Types.Removal
	}
	ordered := make([]int, 0, len(positions))
	for _, window := range slices.Sorted(maps.Keys(positions)) {
		ordered = append(ordered, positions[window].Start, positions[window].End)
	}
	if !slices.IsSorted(ordered) || ordered[0] < 0 || ordered[len(ordered)-1] > len(nodes) {
		return nil, fmt.Errorf("%w: feature windows %v are out of order or outside a sequence of %d nodes", schemaext.ErrInvalidValue, ordered, len(nodes))
	}
	if metadata != nil && len(metadata) != len(nodes) {
		return nil, fmt.Errorf("%w: %d metadata records for %d common nodes", schemaext.ErrInvalidValue, len(metadata), len(nodes))
	}
	graph := &Graph{contribution: plangraph.Contribution[[]ast.Node]{Owner: owner}, prerequisites: windows.Prerequisites != nil,
		types: windows.Types != nil}
	for i := 0; i <= len(nodes); i++ {
		for _, window := range slices.Sorted(maps.Keys(positions)) {
			if positions[window].Start == i {
				graph.windows[window][0] = graph.marker("common/" + windowNames[window])
			}
			if positions[window].End == i {
				graph.windows[window][1] = graph.marker("common/after-" + windowNames[window])
			}
		}
		if i == len(nodes) {
			break
		}
		step := plangraph.Step[[]ast.Node]{ID: plangraph.StepID{Owner: owner, Name: fmt.Sprintf("common/%06d", i)}, Payload: []ast.Node{nodes[i]}}
		var description featureplan.CommonStep
		if metadata != nil {
			description = metadata[i].Clone()
			if description.ID != (plangraph.StepID{}) {
				return nil, fmt.Errorf("%w: common node metadata supplies a step ID", schemaext.ErrInvalidValue)
			}
			step.Effects, step.Transaction, step.Impact = description.Effects, description.Transaction, description.Impact
		}
		graph.append(step, description)
	}
	return graph, nil
}

func (g *Graph) marker(name string) plangraph.StepID {
	id := plangraph.StepID{Owner: g.contribution.Owner, Name: name}
	g.append(plangraph.Step[[]ast.Node]{ID: id}, featureplan.CommonStep{})
	return id
}

func (g *Graph) append(step plangraph.Step[[]ast.Node], description featureplan.CommonStep) {
	if count := len(g.contribution.Steps); count > 0 {
		g.contribution.Dependencies = append(g.contribution.Dependencies, plangraph.Dependency{Before: g.contribution.Steps[count-1].ID, After: step.ID})
	}
	g.contribution.Steps = append(g.contribution.Steps, step)
	description.ID = step.ID
	g.steps = append(g.steps, description.Clone())
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
// the nodes in plan order. A default-phase rewrite replacement inherits its
// sources' boundaries instead of joining a feature window. An explicit phase
// adds its window constraints to those boundaries. Other steps join one window:
// a step whose operation asked for [featureplan.PhaseDependent] joins the
// dependent window; one that asked for [featureplan.PhasePrerequisite] joins the
// prerequisite removal window when it drops an object and the prerequisite
// creation window otherwise, and is refused by a graph without them; one that
// asked for [featureplan.PhaseType] joins the type removal window when it
// drops an object no step of that phase creates again and the type creation
// window otherwise, and is refused by a graph without them; and any other
// joins the removal window when it drops an object and the creation window
// otherwise. A step that creates an object another step drops is refused
// unless both are dependent or both ask for the type phase, because the
// windows would run the creation first. The effects of different
// contributions are then ordered by [plangraph.LifecycleDependencies], so an
// owner that creates an object precedes another owner that reads it. The
// complete graph is validated before any node is returned, so a conflict
// leaves no prefix, including a replacement its owner left unordered; it is
// refused with [ptaherr.ErrInvalidSchemaDiff]. g is not changed.
func (g *Graph) Schedule(ctx context.Context, features Result) ([]ast.Node, error) {
	if err := refuseSplitReplacements(features); err != nil {
		return nil, err
	}
	common := plangraph.Contribution[[]ast.Node]{Owner: g.contribution.Owner,
		Steps: slices.Clone(g.contribution.Steps), Dependencies: slices.Clone(g.contribution.Dependencies)}
	replacements := make(map[plangraph.StepID]bool, len(features.Rewrites))
	for _, rewrite := range features.Rewrites {
		replacements[rewrite.Replacement] = true
	}
	recreated := createdObjects(features)
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			// ScheduleRewritten validates the claim and transfers source edges.
			// Adding the default window here can contradict those same edges.
			if replacements[step.ID] && features.Phases[step.ID] == featureplan.PhaseDefault {
				continue
			}
			drops, rebuilds := false, false
			for _, effect := range step.Effects {
				if effect.Action == plangraph.Drop {
					drops = true
					rebuilds = rebuilds || recreated[effect.Subject.Key()]
				}
			}
			window, err := g.window(features.Phases[step.ID], drops, rebuilds)
			if err != nil {
				return nil, fmt.Errorf("feature step %s/%s: %w", step.ID.Owner, step.ID.Name, err)
			}
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

// window is the window a step of phase joins. drops says the step drops an
// object, and rebuilds that a step of the type phase creates one of the
// objects it drops again.
func (g *Graph) window(phase featureplan.Phase, drops, rebuilds bool) ([2]plangraph.StepID, error) {
	switch {
	case phase == featureplan.PhaseDependent:
		return g.windows[dependentWindow], nil
	case phase == featureplan.PhasePrerequisite && !g.prerequisites, phase == featureplan.PhaseType && !g.types:
		return [2]plangraph.StepID{}, fmt.Errorf("%w: the %q phase has no window in this host", schemaext.ErrInvalidValue, phase)
	case phase == featureplan.PhasePrerequisite && drops:
		return g.windows[prerequisiteRemovalWindow], nil
	case phase == featureplan.PhasePrerequisite:
		return g.windows[prerequisiteCreationWindow], nil
	case phase == featureplan.PhaseType && drops && !rebuilds:
		return g.windows[typeRemovalWindow], nil
	case phase == featureplan.PhaseType:
		return g.windows[typeCreationWindow], nil
	case drops:
		return g.windows[removalWindow], nil
	default:
		return g.windows[creationWindow], nil
	}
}

// createdObjects is every object a step creates. A step of the type phase
// that drops one of them rebuilds it rather than removes it. The creating
// step asks for the type phase too, or is the dropping step itself:
// [refuseSplitReplacements] has refused a drop and a creation of one object in
// two steps of any other pair of phases before this is read.
func createdObjects(features Result) map[objectidentity.Key]bool {
	created := make(map[objectidentity.Key]bool)
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				if effect.Action == plangraph.Create {
					created[effect.Subject.Key()] = true
				}
			}
		}
	}
	return created
}

// refuseSplitReplacements refuses a step that creates an object another step
// drops, unless both ask for the dependent phase or both for the type phase.
// The default windows run every creation before every drop, and the
// dependent window is not ordered against them by object, so the creation
// would meet the object it replaces. Such a replacement is one step, or both
// halves ask for the dependent phase or for the type phase, whose owner
// orders them in one window.
func refuseSplitReplacements(features Result) error {
	type use struct {
		step  plangraph.StepID
		phase featureplan.Phase
	}
	dropped := make(map[objectidentity.Key]use)
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				if effect.Action == plangraph.Drop {
					dropped[effect.Subject.Key()] = use{step: step.ID, phase: features.Phases[step.ID]}
				}
			}
		}
	}
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				other, found := dropped[effect.Subject.Key()]
				if !found || effect.Action != plangraph.Create || other.step == step.ID {
					continue
				}
				if phase := features.Phases[step.ID]; phase == other.phase && (phase == featureplan.PhaseDependent || phase == featureplan.PhaseType) {
					continue
				}
				return fmt.Errorf("%w: feature step %s/%s creates %s, which step %s/%s drops; a replacement is one step, "+
					"or both halves ask for the dependent or the type phase",
					ptaherr.ErrInvalidSchemaDiff, step.ID.Owner, step.ID.Name, effect.Subject, other.step.Owner, other.step.Name)
			}
		}
	}
	return nil
}
