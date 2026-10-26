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
	// Lifecycle is the window of [featureplan.PhaseLifecycle]. It lets
	// operations surround the common changes whose resources they share.
	// Effects, dependencies and rewrite boundaries constrain each step.
	// Nil refuses lifecycle operations.
	Lifecycle *Span
	// LifecycleCommon names common node indexes whose established dependencies
	// replace their incidental sequence position. Each needs a known footprint
	// inside Lifecycle. IDs, operands and rewrite receipts remain unchanged.
	LifecycleCommon []int
	// CommonDependencies orders common nodes by their original indexes.
	CommonDependencies []CommonDependency
}

// CommonDependency orders common nodes without depending on assigned step IDs.
type CommonDependency struct{ Before, After int }

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
	lifecycle     bool
	sequenceLast  plangraph.StepID
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
	lifecycleWindow
	windowCount
)

var windowNames = [windowCount]string{"prerequisite-creations", "type-creations", "feature-creations", "dependent-operations",
	"feature-removals", "type-removals", "prerequisite-removals", "lifecycle-operations"}

// NewGraph builds the graph of nodes under owner. Metadata describes each node,
// or is nil when the host knows none. IDs must be empty; the graph assigns them.
// Unknown footprints order nothing beyond position. Common operands and parent
// identities reach feature owners without exposing mutable graph state. Windows
// outside the sequence or out of order are refused.
func NewGraph(owner string, nodes []ast.Node, metadata []featureplan.CommonStep, windows Windows) (*Graph, error) {
	positions, err := windowPositions(windows, len(nodes))
	if err != nil {
		return nil, err
	}
	if metadata != nil && len(metadata) != len(nodes) {
		return nil, fmt.Errorf("%w: %d metadata records for %d common nodes", schemaext.ErrInvalidValue, len(metadata), len(nodes))
	}
	movable, err := lifecycleCommon(windows, metadata, len(nodes))
	if err != nil {
		return nil, err
	}
	graph := &Graph{contribution: plangraph.Contribution[[]ast.Node]{Owner: owner}, prerequisites: windows.Prerequisites != nil,
		types: windows.Types != nil, lifecycle: windows.Lifecycle != nil}
	for i := 0; i <= len(nodes); i++ {
		graph.markWindowsAt(positions, i)
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
		if movable[i] {
			graph.appendIndependent(step, description)
		} else {
			graph.append(step, description)
		}
	}
	for _, index := range windows.LifecycleCommon {
		id := plangraph.StepID{Owner: owner, Name: fmt.Sprintf("common/%06d", index)}
		graph.contribution.Dependencies = append(graph.contribution.Dependencies,
			plangraph.Dependency{Before: graph.windows[lifecycleWindow][0], After: id},
			plangraph.Dependency{Before: id, After: graph.windows[lifecycleWindow][1]})
	}
	for _, edge := range windows.CommonDependencies {
		if edge.Before < 0 || edge.After < 0 || edge.Before >= len(nodes) || edge.After >= len(nodes) || edge.Before == edge.After {
			return nil, fmt.Errorf("%w: invalid common dependency indexes", schemaext.ErrInvalidValue)
		}
		graph.contribution.Dependencies = append(graph.contribution.Dependencies, plangraph.Dependency{
			Before: plangraph.StepID{Owner: owner, Name: fmt.Sprintf("common/%06d", edge.Before)},
			After:  plangraph.StepID{Owner: owner, Name: fmt.Sprintf("common/%06d", edge.After)}})
	}
	return graph, nil
}

func (g *Graph) markWindowsAt(positions map[int]Span, index int) {
	// The lifecycle span encloses insertion windows at either boundary too.
	// Closing a point window before entering this span creates a false cycle
	// when a lifecycle operation must precede an operation at that point.
	if span, ok := positions[lifecycleWindow]; ok && span.Start == index {
		g.windows[lifecycleWindow][0] = g.marker("common/" + windowNames[lifecycleWindow])
	}
	for _, window := range slices.Sorted(maps.Keys(positions)) {
		if window == lifecycleWindow {
			continue
		}
		if positions[window].Start == index {
			g.windows[window][0] = g.marker("common/" + windowNames[window])
		}
		if positions[window].End == index {
			g.windows[window][1] = g.marker("common/after-" + windowNames[window])
		}
	}
	if span, ok := positions[lifecycleWindow]; ok && span.End == index {
		g.windows[lifecycleWindow][1] = g.marker("common/after-" + windowNames[lifecycleWindow])
	}
}

// windowPositions validates ordered insertion points and the separate lifecycle span.
func windowPositions(windows Windows, count int) (map[int]Span, error) {
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
	if !slices.IsSorted(ordered) || ordered[0] < 0 || ordered[len(ordered)-1] > count {
		return nil, fmt.Errorf("%w: feature windows %v are out of order or outside a sequence of %d nodes", schemaext.ErrInvalidValue, ordered, count)
	}
	if span := windows.Lifecycle; span != nil {
		if span.Start < 0 || span.Start > span.End || span.End > count {
			return nil, fmt.Errorf("%w: lifecycle span %v is outside a sequence of %d nodes", schemaext.ErrInvalidValue, *span, count)
		}
		positions[lifecycleWindow] = *span
	}
	return positions, nil
}

func (g *Graph) marker(name string) plangraph.StepID {
	id := plangraph.StepID{Owner: g.contribution.Owner, Name: name}
	g.append(plangraph.Step[[]ast.Node]{ID: id}, featureplan.CommonStep{})
	return id
}

func (g *Graph) append(step plangraph.Step[[]ast.Node], description featureplan.CommonStep) {
	if g.sequenceLast != (plangraph.StepID{}) {
		g.contribution.Dependencies = append(g.contribution.Dependencies, plangraph.Dependency{Before: g.sequenceLast, After: step.ID})
	}
	g.sequenceLast = step.ID
	g.appendIndependent(step, description)
}

func (g *Graph) appendIndependent(step plangraph.Step[[]ast.Node], description featureplan.CommonStep) {
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
// dependent window; one that asked for [featureplan.PhaseLifecycle] joins
// the lifecycle span; one that asked for [featureplan.PhasePrerequisite] joins the
// prerequisite removal window when it drops an object and the prerequisite
// creation window otherwise, and is refused by a graph without them; one that
// asked for [featureplan.PhaseType] joins the type removal window when it
// drops an object no step of that phase creates again and the type creation
// window otherwise, and is refused by a graph without them; and any other
// joins the removal window when it drops an object and the creation window
// otherwise. A step that creates an object another step drops is refused
// unless both ask for the same dependent, type or lifecycle phase, because the
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
	// Lifecycle inference must see the boundaries a claimed source transfers
	// to its replacement. Otherwise an added physical-path effect can infer
	// the reverse of an explicit edge still naming the common source.
	common.Dependencies = append(common.Dependencies, rewriteBoundaries(common, features)...)
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
	case phase == featureplan.PhasePrerequisite && !g.prerequisites, phase == featureplan.PhaseType && !g.types, phase == featureplan.PhaseLifecycle && !g.lifecycle:
		return [2]plangraph.StepID{}, fmt.Errorf("%w: the %q phase has no window in this host", schemaext.ErrInvalidValue, phase)
	case phase == featureplan.PhaseLifecycle:
		return g.windows[lifecycleWindow], nil
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

// createdObjects returns every object created by a feature step. A type-phase
// drop of one of these objects is part of a replacement, so it shares the
// type creation window with the replacement's creation.
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

// refuseSplitReplacements requires a replacement to share one phase that
// permits its drop and creation to be ordered together. Default creation and
// removal windows put the creation first, which would meet the old object.
// Dependent, type and lifecycle phases each permit an owner to order both
// halves in one window. The graph still checks their effects and dependencies.
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
				if phase := features.Phases[step.ID]; phase == other.phase && (phase == featureplan.PhaseDependent || phase == featureplan.PhaseType || phase == featureplan.PhaseLifecycle) {
					continue
				}
				return fmt.Errorf("%w: feature step %s/%s creates %s, which step %s/%s drops; a replacement is one step, "+
					"or both halves ask for the dependent, type or lifecycle phase",
					ptaherr.ErrInvalidSchemaDiff, step.ID.Owner, step.ID.Name, effect.Subject, other.step.Owner, other.step.Name)
			}
		}
	}
	return nil
}
