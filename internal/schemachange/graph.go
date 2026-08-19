package schemachange

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// EdgeKind says WHY one change must precede another.
//
// A typed edge is the difference between a diagnostic an operator can act on
// and one that says a cycle exists. ADR 0001 requires edges to be typed or
// explainable; naming the reason is how a cycle report tells the author which
// declaration to change.
type EdgeKind string

const (
	// EdgeReferencedTable orders a foreign key after the table it references
	// exists, and its removal before that table goes away.
	EdgeReferencedTable EdgeKind = "referenced table"
	// EdgeOwningTable orders a foreign key after the table that carries it.
	EdgeOwningTable EdgeKind = "owning table"
)

// Edge is one ordering requirement between two changes.
type Edge struct {
	From objectidentity.Key
	To   objectidentity.Key
	Kind EdgeKind
	// Why is the sentence a cycle report prints. It is stored rather than
	// derived so the reason survives into a diagnostic unchanged.
	Why string
}

// ErrCycle reports changes that cannot be ordered because each waits for
// another.
var ErrCycle = errors.New("dependency cycle")

// Graph is the ordering information for a change set.
//
// Forward and rollback order come from THIS value, not from two orderings
// computed separately. A rollback derived from its own rules is a rollback that
// can disagree with the plan it undoes, and nothing would detect that it did
// (ADR 0001, the #1350 property list).
type Graph struct {
	changes []Change
	edges   []Edge
	index   map[objectidentity.Key]int
}

// BuildGraph derives the ordering requirements of a change set against the
// schema it applies to.
func BuildGraph(changes []Change, current, desired *schemastate.State) (*Graph, error) {
	graph := &Graph{
		changes: slices.Clone(changes),
		index:   make(map[objectidentity.Key]int),
	}
	for position, change := range graph.changes {
		if previous, duplicated := graph.index[change.ID.Key()]; duplicated {
			return nil, fmt.Errorf(
				"two changes target %s (positions %d and %d), so no order of them is the one intended",
				change.ID, previous, position)
		}
		graph.index[change.ID.Key()] = position
	}
	// Edges follow the change order, which is itself deterministic: the state
	// walks its objects in the order they were added rather than in map order.
	// Sorting them into some other order would be a second ordering rule that
	// can disagree with the first, and it would separate an edge from the
	// change it belongs to in a cycle report.
	for _, change := range graph.changes {
		graph.edges = append(graph.edges, tableEdges(change, current, desired)...)
	}
	return graph, nil
}

// tableEdges records the orderings a foreign-key change has against the tables
// it touches.
//
// A table this change set does not itself change contributes no edge: the
// ordering exists, but both endpoints have to be in the set for an edge to
// order anything. Recording an edge to a change that is not there is how a
// topological sort acquires a phantom node.
func tableEdges(change Change, current, desired *schemastate.State) []Edge {
	key := changePayload(change)
	if key == nil {
		return nil
	}
	edges := make([]Edge, 0, 2)
	referenced := key.ReferencedTable
	owning := owningTable(change, current, desired)
	for _, pair := range []struct {
		table objectidentity.ID
		kind  EdgeKind
		why   string
	}{
		{referenced, EdgeReferencedTable, fmt.Sprintf("%s references %s", change.ID, referenced)},
		{owning, EdgeOwningTable, fmt.Sprintf("%s is carried by %s", change.ID, owning)},
	} {
		if pair.table.Name.Empty() {
			continue
		}
		edges = append(edges, orderedAgainstTable(change, pair.table, pair.kind, pair.why))
	}
	return edges
}

// orderedAgainstTable orients an edge by the direction of the change.
//
// An addition waits for its table; a removal goes first, so the constraint is
// gone before the table it depends on is. One rule, two directions, and the
// rollback order in [Graph.Rollback] is the reverse of whatever this produced
// rather than a second rule that can disagree with it.
func orderedAgainstTable(change Change, table objectidentity.ID, kind EdgeKind, why string) Edge {
	if change.Operation == Remove {
		return Edge{From: change.ID.Key(), To: table.Key(), Kind: kind, Why: why}
	}
	return Edge{From: table.Key(), To: change.ID.Key(), Kind: kind, Why: why}
}

// changePayload returns the foreign key a change is about, preferring the
// desired state.
func changePayload(change Change) *schemastate.ForeignKey {
	if change.After != nil {
		return change.After
	}
	return change.Before
}

// owningTable resolves the table that carries a constraint, from the identity
// the constraint already holds.
//
// The constraint's identity carries its owning table as a component, so this is
// a read rather than a parse -- ADR 0001 invariant 3 in the one place it would
// otherwise be tempting to re-derive a name.
func owningTable(change Change, current, desired *schemastate.State) objectidentity.ID {
	parent := change.ID.Parent.Source
	for _, state := range []*schemastate.State{desired, current} {
		for _, object := range state.OfKind(objectidentity.KindTable) {
			if object.ID.String() == "table "+parent || object.ID.Name.Source == parent {
				return object.ID
			}
		}
	}
	return objectidentity.ID{}
}

// Edges returns the graph's ordering requirements.
func (g *Graph) Edges() []Edge {
	return slices.Clone(g.edges)
}

// Forward returns the changes in the order they must be applied.
func (g *Graph) Forward() ([]Change, error) {
	return g.topological()
}

// Rollback returns the changes in the order they must be undone.
//
// It is the reverse of [Graph.Forward] over the same edges. That is the whole
// point: a rollback order derived from its own traversal is one that can
// disagree with the plan it undoes.
func (g *Graph) Rollback() ([]Change, error) {
	forward, err := g.topological()
	if err != nil {
		return nil, err
	}
	reversed := slices.Clone(forward)
	slices.Reverse(reversed)
	return reversed, nil
}

// topological orders the changes, reporting a cycle in terms of the edges that
// form it rather than as a bare failure.
func (g *Graph) topological() ([]Change, error) {
	indegree := make(map[objectidentity.Key]int)
	successors := make(map[objectidentity.Key][]objectidentity.Key)
	for key := range g.index {
		indegree[key] = 0
	}
	for _, edge := range g.edges {
		if !g.holds(edge.From) || !g.holds(edge.To) {
			continue
		}
		successors[edge.From] = append(successors[edge.From], edge.To)
		indegree[edge.To]++
	}

	ready := make([]objectidentity.Key, 0, len(g.changes))
	for _, change := range g.changes {
		if indegree[change.ID.Key()] == 0 {
			ready = append(ready, change.ID.Key())
		}
	}

	ordered := make([]Change, 0, len(g.changes))
	for len(ready) > 0 {
		key := ready[0]
		ready = ready[1:]
		ordered = append(ordered, g.changes[g.index[key]])
		for _, next := range successors[key] {
			indegree[next]--
			if indegree[next] == 0 {
				ready = append(ready, next)
			}
		}
	}
	if len(ordered) != len(g.changes) {
		return nil, g.cycleError(indegree)
	}
	return ordered, nil
}

// holds reports whether a key belongs to a change in this graph.
func (g *Graph) holds(key objectidentity.Key) bool {
	_, ok := g.index[key]
	return ok
}

// cycleError names the changes still waiting and the reasons they wait, which
// is what makes a cycle actionable rather than a wall.
func (g *Graph) cycleError(indegree map[objectidentity.Key]int) error {
	stuck := make([]string, 0)
	for _, change := range g.changes {
		if indegree[change.ID.Key()] > 0 {
			stuck = append(stuck, change.String())
		}
	}
	reasons := make([]string, 0)
	for _, edge := range g.edges {
		if indegree[edge.To] > 0 {
			reasons = append(reasons, edge.Why)
		}
	}
	slices.Sort(reasons)
	reasons = slices.Compact(reasons)
	return fmt.Errorf("%w: %s cannot be ordered, because %s",
		ErrCycle, strings.Join(stuck, ", "), strings.Join(reasons, "; and "))
}
