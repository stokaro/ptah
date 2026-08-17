package schemachange

// White-box testing required: the cycle diagnostic guards an ordering failure
// the FOREIGN-KEY slice cannot produce, so exercising it needs an edge set the
// public constructor cannot derive.
//
// The reason is specific.
//
// Every edge BuildGraph derives runs between a table and a constraint, and a
// table that is not itself changing contributes no node, so the derived graph
// for this slice is a forest. The guard is there for the families that join the
// model later -- table creations order against each other, and that is where a
// cycle is reachable -- and an unreachable guard with no test is a guard nobody
// has ever seen work.
//
// Testing it needs an edge set the public constructor cannot derive, so the
// test builds one directly.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// TestTopological_CycleNamesTheChangesAndTheReasons pins that a cycle report is
// actionable: it says which changes could not be ordered and why each waits.
//
// "A cycle exists" tells an author that something is wrong and not which
// declaration to change, which is the difference a typed edge buys.
func TestTopological_CycleNamesTheChangesAndTheReasons(t *testing.T) {
	c := qt.New(t)
	first := constraintID("public", "a", "fk_a_b")
	second := constraintID("public", "b", "fk_b_a")
	graph := &Graph{
		changes: []Change{
			{ID: first, Operation: Add},
			{ID: second, Operation: Add},
		},
		index: map[objectidentity.Key]int{first.Key(): 0, second.Key(): 1},
		edges: []Edge{
			{From: first.Key(), To: second.Key(), Kind: EdgeReferencedTable, Why: "fk_b_a waits for a"},
			{From: second.Key(), To: first.Key(), Kind: EdgeReferencedTable, Why: "fk_a_b waits for b"},
		},
	}

	_, err := graph.Forward()

	c.Assert(err, qt.ErrorIs, ErrCycle)
	c.Assert(err.Error(), qt.Contains, "fk_a_b")
	c.Assert(err.Error(), qt.Contains, "fk_b_a")
	c.Assert(err.Error(), qt.Contains, "waits for")
}

// TestTopological_AcyclicGraphOrdersEverything is the control. A traversal that
// reported a cycle for every input would satisfy the row above.
func TestTopological_AcyclicGraphOrdersEverything(t *testing.T) {
	c := qt.New(t)
	first := constraintID("public", "a", "fk_a_b")
	second := constraintID("public", "b", "fk_b_a")
	graph := &Graph{
		changes: []Change{
			{ID: first, Operation: Add},
			{ID: second, Operation: Add},
		},
		index: map[objectidentity.Key]int{first.Key(): 0, second.Key(): 1},
		edges: []Edge{
			{From: first.Key(), To: second.Key(), Kind: EdgeReferencedTable, Why: "fk_b_a waits for a"},
		},
	}

	ordered, err := graph.Forward()

	c.Assert(err, qt.IsNil)
	c.Assert(ordered, qt.HasLen, 2)
	c.Assert(ordered[0].ID.Name.Source, qt.Equals, "fk_a_b")
}

// TestBuildGraph_DerivesNoCycleForThisSlice states the property the file header
// relies on, so the claim is checked rather than asserted in a comment.
//
// Two tables referencing each other is the shape that would be a cycle if
// anything ordered the constraints against each other. It is not one: both
// tables already exist, so each foreign key waits only on tables outside the
// change set, and either order applies.
func TestBuildGraph_DerivesNoCycleForThisSlice(t *testing.T) {
	c := qt.New(t)
	state := schemastate.New("postgres",
		objectidentity.KindTable, objectidentity.KindColumn, objectidentity.KindConstraint)
	changes := []Change{
		{ID: constraintID("public", "a", "fk_a_b"), Operation: Add, After: &schemastate.ForeignKey{
			ReferencedTable: tableID("public", "b"),
		}},
		{ID: constraintID("public", "b", "fk_b_a"), Operation: Add, After: &schemastate.ForeignKey{
			ReferencedTable: tableID("public", "a"),
		}},
	}

	graph, err := BuildGraph(changes, state, state)
	c.Assert(err, qt.IsNil)
	ordered, orderErr := graph.Forward()

	c.Assert(orderErr, qt.IsNil)
	c.Assert(ordered, qt.HasLen, 2)
}

// TestBuildGraph_RefusesTwoChangesForOneObject pins that a change set cannot
// carry two changes to one object.
//
// No order of them is the one intended, and picking either is a guess about
// which the author meant.
func TestBuildGraph_RefusesTwoChangesForOneObject(t *testing.T) {
	c := qt.New(t)
	state := schemastate.New("postgres", objectidentity.KindConstraint)
	id := constraintID("public", "a", "fk_a_b")

	_, err := BuildGraph([]Change{{ID: id, Operation: Add}, {ID: id, Operation: Remove}}, state, state)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "two changes target")
}

func constraintID(schema, table, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).ConstraintParts(schema, table, name)
}

func tableID(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts(schema, name)
}
