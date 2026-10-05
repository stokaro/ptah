package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// A dropped YDB coordination node is destructive: YDB deletes it with its
// semaphores and rate limiter resources even while a session holds a lock on
// it. Creating one is safe, and changing one is a warning, as changing an
// extension is.
func TestClassifySchemaDiff_CoordinationNodes(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		CoordinationNodesAdded:    []schemamodel.CoordinationNode{{Name: "fresh"}},
		CoordinationNodesRemoved:  []schemamodel.CoordinationNode{{Name: "gone"}, {Name: "old"}},
		CoordinationNodesModified: []difftypes.CoordinationNodeChange{{Name: "locks"}},
	}

	findings := safety.ClassifySchemaDiff(diff)

	c.Assert(findings, qt.DeepEquals, []safety.Finding{
		{Category: "coordination_nodes_removed", Count: 2, Severity: safety.Destructive},
		{Category: "coordination_nodes_modified", Count: 1, Severity: safety.Warning},
		{Category: "coordination_nodes_added", Count: 1, Severity: safety.Safe},
	})
}

// The statement that drops a node is destructive whichever classifier reads
// it: the one over the plan's nodes, and the one over SQL text a migration
// file holds.
func TestClassify_DropCoordinationNodeIsDestructive(t *testing.T) {
	c := qt.New(t)
	const reason = "DROP COORDINATION NODE removes the node with its semaphores and rate limiter resources, " +
		"even while a session holds a lock on it"

	nodes := safety.Assess([]ast.Node{
		&ast.CreateCoordinationNodeNode{Name: "fresh"},
		&ast.AlterCoordinationNodeNode{Name: "locks"},
		&ast.DropCoordinationNodeNode{Name: "gone"},
	})
	text := safety.AssessSQL("DROP COORDINATION NODE `app/gone`")

	c.Assert([]safety.Severity{nodes[0].Severity, nodes[1].Severity, nodes[2].Severity}, qt.DeepEquals,
		[]safety.Severity{safety.Safe, safety.Safe, safety.Destructive})
	c.Assert(nodes[2].Reason, qt.Equals, reason)
	c.Assert(text.Severity, qt.Equals, safety.Destructive)
	c.Assert(text.Reason, qt.Equals, reason)
}
