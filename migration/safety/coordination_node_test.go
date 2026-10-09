package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// A dropped YDB coordination node is destructive: YDB deletes it with its
// semaphores and rate limiter resources even while a session holds a lock on
// it. Creating one is safe, and changing one is a warning, as changing an
// extension is.
func TestClassifySchemaDiff_CoordinationNodes(t *testing.T) {
	tests := []struct {
		name     string
		change   *ydbdiff.CoordinationNode
		severity safety.Severity
	}{
		{name: "create", change: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}, severity: safety.Safe},
		{name: "alter", change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}}, severity: safety.Warning},
		{name: "drop", change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}, severity: safety.Destructive},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbcoordination.Ref("app", "locks"), Value: test.change}}}
			c.Assert(safety.ClassifySchemaDiff(diff), qt.DeepEquals, []safety.Finding{{Category: "feature_changes:" + string(ydbdiff.CoordinationNodeKind), Count: 1, Severity: test.severity}})
		})
	}
}

// The statement that drops a node is destructive whichever classifier reads
// it: the one over the plan's nodes, and the one over SQL text a migration
// file holds.
func TestClassify_DropCoordinationNodeIsDestructive(t *testing.T) {
	c := qt.New(t)
	const reason = "DROP COORDINATION NODE removes the node with its semaphores and rate limiter resources, " +
		"even while a session holds a lock on it"

	nodes := safety.Assess([]ast.Node{
		&ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "fresh", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{}}}}},
		&ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{}}}}},
		&ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "gone", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}}},
	})
	text := safety.AssessSQL("DROP COORDINATION NODE `app/gone`")

	c.Assert([]safety.Severity{nodes[0].Severity, nodes[1].Severity, nodes[2].Severity}, qt.DeepEquals,
		[]safety.Severity{safety.Safe, safety.Warning, safety.Destructive})
	c.Assert(nodes[2].Reason, qt.Equals, reason)
	c.Assert(text.Severity, qt.Equals, safety.Destructive)
	c.Assert(text.Reason, qt.Equals, reason)
}
