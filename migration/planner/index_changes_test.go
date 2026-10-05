package planner_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesIndexChangesInPlace drives a hand-built diff
// that renames an index, and one that changes an index's partitioning, through
// every registered planner but YDB's. The comparison records neither on those
// targets, so only such a diff reaches them, and a planner that read neither
// list would plan nothing and report the database synced.
func TestEveryPlannerButYDBRefusesIndexChangesInPlace(t *testing.T) {
	c := qt.New(t)
	dialects, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool { return dialect == platform.YDB })
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("registered dialects: %v", dialects))

	diffs := map[string]*difftypes.SchemaDiff{
		"rename": {IndexesRenamed: []difftypes.IndexRename{{TableName: "users", From: "a", To: "b"}}},
		"partitioning": {IndexPartitioningChanged: []difftypes.IndexPartitioningChange{
			{TableName: "users", Name: "a", Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 2}},
		}},
		"comment": {IndexCommentsChanged: []difftypes.IndexCommentChange{{TableName: "users", Name: "a", Desired: "x"}}},
	}
	for _, dialect := range dialects {
		for name, diff := range diffs {
			t.Run(dialect+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				nodes, err := planner.GenerateSchemaDiffAST(diff, dialect)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(nodes, qt.IsNil)
			})
		}
	}
}
