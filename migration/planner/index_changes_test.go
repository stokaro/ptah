package planner_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesIndexChangesInPlace drives a hand-built diff
// that renames an index, one that writes an index comment, and one that
// changes a YDB index's partitioning through every registered planner but
// YDB's. The comparison records none of them on those targets, so only such a
// diff reaches them, and a planner that read none of them would plan nothing
// and report the database synced.
func TestEveryPlannerButYDBRefusesIndexChangesInPlace(t *testing.T) {
	c := qt.New(t)
	dialects, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool { return dialect == platform.YDB })
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("registered dialects: %v", dialects))

	diffs := map[string]*difftypes.SchemaDiff{
		"rename": {IndexesRenamed: []difftypes.IndexRename{{TableName: "users", From: "a", To: "b"}}},
		"partitioning": {FeatureChanges: []schemaext.ChangeRecord{{
			Subject: objectidentity.NewBuilder(identifier.ForDialect(platform.YDB)).IndexParts("", "users", "a"),
			Value: &ydbdiff.IndexPartitioning{After: &ydbschema.DesiredIndexPartitioning{
				IndexPartitioning: ydbschema.IndexPartitioning{MinPartitions: 2},
			}},
		}}},
		"comment": {IndexCommentsChanged: []difftypes.IndexCommentChange{{TableName: "users", Name: "a", Desired: "x"}}},
	}
	for _, dialect := range dialects {
		for name, diff := range diffs {
			t.Run(dialect+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				nodes, err := planner.GenerateSchemaDiffAST(
					context.Background(), builtintest.Runtime(),
					diff, dialect,
				)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(nodes, qt.IsNil)
			})
		}
	}
}
