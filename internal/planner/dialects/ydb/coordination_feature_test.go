package ydb_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// The public native planner joins standalone owner changes to common table
// steps. Each path is freed before another family creates its replacement.
func TestCoordinationFeaturesTradePathsWithTables(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded:   difftypes.TableChanges{{Name: "app.was_node", Table: schemamodel.Table{StructName: "S", Schema: "app", Name: "was_node"}, Fields: []schemamodel.Field{keyField("id")}}},
		TablesRemoved: difftypes.TableRemovals{{Name: "app.was_table", Current: observedFeeds(t, "app", "was_table")}},
		FeatureChanges: []schemaext.ChangeRecord{
			{Subject: ydbcoordination.Ref("app", "was_table"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}},
			{Subject: ydbcoordination.Ref("app", "was_node"), Value: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}},
		},
	}
	got := render(c, capability.YDB262(), diff)
	dropNode := strings.Index(got, "DROP COORDINATION NODE `app/was_node`;")
	createTable := strings.Index(got, "CREATE TABLE `app/was_node`")
	dropTable := strings.Index(got, "DROP TABLE `app/was_table`;")
	createNode := strings.Index(got, "CREATE COORDINATION NODE `app/was_table`;")
	c.Assert(dropNode >= 0, qt.IsTrue)
	c.Assert(createTable > dropNode, qt.IsTrue)
	c.Assert(dropTable >= 0, qt.IsTrue)
	c.Assert(createNode > dropTable, qt.IsTrue)
}

// TestCoordinationFeaturesTradePathsWithTopics trades two paths between two
// owners, a coordination node and a topic, which neither owner sees: each
// creation follows the drop that frees its path.
func TestCoordinationFeaturesTradePathsWithTopics(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{
			{Subject: ydbtopic.Ref("app", "was_node"), Value: &ydbdiff.Topic{After: &ydbtopic.Desired{}}},
			{Subject: ydbtopic.Ref("app", "was_topic"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{}}},
			{Subject: ydbcoordination.Ref("app", "was_topic"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}},
			{Subject: ydbcoordination.Ref("app", "was_node"), Value: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}},
		},
	}
	got := render(c, capability.YDB262(), diff)
	dropNode := strings.Index(got, "DROP COORDINATION NODE `app/was_node`;")
	createTopic := strings.Index(got, "CREATE TOPIC `app/was_node`")
	dropTopic := strings.Index(got, "DROP TOPIC `app/was_topic`;")
	createNode := strings.Index(got, "CREATE COORDINATION NODE `app/was_topic`;")
	c.Assert(dropNode >= 0, qt.IsTrue)
	c.Assert(createTopic > dropNode, qt.IsTrue)
	c.Assert(dropTopic >= 0, qt.IsTrue)
	c.Assert(createNode > dropTopic, qt.IsTrue)
}

func TestCoordinationFeaturesRefuseAPathClaimedByAnotherCreation(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	diff := &difftypes.SchemaDiff{
		TablesAdded:    difftypes.TableChanges{{Name: "app.shared", Table: schemamodel.Table{StructName: "S", Schema: "app", Name: "shared"}, Fields: []schemamodel.Field{keyField("id")}}},
		FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbcoordination.Ref("app", "shared"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}},
	}
	nodes, err := ydb.New().GenerateMigrationAST(t.Context(), runtime, diff)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*coordination create conflicts with create at scheme path.*`)
	c.Assert(nodes, qt.IsNil)
}
