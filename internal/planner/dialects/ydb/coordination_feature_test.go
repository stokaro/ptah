package ydb_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
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

func TestCoordinationFeaturesTradePathsWithTopics(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TopicsAdded:   difftypes.TopicChanges{{Schema: "app", Name: "was_node"}},
		TopicsRemoved: difftypes.TopicChanges{{Schema: "app", Name: "was_topic"}},
		FeatureChanges: []schemaext.ChangeRecord{
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

func TestCommonSchemeStepRefusesAnEmptyLeafName(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	diff := &difftypes.SchemaDiff{AsyncReplicationsAdded: difftypes.AsyncReplicationChanges{{
		Schema: "app", Spec: ast.AsyncReplicationSpec{
			Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpcs://primary.example.com:2135/?database=/prod", User: "replicator", PasswordSecretPath: "secrets/replicator"},
			Items:      []ast.AsyncReplicationItem{{Source: "accounts", Target: "replica/accounts"}}, ConsistencyLevel: "global", CommitInterval: "PT30S",
		},
	}}}

	nodes, err := ydb.New().GenerateMigrationAST(t.Context(), runtime, diff)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `YDB scheme operation requires an object name`)
	c.Assert(nodes, qt.IsNil)
}
