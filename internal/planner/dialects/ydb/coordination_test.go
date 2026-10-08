package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateMigrationAST_CoordinationNodes_HappyPath pins where coordination
// node statements sit in a plan. A path names one object, so the drops come
// with the topic drops, before any table is created, and the creations and
// changes come with the topic creations, after the tables are dropped: here a
// table takes the path of a dropped node and a node the path of a dropped
// table. Views come last. A change names only the settings that differ.
// TestYDBCoordinationNodes_TradeAPathWithATable applies this order on
// 26.2.1.14 and 25.1.4.7.
func TestGenerateMigrationAST_CoordinationNodes_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "app.gone",
			Table:  schemamodel.Table{StructName: "S", Schema: "app", Name: "gone"},
			Fields: []schemamodel.Field{keyField("id")},
		}},
		TablesRemoved: []string{"old"},
		TopicsAdded:   difftypes.TopicChanges{{Name: "events"}},
		TopicsRemoved: difftypes.TopicChanges{{Name: "queue"}},
		CoordinationNodesAdded: []schemamodel.CoordinationNode{
			{Name: "old", Spec: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2000}},
		},
		CoordinationNodesModified: []difftypes.CoordinationNodeChange{{
			Name: "locks", Changes: ast.CoordinationNodeSpec{ReadConsistencyMode: "strict"},
		}},
		CoordinationNodesRemoved: []schemamodel.CoordinationNode{{Schema: "app", Name: "gone"}},
		ViewsAdded:               difftypes.ViewChanges{{Name: "item_ids", Body: "SELECT id FROM `app/gone`"}},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP TOPIC `queue`;\n"+
		"DROP COORDINATION NODE `app/gone`;\n"+
		"CREATE TABLE `app/gone` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    PRIMARY KEY (`id`)\n"+
		");\n"+
		"DROP TABLE `old`;\n"+
		"CREATE TOPIC `events`;\n"+
		"CREATE COORDINATION NODE `old` WITH (self_check_period = Interval('PT2S'));\n"+
		"ALTER COORDINATION NODE `locks` SET (read_consistency_mode = 'strict');\n"+
		"CREATE VIEW `item_ids` WITH (security_invoker = TRUE) AS\n"+
		"SELECT id FROM `app/gone`\n"+
		";\n")
}

// TestGenerateMigrationAST_CoordinationNodes_FailurePath refuses, before any
// node is returned, a coordination node change the plan cannot make: any of
// them on a target without the key, Ptah's own lock node, and a configuration
// the node would not run with as written, a change judged with the settings
// the node keeps.
func TestGenerateMigrationAST_CoordinationNodes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name:    "a creation without the key",
			caps:    capability.YDB262().With(capability.CoordinationNodes, false),
			diff:    &difftypes.SchemaDiff{CoordinationNodesAdded: []schemamodel.CoordinationNode{{Name: "locks"}}},
			wantErr: `creating coordination node locks, which requires target capability coordination_nodes, unavailable on this ydb target`,
		},
		{
			name: "a change without the key",
			caps: capability.YDB262().With(capability.CoordinationNodes, false),
			diff: &difftypes.SchemaDiff{CoordinationNodesModified: []difftypes.CoordinationNodeChange{{
				Name: "locks", Changes: ast.CoordinationNodeSpec{ReadConsistencyMode: "strict"},
			}}},
			wantErr: `changing coordination node locks, which requires target capability coordination_nodes, .*`,
		},
		{
			name:    "a drop without the key",
			caps:    capability.YDB262().With(capability.CoordinationNodes, false),
			diff:    &difftypes.SchemaDiff{CoordinationNodesRemoved: []schemamodel.CoordinationNode{{Name: "locks"}}},
			wantErr: `dropping coordination node locks, which requires target capability coordination_nodes, .*`,
		},
		{
			name:    "a creation of Ptah's lock node",
			caps:    capability.YDB262(),
			diff:    &difftypes.SchemaDiff{CoordinationNodesAdded: []schemamodel.CoordinationNode{{Name: "ptah_locks"}}},
			wantErr: `coordination node ptah_locks: coordination node ptah_locks at the database root holds Ptah's own locks, .*`,
		},
		{
			name:    "a drop of Ptah's lock node",
			caps:    capability.YDB262(),
			diff:    &difftypes.SchemaDiff{CoordinationNodesRemoved: []schemamodel.CoordinationNode{{Name: "ptah_locks"}}},
			wantErr: `coordination node ptah_locks: coordination node ptah_locks at the database root .*`,
		},
		{
			name: "a change of Ptah's lock node",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{CoordinationNodesModified: []difftypes.CoordinationNodeChange{{
				Name: "ptah_locks", Changes: ast.CoordinationNodeSpec{ReadConsistencyMode: "strict"},
			}}},
			wantErr: `coordination node ptah_locks: coordination node ptah_locks at the database root .*`,
		},
		{
			name: "a creation the node would not run with",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{CoordinationNodesAdded: []schemamodel.CoordinationNode{{
				Name: "locks", Spec: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 100},
			}}},
			wantErr: `coordination node locks: self_check_period PT0.1S: .*`,
		},
		{
			name: "a change that leaves the grace period below the self-check period the node keeps",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{CoordinationNodesModified: []difftypes.CoordinationNodeChange{{
				Name: "locks", Changes: ast.CoordinationNodeSpec{SessionGracePeriodMillis: 5000},
				Previous: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 5000},
			}}},
			wantErr: `coordination node locks: session_grace_period PT5S: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				test.diff,
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
