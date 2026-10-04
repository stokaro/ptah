package generator

// White-box testing required: the down direction is built by reversing a diff
// through unexported helpers, and the reversal is what this pins -- the public
// API only exposes the SQL that comes out of the whole pipeline.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateDownMigration_CoordinationNodes pins the rollback of each
// coordination node change: a created node is dropped, a dropped node is
// created with the configuration the database held, and a changed node gets
// back the settings the change moved, at the values it ran with before,
// including a setting it ran with at YDB's default.
func TestGenerateDownMigration_CoordinationNodes(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "rolling back a creation drops the node",
			diff: &difftypes.SchemaDiff{CoordinationNodesAdded: []schemamodel.CoordinationNode{
				{Schema: "app", Name: "locks", Spec: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2000}},
			}},
			want: "DROP COORDINATION NODE `app/locks`;",
		},
		{
			name: "rolling back a drop creates the node the database held",
			diff: &difftypes.SchemaDiff{CoordinationNodesRemoved: []schemamodel.CoordinationNode{
				{Schema: "app", Name: "locks", Spec: ast.CoordinationNodeSpec{
					SelfCheckPeriodMillis: 2500, ReadConsistencyMode: "strict",
				}},
			}},
			want: "CREATE COORDINATION NODE `app/locks` WITH (self_check_period = Interval('PT2.5S'), " +
				"read_consistency_mode = 'strict');",
		},
		{
			name: "rolling back a change puts the settings back",
			diff: &difftypes.SchemaDiff{CoordinationNodesModified: []difftypes.CoordinationNodeChange{{
				Schema: "app", Name: "locks",
				Changes:  ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 3000, ReadConsistencyMode: "strict"},
				Previous: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2000},
			}}},
			want: "ALTER COORDINATION NODE `app/locks` SET (self_check_period = Interval('PT2S'), " +
				"read_consistency_mode = 'relaxed');",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := generateDownMigrationSQL(test.diff, &schemamodel.Database{}, &catalog.Database{},
				platform.YDB, capability.YDB262())

			c.Assert(err, qt.IsNil)
			c.Assert(legacyRenderedSQL(sql), qt.Contains, test.want)
		})
	}
}
