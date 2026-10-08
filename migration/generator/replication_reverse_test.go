package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback drops the replication the forward plan created, with CASCADE,
// since the forward plan left it running; creates again the one it dropped
// from the specification the removal carried; and moves a paused replication's
// connection and a transfer's lambda back. The forward diff is left as it was.
func TestPlanBidirectionalSchemaDiff_ReplicationsRollBack(t *testing.T) {
	c := qt.New(t)
	replicationOf := func(source, target string) ast.AsyncReplicationSpec {
		return ast.AsyncReplicationSpec{
			Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod"},
			Items:      []ast.AsyncReplicationItem{{Source: source, Target: target}},
		}
	}
	moved := replicationOf("p", "rp")
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	before := ast.TransferSpec{Source: "events", Target: "log", Lambda: "($m) -> { return []; }"}
	after := before
	after.Lambda = "($m) -> { return [<| a: 1 |>]; }"
	diff := &difftypes.SchemaDiff{
		AsyncReplicationsAdded:   difftypes.AsyncReplicationChanges{{Name: "mirror", Spec: replicationOf("a", "ra")}},
		AsyncReplicationsRemoved: difftypes.AsyncReplicationChanges{{Name: "old", Spec: replicationOf("o", "ro")}},
		AsyncReplicationsModified: []difftypes.AsyncReplicationDiff{{Name: "paused", ConnectionChanged: true,
			Desired: moved, Current: replicationOf("p", "rp"), State: catalog.ReplicationPaused}},
		TransfersModified: []difftypes.TransferDiff{{Name: "ingest", LambdaChanged: true, Desired: after,
			Current: before, State: catalog.ReplicationRunning}},
		DeclaredTables: []schemamodel.Table{{StructName: "L", Name: "log"}},
		Replications: difftypes.ReplicationContext{
			CurrentReplications: []catalog.AsyncReplication{
				{Name: "old", State: catalog.ReplicationDone, Spec: replicationOf("o", "ro")},
				{Name: "paused", State: catalog.ReplicationPaused, Spec: replicationOf("p", "rp")},
			},
			CurrentTransfers: []catalog.Transfer{{Name: "ingest", State: catalog.ReplicationRunning, Spec: before}},
			DeclaredReplications: []schemamodel.AsyncReplication{
				{Name: "mirror", Spec: replicationOf("a", "ra")},
				{Name: "paused", Spec: moved},
			},
			DeclaredTransfers: []schemamodel.Transfer{{Name: "ingest", Spec: after}},
			CurrentTopics:     []string{"events"},
			DeclaredTopics:    []string{"events"},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "log", Type: "TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
	}}}}

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff,
			DesiredSchema: &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "L", Name: "log"}}},
			CurrentSchema: current,
			Dialect:       platform.YDB,
			Capabilities:  capability.YDB262(),
		})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "DROP ASYNC REPLICATION `mirror` CASCADE;\n"+
		"CREATE ASYNC REPLICATION `old` FOR `o` AS `ro` WITH (CONNECTION_STRING = 'grpc://primary:2136/?database=/prod');\n"+
		"ALTER ASYNC REPLICATION `paused` SET (CONNECTION_STRING = 'grpc://primary:2136/?database=/prod');\n"+
		"ALTER TRANSFER `ingest` SET USING ($m) -> { return []; };\n")
	c.Assert(diff.AsyncReplicationsAdded.Names(), qt.DeepEquals, []string{"mirror"})
	c.Assert(diff.AsyncReplicationsModified[0].Desired.Connection.ConnectionString, qt.Equals,
		"grpc://standby:2136/?database=/prod", qt.Commentf("the reversal must not write through to the forward diff"))
}
