package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback drops the replication the forward plan created, with CASCADE,
// since the forward plan left it running; creates again the running one it
// dropped from the specification the removal carried; and moves a paused
// replication's connection and a transfer's lambda back. Each step says what
// it cannot recover. The forward diff is left as it was.
func TestPlanBidirectionalSchemaDiff_ReplicationsRollBack(t *testing.T) {
	c := qt.New(t)
	replicationOf := func(source, target string) ydbreplication.ReplicationSpec {
		return ydbreplication.ReplicationSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
			Items:      []ydbreplication.Item{{Source: source, Target: target}},
		}
	}
	moved := replicationOf("p", "rp")
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	before := ydbreplication.TransferSpec{Source: "events", Target: "log", Lambda: "($m) -> { return []; }"}
	after := before
	after.Lambda = "($m) -> { return [<| a: 1 |>]; }"
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{
			{Subject: ydbreplication.ReplicationRef("", "mirror"), Value: ydbdiff.NewAsyncReplication(nil,
				&ydbreplication.DesiredReplication{Spec: replicationOf("a", "ra")})},
			{Subject: ydbreplication.ReplicationRef("", "old"), Value: ydbdiff.NewAsyncReplication(
				&ydbreplication.ObservedReplication{Spec: replicationOf("o", "ro"), State: ydbreplication.StateRunning}, nil)},
			{Subject: ydbreplication.ReplicationRef("", "paused"), Value: ydbdiff.NewAsyncReplication(
				&ydbreplication.ObservedReplication{Spec: replicationOf("p", "rp"), State: ydbreplication.StatePaused},
				&ydbreplication.DesiredReplication{Spec: moved})},
			{Subject: ydbreplication.TransferRef("", "ingest"), Value: ydbdiff.NewTransfer(
				&ydbreplication.ObservedTransfer{Spec: before, State: ydbreplication.StateRunning},
				&ydbreplication.DesiredTransfer{Spec: after})},
		},
		DeclaredTables: []schemamodel.Table{{StructName: "L", Name: "log"}},
		Features: difftypes.FeatureContext{
			CurrentObjects: must.Must(schemaext.NewObjects(
				ydbreplication.ObservedReplicationObject("", "old", replicationOf("o", "ro"), ydbreplication.StateRunning),
				ydbreplication.ObservedReplicationObject("", "paused", replicationOf("p", "rp"), ydbreplication.StatePaused),
				ydbreplication.ObservedTransferObject("", "ingest", before, ydbreplication.StateRunning),
				ydbtopic.ObservedObject("", "events", ydbtopic.Spec{}),
			)),
			DesiredObjects: must.Must(schemaext.NewObjects(
				ydbreplication.DesiredReplicationObject("", "mirror", "", replicationOf("a", "ra")),
				ydbreplication.DesiredReplicationObject("", "paused", "", moved),
				ydbreplication.DesiredTransferObject("", "ingest", "", after),
				ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{}),
			)),
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
	c.Assert(sql, qt.Equals, "-- Rollback of \"ptah.run/ydb/async-replication mirror\": \"drop the created replication with its replica tables\".\n"+
		"-- Recovery limit: \"dropping async replication mirror drops the replica tables it created, with every row it copied\"\n"+
		"-- Rollback of \"ptah.run/ydb/async-replication old\": \"create the dropped replication again with its connection and items\".\n"+
		"-- Recovery limit: \"the replica tables async replication old dropped are created again empty, and the replication copies its source again from the start\"\n"+
		"-- Rollback of \"ptah.run/ydb/async-replication paused\": \"restore the replication's connection and credential in place\".\n"+
		"-- Rollback of \"ptah.run/ydb/transfer ingest\": \"restore the transfer's lambda, batch settings and connection in place\".\n"+
		"-- Recovery limit: \"the rows transfer ingest wrote with the changed lambda stay as written\"\n"+
		"DROP ASYNC REPLICATION `mirror` CASCADE;\n"+
		"CREATE ASYNC REPLICATION `old` FOR `o` AS `ro` WITH (CONNECTION_STRING = 'grpc://primary:2136/?database=/prod');\n"+
		"ALTER ASYNC REPLICATION `paused` SET (CONNECTION_STRING = 'grpc://primary:2136/?database=/prod');\n"+
		"ALTER TRANSFER `ingest` SET USING ($m) -> { return []; };\n")
	c.Assert(diff.FeatureChanges[2].Value.(*ydbdiff.AsyncReplication).After.Spec.Connection.ConnectionString, qt.Equals,
		"grpc://standby:2136/?database=/prod", qt.Commentf("the reversal must not write through to the forward diff"))
}
