package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// replicationSpec is a replication of one table of /prod.
var replicationSpec = ydbreplication.ReplicationSpec{
	Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
	Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
}

// transferSpec is a transfer of a topic into a table.
var transferSpec = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}

// keptTablesReason and droppedTransferReason are the reasons a drop that
// keeps the replica tables and a transfer's drop are judged with.
const (
	keptTablesReason = "DROP ASYNC REPLICATION without CASCADE ends the replication and keeps " +
		"its tables, read-only for good unless it was failed over first"
	droppedTransferReason = "DROP TRANSFER stops the transfer and drops the topic consumer YDB created for it, with its " +
		"position in the topic"
)

// replicationStatement is the statement of change to replication mirror.
func replicationStatement(before *ydbreplication.ObservedReplication, after *ydbreplication.DesiredReplication) ast.Node {
	return &ast.ExtensionStatement{Payload: &ydbast.AsyncReplication{Name: "mirror",
		Change: *ydbdiff.NewAsyncReplication(before, after)}}
}

// transferStatement is the statement of change to transfer ingest.
func transferStatement(before *ydbreplication.ObservedTransfer, after *ydbreplication.DesiredTransfer) ast.Node {
	return &ast.ExtensionStatement{Payload: &ydbast.Transfer{Name: "ingest", Change: *ydbdiff.NewTransfer(before, after)}}
}

// A replication dropped with CASCADE takes its replica tables with it and a
// dropped transfer its consumer's position, so each is destructive; a
// replication dropped without CASCADE keeps its tables and is a warning,
// alike whether the node or its rendered statement is judged. A change of
// either is a warning, and a created one is safe; the node names what a
// creation adds, and its rendered statement is judged as any other creation.
func TestAssessRendered_Replications(t *testing.T) {
	moved := replicationSpec.Clone()
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	relambda := transferSpec
	relambda.Lambda = "($m) -> { return [<| a: 1 |>]; }"
	tests := []struct {
		name     string
		node     ast.Node
		severity safety.Severity
		reason   string
		// nodeReason is the reason the node is judged with.
		nodeReason string
	}{
		{name: "a created replication", node: replicationStatement(nil, &ydbreplication.DesiredReplication{Spec: replicationSpec}),
			severity: safety.Safe, reason: "does not remove data or tighten constraints",
			nodeReason: "CREATE ASYNC REPLICATION adds a replication and its replica tables"},
		{name: "a replication dropped with its tables", node: replicationStatement(
			&ydbreplication.ObservedReplication{Spec: replicationSpec, State: ydbreplication.StateRunning}, nil),
			severity:   safety.Destructive,
			reason:     "DROP ASYNC REPLICATION ... CASCADE drops the replica tables with the replication",
			nodeReason: "DROP ASYNC REPLICATION ... CASCADE drops the replica tables with the replication"},
		{name: "a replication dropped keeping its tables", node: replicationStatement(
			&ydbreplication.ObservedReplication{Spec: replicationSpec, State: ydbreplication.StateDone}, nil),
			severity: safety.Warning, reason: keptTablesReason, nodeReason: keptTablesReason},
		{name: "a changed replication", node: replicationStatement(
			&ydbreplication.ObservedReplication{Spec: replicationSpec, State: ydbreplication.StatePaused},
			&ydbreplication.DesiredReplication{Spec: moved}),
			severity:   safety.Warning,
			reason:     "ALTER ASYNC REPLICATION points the replication at another source or credential",
			nodeReason: "ALTER ASYNC REPLICATION points the replication at another source or credential"},
		{name: "a created transfer", node: transferStatement(nil, &ydbreplication.DesiredTransfer{Spec: transferSpec}), severity: safety.Safe,
			reason: "does not remove data or tighten constraints", nodeReason: "CREATE TRANSFER adds a transfer"},
		{name: "a dropped transfer", node: transferStatement(
			&ydbreplication.ObservedTransfer{Spec: transferSpec, State: ydbreplication.StateRunning}, nil), severity: safety.Destructive,
			reason: droppedTransferReason, nodeReason: droppedTransferReason},
		{name: "a changed transfer", node: transferStatement(
			&ydbreplication.ObservedTransfer{Spec: transferSpec, State: ydbreplication.StateRunning},
			&ydbreplication.DesiredTransfer{Spec: relambda}),
			severity: safety.Warning, reason: "ALTER TRANSFER changes the rows the transfer writes from each message",
			nodeReason: "ALTER TRANSFER changes the rows the transfer writes from each message"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := safety.AssessRenderedWithCapabilities(c.Context(), must.Must(builtin.New()), []ast.Node{test.node}, platform.YDB, capability.YDB262())
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.HasLen, 1)
			c.Assert(rendered[0].Severity, qt.Equals, test.severity)
			c.Assert(rendered[0].Reason, qt.Equals, test.reason)

			assessed := safety.Assess([]ast.Node{test.node})
			c.Assert(assessed, qt.HasLen, 1)
			c.Assert(assessed[0].Severity, qt.Equals, test.severity)
			c.Assert(assessed[0].Reason, qt.Equals, test.nodeReason)
		})
	}
}

// A statement read as text is judged the same way, so a hand-written
// migration is held by the same gate.
func TestAssessSQL_Replications(t *testing.T) {
	tests := []struct {
		statement string
		severity  safety.Severity
	}{
		{statement: "DROP ASYNC REPLICATION `mirror` CASCADE;", severity: safety.Destructive},
		{statement: "drop async replication mirror cascade", severity: safety.Destructive},
		{statement: "DROP ASYNC REPLICATION `mirror`;", severity: safety.Warning},
		{statement: "DROP TRANSFER `ingest`;", severity: safety.Destructive},
		{statement: "CREATE TRANSFER ingest FROM tp TO t USING ($m) -> { return []; };", severity: safety.Safe},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(safety.AssessSQL(test.statement).Severity, qt.Equals, test.severity)
		})
	}
}

func TestClassifySchemaDiff_Replications(t *testing.T) {
	c := qt.New(t)
	running := &ydbreplication.ObservedReplication{Spec: replicationSpec, State: ydbreplication.StateRunning}
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		{Subject: ydbreplication.ReplicationRef("", "gone"), Value: ydbdiff.NewAsyncReplication(running, nil)},
		{Subject: ydbreplication.ReplicationRef("", "fresh"),
			Value: ydbdiff.NewAsyncReplication(nil, &ydbreplication.DesiredReplication{Spec: replicationSpec})},
		{Subject: ydbreplication.TransferRef("", "fresh"),
			Value: ydbdiff.NewTransfer(nil, &ydbreplication.DesiredTransfer{Spec: transferSpec})},
	}}

	c.Assert(safety.ClassifySchemaDiff(diff), qt.DeepEquals, []safety.Finding{
		{Category: "feature_changes:" + string(ydbdiff.AsyncReplicationKind), Count: 2, Severity: safety.Destructive},
		{Category: "feature_changes:" + string(ydbdiff.TransferKind), Count: 1, Severity: safety.Safe},
	})
}
