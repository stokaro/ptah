package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// replicationSpec is a replication of one table of /prod.
var replicationSpec = ast.AsyncReplicationSpec{
	Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod"},
	Items:      []ast.AsyncReplicationItem{{Source: "a", Target: "ra"}},
}

// transferSpec is a transfer of a topic into a table.
var transferSpec = ast.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}

// A replication dropped with CASCADE takes its replica tables with it and a
// dropped transfer its consumer's position, so each is destructive; a
// replication dropped without CASCADE keeps its tables and is a warning,
// alike whether the node or its rendered statement is judged. A change of
// either is a warning, and a created one is safe.
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
	}{
		{name: "a created replication", node: ast.NewCreateAsyncReplication("mirror", replicationSpec),
			severity: safety.Safe, reason: "does not remove data or tighten constraints"},
		{name: "a replication dropped with its tables", node: ast.NewDropAsyncReplication("mirror", true),
			severity: safety.Destructive,
			reason:   "DROP ASYNC REPLICATION ... CASCADE drops the replica tables with the replication"},
		{name: "a replication dropped keeping its tables", node: ast.NewDropAsyncReplication("mirror", false),
			severity: safety.Warning, reason: "DROP ASYNC REPLICATION without CASCADE ends the replication and keeps " +
				"its tables, read-only for good unless it was failed over first"},
		{name: "a changed replication", node: ast.NewAlterAsyncReplication("mirror", moved, replicationSpec),
			severity: safety.Warning,
			reason:   "ALTER ASYNC REPLICATION points the replication at another source or credential"},
		{name: "a created transfer", node: ast.NewCreateTransfer("ingest", transferSpec), severity: safety.Safe,
			reason: "does not remove data or tighten constraints"},
		{name: "a dropped transfer", node: ast.NewDropTransfer("ingest"), severity: safety.Destructive,
			reason: "DROP TRANSFER stops the transfer and drops the topic consumer YDB created for it, with its " +
				"position in the topic"},
		{name: "a changed transfer", node: ast.NewAlterTransfer("ingest", relambda, transferSpec),
			severity: safety.Warning, reason: "ALTER TRANSFER changes the rows the transfer writes from each message"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := safety.AssessRenderedWithCapabilities([]ast.Node{test.node}, platform.YDB, capability.YDB262())
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.HasLen, 1)
			c.Assert(rendered[0].Severity, qt.Equals, test.severity)
			c.Assert(rendered[0].Reason, qt.Equals, test.reason)

			assessed := safety.Assess([]ast.Node{test.node})
			c.Assert(assessed, qt.HasLen, 1)
			c.Assert(assessed[0].Severity, qt.Equals, test.severity)
			c.Assert(assessed[0].Reason, qt.Equals, test.reason)
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
	diff := &difftypes.SchemaDiff{
		AsyncReplicationsAdded:    difftypes.AsyncReplicationChanges{{Name: "fresh"}},
		AsyncReplicationsRemoved:  difftypes.AsyncReplicationChanges{{Name: "gone"}, {Name: "gone2"}},
		AsyncReplicationsModified: []difftypes.AsyncReplicationDiff{{Name: "mirror", ConnectionChanged: true}},
		TransfersAdded:            difftypes.TransferChanges{{Name: "fresh"}},
		TransfersRemoved:          difftypes.TransferChanges{{Name: "gone"}},
		TransfersModified:         []difftypes.TransferDiff{{Name: "ingest", LambdaChanged: true}},
	}

	c.Assert(safety.ClassifySchemaDiff(diff), qt.DeepEquals, []safety.Finding{
		{Category: "async_replications_removed", Count: 2, Severity: safety.Destructive},
		{Category: "transfers_removed", Count: 1, Severity: safety.Destructive},
		{Category: "async_replications_modified", Count: 1, Severity: safety.Warning},
		{Category: "transfers_modified", Count: 1, Severity: safety.Warning},
		{Category: "async_replications_added", Count: 1, Severity: safety.Safe},
		{Category: "transfers_added", Count: 1, Severity: safety.Safe},
	})
}
