package ydbreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbreverse"
)

func replicationReverseRequest(changes ...schemaext.ChangeRecord) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(), Changes: changes}
}

var (
	reverseMirror = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
	}
	reverseMoved = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://standby:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
	}
	reverseSigned = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod", TokenSecretName: "token"},
		Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
	}
	reverseIngest   = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}
	reverseRelambda = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return [<| id: 1 |>]; }", Consumer: "c"}
)

// TestReplicationReversal_HappyPath drops what the change created, creates
// again what it dropped with CASCADE, leaves a failed-over replication
// dropped, and moves a change back in place in the state the forward change
// ran in, keeping a credential it added. Every loss is reported.
func TestReplicationReversal_HappyPath(t *testing.T) {
	mirror := ydbreplication.ReplicationRef("app", "mirror")
	ingest := ydbreplication.TransferRef("app", "ingest")
	tests := []struct {
		name        string
		subject     objectidentity.ID
		change      schemaext.ChangeValue
		want        schemaext.ChangeValue
		kind        schemaext.Kind
		forward     schemaext.Value
		limitations []string
	}{
		{name: "a created replication is dropped", subject: mirror, kind: ydbreplication.ReplicationKind,
			change:      &ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: reverseMirror}},
			want:        &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: reverseMirror}},
			forward:     &ydbreplication.ObservedReplication{Spec: reverseMirror},
			limitations: []string{"dropping async replication app/mirror drops the replica tables it created, with every row it copied"}},
		{name: "a paused replication dropped with its replicas is created again", subject: mirror, kind: ydbreplication.ReplicationKind,
			change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: reverseMirror, State: ydbreplication.StatePaused}},
			want:   &ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: reverseMirror}},
			limitations: []string{
				"the replica tables async replication app/mirror dropped are created again empty, and the replication copies its source again from the start",
				"async replication app/mirror was paused when it was dropped, and runs once it is created again",
			}},
		{name: "a failed-over replication stays dropped", subject: mirror, kind: ydbreplication.ReplicationKind,
			change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: reverseMirror, State: ydbreplication.StateDone}},
			limitations: []string{"async replication app/mirror was failed over before it was dropped, so it is not created again: " +
				"it replicated nothing more, and its tables stayed as ordinary tables"}},
		{name: "a moved replication moves back while paused", subject: mirror, kind: ydbreplication.ReplicationKind,
			change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: reverseMirror, State: ydbreplication.StatePaused},
				After: &ydbreplication.DesiredReplication{Spec: reverseMoved}},
			want: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: reverseMoved, State: ydbreplication.StatePaused},
				After: &ydbreplication.DesiredReplication{Spec: reverseMirror}},
			forward: &ydbreplication.ObservedReplication{Spec: reverseMoved, State: ydbreplication.StatePaused}},
		{name: "an added credential stays", subject: mirror, kind: ydbreplication.ReplicationKind,
			change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: reverseMirror, State: ydbreplication.StatePaused},
				After: &ydbreplication.DesiredReplication{Spec: reverseSigned}},
			forward:     &ydbreplication.ObservedReplication{Spec: reverseSigned, State: ydbreplication.StatePaused},
			limitations: []string{"the credential the change gave async replication app/mirror stays, since YDB has no statement that takes a credential away"}},
		{name: "a created transfer is dropped", subject: ingest, kind: ydbreplication.TransferKind,
			change:  &ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{Spec: reverseIngest}},
			want:    &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: reverseIngest}},
			forward: &ydbreplication.ObservedTransfer{Spec: reverseIngest},
			limitations: []string{"the rows transfer app/ingest wrote into table t stay",
				"dropping transfer app/ingest drops the consumer YDB created for it, with its position in topic tp"}},
		{name: "a created transfer with its own consumer is dropped", subject: ingest, kind: ydbreplication.TransferKind,
			change:      &ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{Spec: reverseRelambda}},
			want:        &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: reverseRelambda}},
			forward:     &ydbreplication.ObservedTransfer{Spec: reverseRelambda},
			limitations: []string{"the rows transfer app/ingest wrote into table t stay"}},
		{name: "a dropped transfer is created again", subject: ingest, kind: ydbreplication.TransferKind,
			change: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: reverseIngest}},
			want:   &ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{Spec: reverseIngest}},
			limitations: []string{"dropping transfer app/ingest removed the consumer YDB created for it, if it had one, with its position " +
				"in topic tp; the transfer created again does not resume from it"}},
		{name: "a new lambda goes back", subject: ingest, kind: ydbreplication.TransferKind,
			change: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: reverseIngest, State: ydbreplication.StateRunning},
				After: &ydbreplication.DesiredTransfer{Spec: reverseRelambda}},
			want: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: reverseRelambda, State: ydbreplication.StateRunning},
				After: &ydbreplication.DesiredTransfer{Spec: reverseIngest}},
			forward:     &ydbreplication.ObservedTransfer{Spec: reverseRelambda, State: ydbreplication.StateRunning},
			limitations: []string{"the rows transfer app/ingest wrote with the changed lambda stay as written"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := replicationReverseRequest(schemaext.ChangeRecord{Subject: test.subject, Value: test.change})
			services := map[schemaext.Kind]schemaext.ReversalService{
				ydbreplication.ReplicationKind: ydbreverse.AsyncReplicationService{}, ydbreplication.TransferKind: ydbreverse.TransferService{},
			}
			result, err := services[test.kind].ReverseChanges(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change, qt.DeepEquals, schemaext.ChangeRecord{Subject: test.subject, Value: test.want})
			c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: test.kind, Value: test.forward}})
			c.Assert(result[0].Limitations, qt.DeepEquals, test.limitations)
		})
	}
}

// TestReplicationReversal_FailurePath refuses a target without the key, a
// change of the other kind and an empty change, with no partial result.
func TestReplicationReversal_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReversalRequest
	}{
		{name: "a line without async replication", request: schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
			Capabilities: capability.YDB262().With(capability.AsyncReplication, false),
			Changes: []schemaext.ChangeRecord{{Subject: ydbreplication.ReplicationRef("", "mirror"),
				Value: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: reverseMirror}}}}}},
		{name: "a transfer change", request: replicationReverseRequest(schemaext.ChangeRecord{Subject: ydbreplication.ReplicationRef("", "mirror"),
			Value: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: reverseIngest}}})},
		{name: "a transfer's identity", request: replicationReverseRequest(schemaext.ChangeRecord{Subject: ydbreplication.TransferRef("", "mirror"),
			Value: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: reverseMirror}}})},
		{name: "an empty change", request: replicationReverseRequest(schemaext.ChangeRecord{Subject: ydbreplication.ReplicationRef("", "mirror"),
			Value: &ydbdiff.AsyncReplication{}})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreverse.AsyncReplicationService{}).ReverseChanges(t.Context(), test.request)
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.IsNil)
		})
	}
}
