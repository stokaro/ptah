package ydbcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/engine"
)

func replicationRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbreplication.Codecs(), ydbdiff.AsyncReplicationCodec(), ydbdiff.TransferCodec()),
		Comparisons: []engine.ObjectComparison{
			{Target: "ydb", Kinds: []schemaext.Kind{ydbreplication.ReplicationKind},
				ChangeKinds: []schemaext.Kind{ydbdiff.AsyncReplicationKind}, Service: ydbcompare.AsyncReplicationService{}},
			{Target: "ydb", Kinds: []schemaext.Kind{ydbreplication.TransferKind},
				ChangeKinds: []schemaext.Kind{ydbdiff.TransferKind}, Service: ydbcompare.TransferService{}},
		},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

// replicationState is a source of one kind with value at app/mirror, or none.
func replicationState(c *qt.C, kind schemaext.Kind, direction schemaext.Representation, knowledge schemaext.KnowledgeState,
	value schemaext.Value, limits ...schemaext.SubjectCoverage,
) schemaext.ObjectState {
	c.Helper()
	enroll := map[schemaext.Kind]func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error){
		ydbreplication.ReplicationKind: ydbreplication.ReplicationCoverage, ydbreplication.TransferKind: ydbreplication.TransferCoverage,
	}[kind]
	coverage, err := enroll(direction, schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}, limits)
	c.Assert(err, qt.IsNil)
	state := schemaext.ObjectState{Coverage: coverage}
	if value != nil {
		ref := ydbreplication.ReplicationRef("app", "mirror")
		if kind == ydbreplication.TransferKind {
			ref = ydbreplication.TransferRef("app", "mirror")
		}
		state.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: value}))
	}
	return state
}

var (
	declaredMirror = &ydbreplication.DesiredReplication{Spec: ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "dir", Target: "replica"}},
	}}
	// heldMirror is declaredMirror as YDB reads it back: the directory item as
	// one item per table, and the consistency level written out.
	heldMirror = &ydbreplication.ObservedReplication{Spec: ydbreplication.ReplicationSpec{
		Connection:       ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:            []ydbreplication.Item{{Source: "dir/t1", Target: "replica/t1"}, {Source: "dir/sub/t2", Target: "replica/sub/t2"}},
		ConsistencyLevel: ydbreplication.ConsistencyRow,
	}, State: ydbreplication.StateRunning}
	movedMirror = &ydbreplication.DesiredReplication{Spec: ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://standby:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "dir", Target: "replica"}},
	}}
	declaredIngest = &ydbreplication.DesiredTransfer{Spec: ydbreplication.TransferSpec{Source: "tp", Target: "t",
		Lambda: "($m) -> { return []; }"}}
	// heldIngest is declaredIngest as YDB reads it back, with the consumer YDB
	// created for it.
	heldIngest = &ydbreplication.ObservedTransfer{Spec: ydbreplication.TransferSpec{Source: "tp", Target: "t",
		Lambda: "($m) -> { return []; }", Consumer: "generated"}, State: ydbreplication.StateRunning}
)

// TestReplicationComparison_ComparesAsYDBKeepsItAndRespectsEvidence compares
// each kind as YDB keeps it, so a directory item read back as its tables and
// a consumer YDB created read back are no change. A creation is planned only
// where the read established absence, a removal only where the source claims
// the namespace, and an object the source makes no claim about is kept.
func TestReplicationComparison_ComparesAsYDBKeepsItAndRespectsEvidence(t *testing.T) {
	mirror := ydbreplication.ReplicationRef("app", "mirror")
	ingest := ydbreplication.TransferRef("app", "mirror")
	limit := []schemaext.SubjectCoverage{{Kind: ydbreplication.ReplicationKind, Subject: mirror,
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the replication API is not served"}}}
	tests := []struct {
		name                               string
		kind                               schemaext.Kind
		desired, current                   schemaext.Value
		desiredKnowledge, currentKnowledge schemaext.KnowledgeState
		currentLimits                      []schemaext.SubjectCoverage
		want                               []schemaext.ChangeRecord
		undecided, objects                 int
	}{
		{name: "a created replication", kind: ydbreplication.ReplicationKind, desired: declaredMirror,
			desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1,
			want: []schemaext.ChangeRecord{{Subject: mirror, Value: &ydbdiff.AsyncReplication{After: declaredMirror}}}},
		{name: "a dropped replication", kind: ydbreplication.ReplicationKind, current: heldMirror,
			desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete,
			want: []schemaext.ChangeRecord{{Subject: mirror, Value: &ydbdiff.AsyncReplication{Before: heldMirror}}}},
		{name: "a directory item read back as its tables", kind: ydbreplication.ReplicationKind, desired: declaredMirror,
			current: heldMirror, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "a moved replication", kind: ydbreplication.ReplicationKind, desired: movedMirror, current: heldMirror,
			desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1,
			want: []schemaext.ChangeRecord{{Subject: mirror, Value: &ydbdiff.AsyncReplication{Before: heldMirror, After: movedMirror}}}},
		{name: "a source without a claim keeps the held replication", kind: ydbreplication.ReplicationKind, current: heldMirror,
			desiredKnowledge: schemaext.Uninspected, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "an unread namespace withholds the creation", kind: ydbreplication.ReplicationKind, desired: declaredMirror,
			desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected, objects: 1, undecided: 2},
		{name: "an unread replication withholds its removal", kind: ydbreplication.ReplicationKind,
			desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, currentLimits: limit, undecided: 1},
		{name: "a consumer YDB created", kind: ydbreplication.TransferKind, desired: declaredIngest, current: heldIngest,
			desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "a dropped transfer", kind: ydbreplication.TransferKind, current: heldIngest,
			desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete,
			want: []schemaext.ChangeRecord{{Subject: ingest, Value: &ydbdiff.Transfer{Before: heldIngest}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
				Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{test.kind},
				Desired: replicationState(c, test.kind, schemaext.Desired, test.desiredKnowledge, test.desired),
				Current: replicationState(c, test.kind, schemaext.Observed, test.currentKnowledge, test.current, test.currentLimits...)}
			result, err := replicationRuntime(c).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.DeepEquals, test.want)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, test.objects)
		})
	}
}

// TestReplicationComparison_RefusesATargetWithoutTheKey refuses a comparison
// that would plan a statement on a line without the kind's key, and leaves one
// that plans none alone.
func TestReplicationComparison_RefusesATargetWithoutTheKey(t *testing.T) {
	tests := []struct {
		name    string
		kind    schemaext.Kind
		caps    capability.Capabilities
		desired schemaext.Value
		wantErr string
	}{
		{name: "a replication", kind: ydbreplication.ReplicationKind, caps: capability.YDB262().With(capability.AsyncReplication, false),
			desired: declaredMirror,
			wantErr: `async replication app.mirror, which requires target capability async_replication, unavailable on this ydb target`},
		{name: "a transfer", kind: ydbreplication.TransferKind, caps: capability.YDB251(), desired: declaredIngest,
			wantErr: `transfer app.mirror, which requires target capability transfers, unavailable on this ydb target`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
				Capabilities: test.caps, Kinds: []schemaext.Kind{test.kind},
				Desired: replicationState(c, test.kind, schemaext.Desired, schemaext.Complete, test.desired),
				Current: replicationState(c, test.kind, schemaext.Observed, schemaext.Complete, nil)}
			result, err := replicationRuntime(c).CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestReplicationComparison_AcceptsATargetWithoutTheKeyWhenNothingChanges is
// the control: with nothing to plan, a line without the key compares.
func TestReplicationComparison_AcceptsATargetWithoutTheKeyWhenNothingChanges(t *testing.T) {
	c := qt.New(t)
	request := schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.YDB251(), Kinds: []schemaext.Kind{ydbreplication.TransferKind},
		Desired: replicationState(c, ydbreplication.TransferKind, schemaext.Desired, schemaext.Complete, nil),
		Current: replicationState(c, ydbreplication.TransferKind, schemaext.Observed, schemaext.Complete, nil)}
	result, err := replicationRuntime(c).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
}
