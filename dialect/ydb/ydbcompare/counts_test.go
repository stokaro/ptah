package ydbcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
)

// enrollment records the coverage of one standalone kind.
type enrollment func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error)

// countedState is objects in representation, from a source whose knowledge of
// the namespace is knowledge, with limits on single objects.
func countedState(enroll enrollment, representation schemaext.Representation, knowledge schemaext.KnowledgeState,
	objects []schemaext.Object, limits ...schemaext.SubjectCoverage,
) schemaext.ObjectState {
	return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(enroll(representation, schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}, limits))}
}

// unread is a limit saying that a side could not establish ref.
func unread(kind schemaext.Kind, ref objectidentity.ID) schemaext.SubjectCoverage {
	return schemaext.SubjectCoverage{Kind: kind, Subject: ref,
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unreadable setting"}}
}

// countCase is a batch of one kind, the changes and undecided objects its
// owner reports for it, and the count it states.
type countCase struct {
	name               string
	desired, current   schemaext.ObjectState
	changes, undecided int
	want               schemaext.ObjectCount
}

// TestCoordinationComparison_CountsWhatItCompares pins the counts the
// inventory reports for coordination nodes: a node both sides hold is matched
// whether or not it differs, a creation is added and a drop removed, and a
// node the comparison keeps or cannot decide is counted on neither side.
func TestCoordinationComparison_CountsWhatItCompares(t *testing.T) {
	kind := ydbdiff.CoordinationNodeKind
	strict := ydbcoordination.Spec{ReadConsistencyMode: "strict"}
	declared := func(name string, spec ydbcoordination.Spec) schemaext.Object {
		return ydbcoordination.DesiredObject("app", name, "", spec)
	}
	held := func(name string, spec ydbcoordination.Spec) schemaext.Object {
		return ydbcoordination.ObservedObject("app", name, spec)
	}
	state := func(representation schemaext.Representation, knowledge schemaext.KnowledgeState, objects []schemaext.Object,
		limits ...schemaext.SubjectCoverage,
	) schemaext.ObjectState {
		return countedState(ydbcoordination.Coverage, representation, knowledge, objects, limits...)
	}
	tests := []countCase{
		{name: "a node that agrees", want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("locks", ydbcoordination.Spec{})}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("locks", ydbcoordination.Spec{})})},
		{name: "a node that differs", changes: 1, want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("locks", ydbcoordination.Spec{})}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("locks", strict)})},
		{name: "a creation", changes: 1, want: schemaext.ObjectCount{Kind: kind, Added: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("locks", ydbcoordination.Spec{})}),
			current: state(schemaext.Observed, schemaext.Complete, nil)},
		{name: "a drop", changes: 1, want: schemaext.ObjectCount{Kind: kind, Removed: 1},
			desired: state(schemaext.Desired, schemaext.Complete, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("locks", strict)})},
		{name: "nodes that agree, differ, are created and are dropped", changes: 3, want: schemaext.ObjectCount{Kind: kind, Matched: 2, Added: 1, Removed: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{
				declared("same", ydbcoordination.Spec{}), declared("changed", ydbcoordination.Spec{}), declared("created", ydbcoordination.Spec{})}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{
				held("same", ydbcoordination.Spec{}), held("changed", strict), held("dropped", strict)})},
		{name: "a node a source without a claim keeps", want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Uninspected, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("locks", strict)})},
		{name: "a node the read could not establish", undecided: 1, want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("locks", ydbcoordination.Spec{})}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("locks", strict)},
				unread(ydbcoordination.Kind, ydbcoordination.Ref("app", "locks")))},
		{name: "a declaration the source cannot describe", undecided: 1, want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("locks", ydbcoordination.Spec{})},
				unread(ydbcoordination.Kind, ydbcoordination.Ref("app", "locks"))),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("locks", strict)})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := coordinationRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb",
				Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Of(map[capability.Capability]bool{capability.CoordinationNodes: true}),
				Kinds: []schemaext.Kind{ydbcoordination.Kind}, Desired: test.desired, Current: test.current})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}

// TestStreamingComparison_CountsWhatItCompares pins the counts the inventory
// reports for streaming queries, as coordination nodes are counted.
func TestStreamingComparison_CountsWhatItCompares(t *testing.T) {
	kind := ydbdiff.StreamingQueryKind
	running := ydbstreaming.Spec{Text: "SELECT 1;"}
	stopped := ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}
	declared := func(name string, spec ydbstreaming.Spec) schemaext.Object {
		return ydbstreaming.DesiredObject("app", name, "", spec, false)
	}
	held := func(name string, spec ydbstreaming.Spec) schemaext.Object {
		return ydbstreaming.ObservedObject("app", name, spec)
	}
	state := func(representation schemaext.Representation, knowledge schemaext.KnowledgeState, objects []schemaext.Object,
		limits ...schemaext.SubjectCoverage,
	) schemaext.ObjectState {
		return countedState(ydbstreaming.Coverage, representation, knowledge, objects, limits...)
	}
	tests := []countCase{
		{name: "a query that agrees", want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("query", running)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("query", running)})},
		{name: "a query that differs", changes: 1, want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("query", running)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("query", stopped)})},
		{name: "a creation", changes: 1, want: schemaext.ObjectCount{Kind: kind, Added: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("query", running)}),
			current: state(schemaext.Observed, schemaext.Complete, nil)},
		{name: "a drop", changes: 1, want: schemaext.ObjectCount{Kind: kind, Removed: 1},
			desired: state(schemaext.Desired, schemaext.Complete, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("query", running)})},
		{name: "queries that agree, differ, are created and are dropped", changes: 3, want: schemaext.ObjectCount{Kind: kind, Matched: 2, Added: 1, Removed: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{
				declared("same", running), declared("changed", running), declared("created", running)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{
				held("same", running), held("changed", stopped), held("dropped", running)})},
		{name: "a query a source without a claim keeps", want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Uninspected, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("query", running)})},
		{name: "a query the read could not establish", undecided: 1, want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("query", running)}),
			current: state(schemaext.Observed, schemaext.Complete, nil, unread(ydbstreaming.Kind, ydbstreaming.Ref("app", "query")))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := streamingRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb",
				Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Of(map[capability.Capability]bool{capability.StreamingQueries: true}),
				Kinds: []schemaext.Kind{ydbstreaming.Kind}, Desired: test.desired, Current: test.current})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}

// TestSecretComparison_CountsWhatItCompares pins the counts the inventory
// reports for secrets: a secret both sides hold is matched whether or not the
// caller rotates it, a creation is added and a drop removed, and a secret the
// comparison keeps or cannot decide is counted on neither side.
func TestSecretComparison_CountsWhatItCompares(t *testing.T) {
	kind := ydbdiff.SecretKind
	declared := func(name string) schemaext.Object { return ydbsecret.DesiredObject("ext", name, "", "PTAH_SECRET_PW") }
	held := func(name string) schemaext.Object { return ydbsecret.ObservedObject("ext", name) }
	state := func(representation schemaext.Representation, knowledge schemaext.KnowledgeState, objects []schemaext.Object,
		limits ...schemaext.SubjectCoverage,
	) schemaext.ObjectState {
		return countedState(ydbsecret.Coverage, representation, knowledge, objects, limits...)
	}
	tests := []struct {
		name               string
		desired, current   schemaext.ObjectState
		rotate             []string
		changes, undecided int
		want               schemaext.ObjectCount
	}{
		{name: "a secret both sides hold", want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("pw")}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("pw")})},
		{name: "a rotated secret", rotate: []string{"ext/pw"}, changes: 1, want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("pw")}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("pw")})},
		{name: "a creation", changes: 1, want: schemaext.ObjectCount{Kind: kind, Added: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("pw")}),
			current: state(schemaext.Observed, schemaext.Complete, nil)},
		{name: "a drop", changes: 1, want: schemaext.ObjectCount{Kind: kind, Removed: 1},
			desired: state(schemaext.Desired, schemaext.Complete, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("pw")})},
		{name: "secrets kept, rotated, created and dropped", rotate: []string{"ext/rotated"}, changes: 3,
			want:    schemaext.ObjectCount{Kind: kind, Matched: 2, Added: 1, Removed: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("same"), declared("rotated"), declared("created")}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("same"), held("rotated"), held("dropped")})},
		{name: "a secret a source without a claim keeps", want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Uninspected, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("pw")})},
		{name: "a secret the read could not establish", undecided: 1, want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("pw")}),
			current: state(schemaext.Observed, schemaext.Complete, nil, unread(ydbsecret.Kind, ydbsecret.Ref("ext", "pw")))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := secretRequest(c, test.desired, test.current)
			request.Requests = must.Must(ydbsecret.RotationRequests(test.rotate))

			result, err := secretRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}

// TestTopicComparison_CountsWhatItCompares pins the counts the inventory
// reports for topics: a topic both sides hold is matched whether or not its
// resolved settings differ, a creation is added and a drop removed, and a
// topic the comparison keeps or cannot decide is counted on neither side.
func TestTopicComparison_CountsWhatItCompares(t *testing.T) {
	kind := ydbdiff.TopicKind
	billing := []ydbtopic.ConsumerSpec{{Name: "billing"}}
	declaredSpec := ydbtopic.Spec{RetentionPeriod: "PT2H", Consumers: billing}
	changedSpec := ydbtopic.Spec{RetentionPeriod: "PT3H", Consumers: billing}
	heldSpec := ydbtopic.Spec{MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "PT2H",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576, Consumers: billing}
	declared := func(name string, spec ydbtopic.Spec) schemaext.Object {
		return ydbtopic.DesiredObject("app", name, "", spec)
	}
	held := func(name string) schemaext.Object { return ydbtopic.ObservedObject("app", name, heldSpec) }
	state := func(representation schemaext.Representation, knowledge schemaext.KnowledgeState, objects []schemaext.Object,
		limits ...schemaext.SubjectCoverage,
	) schemaext.ObjectState {
		return countedState(ydbtopic.Coverage, representation, knowledge, objects, limits...)
	}
	tests := []countCase{
		{name: "a topic that agrees once resolved", want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("events", declaredSpec)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("events")})},
		{name: "a topic that differs", changes: 1, want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("events", changedSpec)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("events")})},
		{name: "a creation", changes: 1, want: schemaext.ObjectCount{Kind: kind, Added: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("events", declaredSpec)}),
			current: state(schemaext.Observed, schemaext.Complete, nil)},
		{name: "a drop", changes: 1, want: schemaext.ObjectCount{Kind: kind, Removed: 1},
			desired: state(schemaext.Desired, schemaext.Complete, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("events")})},
		{name: "topics that agree, differ, are created and are dropped", changes: 3, want: schemaext.ObjectCount{Kind: kind, Matched: 2, Added: 1, Removed: 1},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{
				declared("same", declaredSpec), declared("changed", changedSpec), declared("created", declaredSpec)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("same"), held("changed"), held("dropped")})},
		{name: "a topic a source without a claim keeps", want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Uninspected, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{held("events")})},
		{name: "a topic the read could not establish", undecided: 1, want: schemaext.ObjectCount{Kind: kind},
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{declared("events", declaredSpec)}),
			current: state(schemaext.Observed, schemaext.Complete, nil, unread(ydbtopic.Kind, ydbtopic.Ref("app", "events")))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := topicRuntime(c).CompareObjects(t.Context(), topicRequest(test.desired, test.current))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}

// TestReplicationComparison_CountsWhatItCompares pins the counts the
// inventory reports for async replications and transfers, compared in one
// batch: an object both sides hold is matched whether or not it differs, a
// creation is added and a drop removed, an object the comparison keeps or
// cannot decide is counted on neither side, and a kind the batch holds
// nothing of is counted with zeros.
func TestReplicationComparison_CountsWhatItCompares(t *testing.T) {
	mirror := ydbreplication.ReplicationRef("app", "mirror")
	movedIngest := &ydbreplication.DesiredTransfer{Spec: ydbreplication.TransferSpec{Source: "other", Target: "t",
		Lambda: "($m) -> { return []; }"}}
	transfer := func(name string, value schemaext.Value) schemaext.Object {
		return schemaext.Object{Ref: ydbreplication.TransferRef("app", name), Value: value}
	}
	state := func(representation schemaext.Representation, knowledge schemaext.KnowledgeState, objects []schemaext.Object,
		limits ...schemaext.SubjectCoverage,
	) schemaext.ObjectState {
		replications := countedState(ydbreplication.ReplicationCoverage, representation, knowledge, objects, limits...)
		transfers := must.Must(ydbreplication.TransferCoverage(representation, schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}, nil))
		replications.Coverage = must.Must(replications.Coverage.Combine(transfers))
		return replications
	}
	tests := []struct {
		name               string
		desired, current   schemaext.ObjectState
		changes, undecided int
		want               []schemaext.ObjectCount
	}{
		{name: "a replication that agrees",
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{{Ref: mirror, Value: declaredMirror}}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{{Ref: mirror, Value: heldMirror}}),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.AsyncReplicationKind, Matched: 1}, {Kind: ydbdiff.TransferKind}}},
		{name: "a replication that differs", changes: 1,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{{Ref: mirror, Value: movedMirror}}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{{Ref: mirror, Value: heldMirror}}),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.AsyncReplicationKind, Matched: 1}, {Kind: ydbdiff.TransferKind}}},
		{name: "a creation", changes: 1,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{{Ref: mirror, Value: declaredMirror}}),
			current: state(schemaext.Observed, schemaext.Complete, nil),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.AsyncReplicationKind, Added: 1}, {Kind: ydbdiff.TransferKind}}},
		{name: "a drop", changes: 1,
			desired: state(schemaext.Desired, schemaext.Complete, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{{Ref: mirror, Value: heldMirror}}),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.AsyncReplicationKind, Removed: 1}, {Kind: ydbdiff.TransferKind}}},
		{name: "transfers that agree, differ, are created and are dropped", changes: 3,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{
				transfer("same", declaredIngest), transfer("changed", movedIngest), transfer("created", declaredIngest)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{
				transfer("same", heldIngest), transfer("changed", heldIngest), transfer("dropped", heldIngest)}),
			want: []schemaext.ObjectCount{{Kind: ydbdiff.AsyncReplicationKind}, {Kind: ydbdiff.TransferKind, Matched: 2, Added: 1, Removed: 1}}},
		{name: "a replication a source without a claim keeps",
			desired: state(schemaext.Desired, schemaext.Uninspected, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{{Ref: mirror, Value: heldMirror}}),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.AsyncReplicationKind}, {Kind: ydbdiff.TransferKind}}},
		{name: "a replication the read could not establish", undecided: 1,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{{Ref: mirror, Value: declaredMirror}}),
			current: state(schemaext.Observed, schemaext.Complete, nil, unread(ydbreplication.ReplicationKind, mirror)),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.AsyncReplicationKind}, {Kind: ydbdiff.TransferKind}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := replicationRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb",
				Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
				Kinds: []schemaext.Kind{ydbreplication.ReplicationKind, ydbreplication.TransferKind}, Desired: test.desired, Current: test.current})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, test.want)
		})
	}
}

// TestWorkloadComparison_CountsWhatItCompares pins the counts the inventory
// reports for resource pools and classifiers, compared in one batch: a
// declared object the database holds is matched whether or not it differs and
// a creation is added. Omission never drops a workload object, so nothing is
// removed, and an object the comparison keeps or cannot decide is counted on
// neither side.
func TestWorkloadComparison_CountsWhatItCompares(t *testing.T) {
	zero := ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}
	routed := ydbworkload.ClassifierSpec{ResourcePool: "Pool"}
	reranked := ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 10}
	state := func(representation schemaext.Representation, knowledge schemaext.KnowledgeState, objects []schemaext.Object,
		limits ...schemaext.SubjectCoverage,
	) schemaext.ObjectState {
		knows := schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}
		pools := must.Must(ydbworkload.Coverage(ydbworkload.PoolKind, representation, knows, limits))
		classifiers := must.Must(ydbworkload.Coverage(ydbworkload.ClassifierKind, representation, knows, nil))
		return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)), Coverage: must.Must(pools.Combine(classifiers))}
	}
	pool := ydbworkload.PoolRef("workload")
	absent := schemaext.SubjectCoverage{Kind: ydbworkload.PoolKind, Subject: pool,
		Knowledge: schemaext.Knowledge{State: schemaext.Absent, Reason: "the source asks for its absence"}}
	tests := []struct {
		name               string
		desired, current   schemaext.ObjectState
		changes, undecided int
		want               []schemaext.ObjectCount
	}{
		{name: "a pool that agrees",
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{ydbworkload.DesiredPoolObject("workload", "", zero)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{ydbworkload.ObservedPoolObject("workload", zero)}),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.ResourcePoolKind, Matched: 1}, {Kind: ydbdiff.ResourcePoolClassifierKind}}},
		{name: "a pool that differs", changes: 1,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{ydbworkload.DesiredPoolObject("workload", "", zero)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{ydbworkload.ObservedPoolObject("workload", ydbworkload.PoolSpec{})}),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.ResourcePoolKind, Matched: 1}, {Kind: ydbdiff.ResourcePoolClassifierKind}}},
		{name: "a creation", changes: 1,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{ydbworkload.DesiredPoolObject("workload", "", zero)}),
			current: state(schemaext.Observed, schemaext.Complete, nil),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.ResourcePoolKind, Added: 1}, {Kind: ydbdiff.ResourcePoolClassifierKind}}},
		{name: "classifiers that agree, differ, are created and are left out", changes: 2,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{ydbworkload.DesiredClassifierObject("same", "", routed),
				ydbworkload.DesiredClassifierObject("changed", "", routed), ydbworkload.DesiredClassifierObject("created", "", routed)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{ydbworkload.ObservedClassifierObject("same", routed),
				ydbworkload.ObservedClassifierObject("changed", reranked), ydbworkload.ObservedClassifierObject("omitted", routed)}),
			want: []schemaext.ObjectCount{{Kind: ydbdiff.ResourcePoolKind}, {Kind: ydbdiff.ResourcePoolClassifierKind, Matched: 2, Added: 1}}},
		{name: "a pool the source leaves out",
			desired: state(schemaext.Desired, schemaext.Complete, nil),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{ydbworkload.ObservedPoolObject("workload", zero)}),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.ResourcePoolKind}, {Kind: ydbdiff.ResourcePoolClassifierKind}}},
		{name: "a pool the source declares absent", undecided: 1,
			desired: state(schemaext.Desired, schemaext.Complete, nil, absent),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{ydbworkload.ObservedPoolObject("workload", zero)}),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.ResourcePoolKind}, {Kind: ydbdiff.ResourcePoolClassifierKind}}},
		{name: "a default pool the read did not report", undecided: 1,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{ydbworkload.DesiredPoolObject(ydbworkload.DefaultPool, "", ydbworkload.PoolSpec{ResourceWeight: new(50.0)})}),
			current: state(schemaext.Observed, schemaext.Complete, nil),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.ResourcePoolKind}, {Kind: ydbdiff.ResourcePoolClassifierKind}}},
		{name: "a pool the read could not establish", undecided: 1,
			desired: state(schemaext.Desired, schemaext.Complete, []schemaext.Object{ydbworkload.DesiredPoolObject("workload", "", zero)}),
			current: state(schemaext.Observed, schemaext.Complete, []schemaext.Object{ydbworkload.ObservedPoolObject("workload", zero)},
				unread(ydbworkload.PoolKind, pool)),
			want: []schemaext.ObjectCount{{Kind: ydbdiff.ResourcePoolKind}, {Kind: ydbdiff.ResourcePoolClassifierKind}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := workloadRuntime().CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb",
				Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Of(map[capability.Capability]bool{capability.ResourcePools: true}),
				Kinds: []schemaext.Kind{ydbworkload.PoolKind, ydbworkload.ClassifierKind}, Desired: test.desired, Current: test.current})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, test.want)
		})
	}
}

// TestExternalComparison_CountsWhatItCompares pins the counts the inventory
// reports for external data sources and external tables, compared in one
// batch: an object both sides hold is matched whether or not it differs, a
// creation is added and a drop removed, and an object the comparison keeps or
// cannot decide is counted on neither side.
func TestExternalComparison_CountsWhatItCompares(t *testing.T) {
	events := ydbexternal.Table{DataSource: "ext/bucket", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	moved := events.Clone()
	moved.Location = "f/"
	uninspected := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source did not describe external objects"}
	unclaimed := schemaext.ObjectState{Coverage: must.Must(must.Must(ydbexternal.SourceCoverage(schemaext.Desired, uninspected, nil)).Combine(
		must.Must(ydbexternal.TableCoverage(schemaext.Desired, uninspected, nil))))}
	tests := []struct {
		name               string
		desired, current   schemaext.ObjectState
		changes, undecided int
		want               []schemaext.ObjectCount
	}{
		{name: "a source and a table that agree",
			desired: externalState(schemaext.Desired, []schemaext.Object{
				ydbexternal.DesiredSourceObject("ext", "bucket", "", comparedBucket), ydbexternal.DesiredTableObject("ext", "events", "", events)}),
			current: externalState(schemaext.Observed, []schemaext.Object{
				ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket), ydbexternal.ObservedTableObject("ext", "events", events)}),
			want: []schemaext.ObjectCount{{Kind: ydbdiff.ExternalDataSourceKind, Matched: 1}, {Kind: ydbdiff.ExternalTableKind, Matched: 1}}},
		{name: "a table that differs", changes: 1,
			desired: externalState(schemaext.Desired, []schemaext.Object{
				ydbexternal.DesiredSourceObject("ext", "bucket", "", comparedBucket), ydbexternal.DesiredTableObject("ext", "events", "", moved)}),
			current: externalState(schemaext.Observed, []schemaext.Object{
				ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket), ydbexternal.ObservedTableObject("ext", "events", events)}),
			want: []schemaext.ObjectCount{{Kind: ydbdiff.ExternalDataSourceKind, Matched: 1}, {Kind: ydbdiff.ExternalTableKind, Matched: 1}}},
		{name: "a source and a table to create", changes: 2,
			desired: externalState(schemaext.Desired, []schemaext.Object{
				ydbexternal.DesiredSourceObject("ext", "bucket", "", comparedBucket), ydbexternal.DesiredTableObject("ext", "events", "", events)}),
			current: externalState(schemaext.Observed, nil),
			want:    []schemaext.ObjectCount{{Kind: ydbdiff.ExternalDataSourceKind, Added: 1}, {Kind: ydbdiff.ExternalTableKind, Added: 1}}},
		{name: "a source and a table to drop", changes: 2,
			desired: externalState(schemaext.Desired, nil),
			current: externalState(schemaext.Observed, []schemaext.Object{
				ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket), ydbexternal.ObservedTableObject("ext", "events", events)}),
			want: []schemaext.ObjectCount{{Kind: ydbdiff.ExternalDataSourceKind, Removed: 1}, {Kind: ydbdiff.ExternalTableKind, Removed: 1}}},
		{name: "a source and a table a source without a claim keeps",
			desired: unclaimed,
			current: externalState(schemaext.Observed, []schemaext.Object{
				ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket), ydbexternal.ObservedTableObject("ext", "events", events)}),
			want: []schemaext.ObjectCount{{Kind: ydbdiff.ExternalDataSourceKind}, {Kind: ydbdiff.ExternalTableKind}}},
		{name: "a table the read could not establish", undecided: 1,
			desired: externalState(schemaext.Desired, []schemaext.Object{
				ydbexternal.DesiredSourceObject("ext", "bucket", "", comparedBucket), ydbexternal.DesiredTableObject("ext", "events", "", events)}),
			current: externalState(schemaext.Observed, []schemaext.Object{ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket)},
				unread(ydbexternal.TableKind, ydbexternal.TableRef("ext", "events"))),
			want: []schemaext.ObjectCount{{Kind: ydbdiff.ExternalDataSourceKind, Matched: 1}, {Kind: ydbdiff.ExternalTableKind}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := externalRuntime(c).CompareObjects(t.Context(), externalComparison("/local", test.desired, test.current))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, test.want)
		})
	}
}

// TestExternalComparison_CountsATableTakenAlongAsMatched counts a data source
// the plan drops and creates again, and the unchanged table over it the plan
// takes along, as objects both sides hold: each has one change, and neither
// is an addition or a removal.
func TestExternalComparison_CountsATableTakenAlongAsMatched(t *testing.T) {
	c := qt.New(t)
	events := ydbexternal.Table{DataSource: "ext/bucket", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	relocated := comparedBucket
	relocated.Location = "https://storage.example/other/"
	request := externalComparison("/local",
		externalState(schemaext.Desired, []schemaext.Object{
			ydbexternal.DesiredSourceObject("ext", "bucket", "", relocated), ydbexternal.DesiredTableObject("ext", "events", "", events)}),
		externalState(schemaext.Observed, []schemaext.Object{
			ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket), ydbexternal.ObservedTableObject("ext", "events", events)}))
	request.Capabilities = request.Capabilities.With(capability.ExternalObjectReplace, false)

	result, err := externalRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 2)
	c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{
		{Kind: ydbdiff.ExternalDataSourceKind, Matched: 1}, {Kind: ydbdiff.ExternalTableKind, Matched: 1}})
}

// feedState is changefeeds in representation, from a source whose knowledge
// of the namespace is knowledge, with limits on single changefeeds.
func feedState(representation schemaext.Representation, knowledge schemaext.Knowledge, objects []schemaext.Object,
	limits ...schemaext.SubjectCoverage,
) schemaext.ObjectState {
	kinds := must.Must(ydbschema.ChangefeedCoverage(representation, nil)).KindRecords()
	kinds[0].Knowledge = schemaext.GlobalKnowledge(knowledge)
	return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(schemaext.NewCoverage(representation, kinds, limits))}
}

// TestComparison_CountsWhatItCompares pins the counts the inventory reports
// for changefeeds: on a table both sides hold, a changefeed both sides hold is
// matched whether or not it differs, a creation is added and a drop removed.
// A changefeed below a table only one side holds belongs to that table's
// change, and one the comparison keeps, one a replication controls and one it
// cannot decide are counted on neither side.
func TestComparison_CountsWhatItCompares(t *testing.T) {
	kind := ydbdiff.ChangefeedKind
	complete := schemaext.Knowledge{State: schemaext.Complete}
	uninspected := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source did not enumerate streams"}
	shorter := feed("changed")
	shorter.RetentionPeriod = "PT12H"
	declared := func(table string, spec ydbschema.ChangefeedSpec) schemaext.Object {
		return ydbschema.DesiredObject("", table, spec)
	}
	held := func(table string, spec ydbschema.ChangefeedSpec) schemaext.Object {
		return ydbschema.ObservedObject("", table, spec)
	}
	tests := []countCase{
		{name: "a changefeed that agrees", want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: feedState(schemaext.Desired, complete, []schemaext.Object{declared("orders", feed("updates"))}),
			current: feedState(schemaext.Observed, complete, []schemaext.Object{held("orders", feed("updates"))})},
		{name: "a changefeed that differs", changes: 1, want: schemaext.ObjectCount{Kind: kind, Matched: 1},
			desired: feedState(schemaext.Desired, complete, []schemaext.Object{declared("orders", shorter)}),
			current: feedState(schemaext.Observed, complete, []schemaext.Object{held("orders", feed("changed"))})},
		{name: "a creation", changes: 1, want: schemaext.ObjectCount{Kind: kind, Added: 1},
			desired: feedState(schemaext.Desired, complete, []schemaext.Object{declared("orders", feed("updates"))}),
			current: feedState(schemaext.Observed, complete, nil)},
		{name: "a drop", changes: 1, want: schemaext.ObjectCount{Kind: kind, Removed: 1},
			desired: feedState(schemaext.Desired, complete, nil),
			current: feedState(schemaext.Observed, complete, []schemaext.Object{held("orders", feed("updates"))})},
		{name: "changefeeds that agree, differ, are created and are dropped", changes: 3, want: schemaext.ObjectCount{Kind: kind, Matched: 2, Added: 1, Removed: 1},
			desired: feedState(schemaext.Desired, complete, []schemaext.Object{
				declared("orders", feed("same")), declared("orders", shorter), declared("orders", feed("created"))}),
			current: feedState(schemaext.Observed, complete, []schemaext.Object{
				held("orders", feed("same")), held("orders", feed("changed")), held("orders", feed("dropped"))})},
		{name: "changefeeds below a table only one side holds", want: schemaext.ObjectCount{Kind: kind},
			desired: feedState(schemaext.Desired, complete, []schemaext.Object{declared("fresh", feed("updates"))}),
			current: feedState(schemaext.Observed, complete, []schemaext.Object{held("gone", feed("updates"))})},
		{name: "a changefeed a source without a claim keeps", want: schemaext.ObjectCount{Kind: kind},
			desired: feedState(schemaext.Desired, uninspected, nil),
			current: feedState(schemaext.Observed, complete, []schemaext.Object{held("orders", feed("updates"))})},
		{name: "a changefeed a replication controls", want: schemaext.ObjectCount{Kind: kind},
			desired: feedState(schemaext.Desired, complete, nil),
			current: feedState(schemaext.Observed, complete, []schemaext.Object{
				{Ref: ydbschema.ChangefeedRef("", "orders", "updates"), Value: replicationStream()}})},
		{name: "a changefeed the read could not establish", undecided: 1, want: schemaext.ObjectCount{Kind: kind},
			desired: feedState(schemaext.Desired, complete, []schemaext.Object{declared("orders", feed("updates"))}),
			current: feedState(schemaext.Observed, complete, nil, limit(schemaext.Unrepresentable))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)

			result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb",
				Capabilities: capability.YDB262(), Desired: test.desired, Current: test.current,
				Parents: []schemaext.ParentState{{Subject: table("", "orders"), Desired: true, Current: true},
					{Subject: table("", "fresh"), Desired: true}, {Subject: table("", "gone"), Current: true}}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}
