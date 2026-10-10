package ydbcompare_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
)

func topicRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbtopic.Codecs(), ydbdiff.TopicCodec()),
		Comparisons: []engine.ObjectComparison{{Target: "ydb", Kinds: []schemaext.Kind{ydbtopic.Kind},
			ChangeKinds: []schemaext.Kind{ydbdiff.TopicKind}, Service: ydbcompare.TopicService{}}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func topicState(c *qt.C, direction schemaext.Representation, knowledge schemaext.KnowledgeState, value schemaext.Value, limits ...schemaext.SubjectCoverage) schemaext.ObjectState {
	c.Helper()
	coverage, err := ydbtopic.Coverage(direction, schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}, limits)
	c.Assert(err, qt.IsNil)
	state := schemaext.ObjectState{Coverage: coverage}
	if value != nil {
		state.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbtopic.Ref("app", "events"), Value: value}))
	}
	return state
}

func topicRequest(desired, current schemaext.ObjectState) schemaext.ObjectComparisonRequest {
	return schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbtopic.Kind}, Desired: desired, Current: current}
}

// TestTopicComparison_ComparesResolvedSettingsAndEvidence compares a topic
// through its resolved settings: a declaration naming a default and one
// leaving it out are the same topic. A creation is planned only where the
// read established absence, and a removal only where the source describes
// topics; an unread topic is reported only to a source that makes a claim
// about it.
func TestTopicComparison_ComparesResolvedSettingsAndEvidence(t *testing.T) {
	declared := &ydbtopic.Desired{Spec: ydbtopic.Spec{RetentionPeriod: "PT2H", Consumers: []ydbtopic.ConsumerSpec{{Name: "billing"}}}}
	defaults := &ydbtopic.Desired{Spec: ydbtopic.Spec{RetentionPeriod: "PT120M", MinActivePartitions: 1,
		Consumers: []ydbtopic.ConsumerSpec{{Name: "billing"}}}}
	observed := &ydbtopic.Observed{Spec: ydbtopic.Spec{MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "PT2H",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576, Consumers: []ydbtopic.ConsumerSpec{{Name: "billing"}}}}
	other := &ydbtopic.Desired{Spec: ydbtopic.Spec{RetentionPeriod: "PT3H"}}
	limit := []schemaext.SubjectCoverage{{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("app", "events"),
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbtopic.QueueGroupReason}}}
	tests := []struct {
		name                               string
		desired, current                   schemaext.Value
		desiredKnowledge, currentKnowledge schemaext.KnowledgeState
		currentLimits                      []schemaext.SubjectCoverage
		want                               []schemaext.ChangeRecord
		undecided, objects                 int
	}{
		{name: "create", desired: declared, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1,
			want: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{After: declared}}}},
		{name: "drop", current: observed, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete,
			want: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{Before: observed}}}},
		{name: "equal once resolved", desired: declared, current: observed, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "equal with the defaults named", desired: defaults, current: observed, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "changed", desired: other, current: observed, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1,
			want: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{Before: observed, After: other}}}},
		{name: "a source that cannot describe topics keeps the held one", current: observed, desiredKnowledge: schemaext.Uninspected,
			currentKnowledge: schemaext.Complete, objects: 1},
		{name: "an unread namespace withholds the creation", desired: declared, desiredKnowledge: schemaext.Complete,
			currentKnowledge: schemaext.Uninspected, objects: 1, undecided: 2},
		{name: "an unread topic withholds its removal", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete,
			currentLimits: limit, undecided: 1},
		{name: "an unread topic withholds the creation", desired: declared, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete,
			currentLimits: limit, objects: 1, undecided: 1},
		{name: "an unread topic is nothing to a source without a claim", desiredKnowledge: schemaext.Uninspected,
			currentKnowledge: schemaext.Complete, currentLimits: limit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := topicRequest(
				topicState(c, schemaext.Desired, test.desiredKnowledge, test.desired),
				topicState(c, schemaext.Observed, test.currentKnowledge, test.current, test.currentLimits...))
			result, err := topicRuntime(c).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.DeepEquals, test.want)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, test.objects)
		})
	}
}

// TestTopicComparison_KeepsTheHeldTopicAsADeclaration adopts a topic the
// source does not describe as a declaration of every setting and consumer the
// read reported.
func TestTopicComparison_KeepsTheHeldTopicAsADeclaration(t *testing.T) {
	c := qt.New(t)
	held := &ydbtopic.Observed{Spec: ydbtopic.Spec{RetentionPeriod: "PT2H", Consumers: []ydbtopic.ConsumerSpec{{Name: "billing"}}}}
	request := topicRequest(topicState(c, schemaext.Desired, schemaext.Uninspected, nil),
		topicState(c, schemaext.Observed, schemaext.Complete, held))

	result, err := topicRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	values, err := result.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []schemaext.Object{ydbtopic.DesiredObject("app", "events", "", held.Spec)})
	c.Assert(result.Desired.Coverage.Lookup(ydbtopic.Kind, values[0].Ref).State, qt.Equals, schemaext.Complete)
}

// TestTopicComparison_PlansNothingOnALineWithoutTopics compares topics on a
// target without the topics capability, and refuses nothing, when no
// statement would run: a topic both sides hold, one a source without a claim
// keeps, and an unread one.
func TestTopicComparison_PlansNothingOnALineWithoutTopics(t *testing.T) {
	declared := &ydbtopic.Desired{}
	observed := &ydbtopic.Observed{Spec: ydbtopic.Spec{MinActivePartitions: 1, RetentionPeriod: "P1D",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576}}
	limit := []schemaext.SubjectCoverage{{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("app", "events"),
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbtopic.UnsupportedReason}}}
	tests := []struct {
		name             string
		desiredKnowledge schemaext.KnowledgeState
		desired, current schemaext.Value
		currentLimits    []schemaext.SubjectCoverage
		undecided        int
	}{
		{name: "equal once resolved", desiredKnowledge: schemaext.Complete, desired: declared, current: observed},
		{name: "kept by a source without a claim", desiredKnowledge: schemaext.Uninspected, current: observed},
		{name: "unread, without a claim", desiredKnowledge: schemaext.Uninspected, currentLimits: limit},
		{name: "unread, with a claim", desiredKnowledge: schemaext.Complete, currentLimits: limit, undecided: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := topicRequest(
				topicState(c, schemaext.Desired, test.desiredKnowledge, test.desired),
				topicState(c, schemaext.Observed, schemaext.Complete, test.current, test.currentLimits...))
			request.Capabilities = capability.YDB262().With(capability.Topics, false)
			result, err := topicRuntime(c).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
		})
	}
}

// TestTopicComparison_FailurePath refuses a request it cannot answer without
// returning part of an answer: a target without the topics capability, named
// by the topic's path, another target, and a canceled context.
func TestTopicComparison_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		caps    capability.Capabilities
		wantErr string
		wantIs  error
	}{
		{name: "a line without topics", target: "ydb", caps: capability.YDB262().With(capability.Topics, false),
			wantErr: "topic app/events, which requires target capability topics, unavailable on this ydb target", wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "another target", target: "postgres", caps: capability.YDB262(),
			wantErr: `.*YDB comparison on "postgres"`, wantIs: ptaherr.ErrUnsupportedDialect},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := topicRequest(topicState(c, schemaext.Desired, schemaext.Complete, &ydbtopic.Desired{}),
				topicState(c, schemaext.Observed, schemaext.Complete, nil))
			request.Target, request.Capabilities = test.target, test.caps
			result, err := (ydbcompare.TopicService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
}

// TestTopicComparison_FailurePath_CoverageSubjects refuses a coverage record
// the topic comparison cannot read: one about another kind, one that declares
// a default topic, and one whose path is not a topic's.
func TestTopicComparison_FailurePath_CoverageSubjects(t *testing.T) {
	unread := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}
	tests := []struct {
		name     string
		coverage schemaext.Coverage
		wantErr  string
	}{
		{name: "another kind", coverage: must.Must(must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)).Combine(
			must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
				[]schemaext.SubjectCoverage{{Kind: ydbsecret.Kind, Subject: ydbsecret.Ref("app", "pw"), Knowledge: unread}})))),
			wantErr: "invalid feature value: topic coverage cannot declare another kind or a default object"},
		{name: "a default topic", coverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("app", "events"), Knowledge: schemaext.Knowledge{State: schemaext.Defaulted}}})),
			wantErr: "invalid feature value: topic coverage cannot declare another kind or a default object"},
		{name: "an absolute directory", coverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("/app", "events"), Knowledge: unread}})),
			wantErr: "invalid feature value: a topic requires a schema-scoped YDB identity"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := topicRequest(schemaext.ObjectState{Coverage: test.coverage}, topicState(c, schemaext.Observed, schemaext.Complete, nil))
			result, err := (ydbcompare.TopicService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
}

// TestTopicComparison_RefusesACanceledContext returns no answer once the
// caller has given up.
func TestTopicComparison_RefusesACanceledContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := (ydbcompare.TopicService{}).CompareObjects(ctx, schemaext.ObjectComparisonRequest{})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}
