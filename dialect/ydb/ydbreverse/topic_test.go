package ydbreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbtopic"
)

func topicReverseRequest(changes ...schemaext.ChangeRecord) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(), Changes: changes}
}

// TestTopicReversal_HappyPath drops what the change created, creates again
// what it dropped with the settings and consumers it had, and moves a changed
// topic back to the nearest state YDB reaches in place: the partitions the
// change added stay, and auto-partitioning it enabled stays, paused. Each
// thing the rollback cannot restore -- messages, consumer positions,
// partitions -- is reported.
func TestTopicReversal_HappyPath(t *testing.T) {
	ref := ydbtopic.Ref("app", "events")
	held := ydbtopic.Spec{RetentionPeriod: "PT2H", Consumers: []ydbtopic.ConsumerSpec{{Name: "billing"}}}
	grown := ydbtopic.Spec{MinActivePartitions: 3, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up"}
	tests := []struct {
		name        string
		change      *ydbdiff.Topic
		want        schemaext.ChangeValue
		forward     schemaext.Value
		limitations []string
	}{
		{name: "a creation is dropped", change: &ydbdiff.Topic{After: &ydbtopic.Desired{Spec: held}},
			want: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: held}}, forward: &ydbtopic.Observed{Spec: held},
			limitations: []string{"dropping topic app/events discards every message written to it and every consumer's position"}},
		{name: "a drop is created again", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: held}},
			want:        &ydbdiff.Topic{After: &ydbtopic.Desired{Spec: held}},
			limitations: []string{"the messages topic app/events held and its consumers' positions were dropped; the rollback creates it empty"}},
		{name: "a consumer dropped comes back", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: held}, After: &ydbtopic.Desired{Spec: ydbtopic.Spec{RetentionPeriod: "PT2H"}}},
			want:        &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: ydbtopic.Spec{RetentionPeriod: "PT2H"}}, After: &ydbtopic.Desired{Spec: held}},
			forward:     &ydbtopic.Observed{Spec: ydbtopic.Spec{RetentionPeriod: "PT2H"}},
			limitations: []string{`consumer "billing" of topic app/events was dropped; the rollback adds it again without its position`}},
		{name: "partitions and auto-partitioning stay", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{}, After: &ydbtopic.Desired{Spec: grown}},
			want: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: grown}, After: &ydbtopic.Desired{Spec: ydbtopic.Spec{
				MinActivePartitions: 3, MaxActivePartitions: 6, AutoPartitioningStrategy: "paused",
				AutoPartitioningUpUtilizationPercent: 90, AutoPartitioningDownUtilizationPercent: 30, AutoPartitioningStabilizationWindow: "PT5M"}}},
			forward: &ydbtopic.Observed{Spec: grown},
			limitations: []string{
				"topic app/events keeps the 3 partitions the change gave it, since YDB never removes a partition",
				"topic app/events keeps auto-partitioning, paused, since YDB does not disable it once it is on",
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreverse.TopicService{}).ReverseChanges(t.Context(), topicReverseRequest(schemaext.ChangeRecord{Subject: ref, Value: test.change}))
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change, qt.DeepEquals, schemaext.ChangeRecord{Subject: ref, Value: test.want})
			c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: ydbtopic.Kind, Value: test.forward}})
			c.Assert(result[0].Limitations, qt.DeepEquals, test.limitations)
		})
	}
}

// TestTopicReversal_FailurePath refuses a target without the topics
// capability and a change the owner does not recognize, with no partial
// result.
func TestTopicReversal_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReversalRequest
	}{
		{name: "a line without topics", request: schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
			Capabilities: capability.YDB262().With(capability.Topics, false),
			Changes:      []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("", "events"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{}}}}}},
		{name: "a change of another family", request: topicReverseRequest(schemaext.ChangeRecord{Subject: ydbtopic.Ref("", "events"), Value: &ydbdiff.StreamingQuery{}})},
		{name: "an empty change", request: topicReverseRequest(schemaext.ChangeRecord{Subject: ydbtopic.Ref("", "events"), Value: &ydbdiff.Topic{}})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreverse.TopicService{}).ReverseChanges(t.Context(), test.request)
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.IsNil)
		})
	}
}
