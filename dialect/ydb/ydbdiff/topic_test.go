package ydbdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
)

// topicSpec is a topic holding a consumer of each name.
func topicSpec(names ...string) ydbtopic.Spec {
	spec := ydbtopic.Spec{}
	for _, name := range names {
		spec.Consumers = append(spec.Consumers, ydbtopic.ConsumerSpec{Name: name})
	}
	return spec
}

// TestTopicChange_RoundTrip keeps both operands, every setting and every
// consumer through the registered change codec.
func TestTopicChange_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		change *ydbdiff.Topic
	}{
		{name: "create", change: &ydbdiff.Topic{After: &ydbtopic.Desired{Spec: ydbtopic.Spec{RetentionPeriod: "PT2H"}, StructName: "Events"}}},
		{name: "drop", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: topicSpec("billing")}}},
		{name: "alter", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: topicSpec("gone")}, After: &ydbtopic.Desired{Spec: topicSpec("fresh")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbdiff.Codecs()})).Codecs()
			document, err := registry.Marshal(t.Context(), schemaext.Change, []schemaext.Payload{test.change})
			c.Assert(err, qt.IsNil)
			decoded, err := registry.Unmarshal(t.Context(), document)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.change})
		})
	}
}

// TestTopicChange_FailurePath refuses a change without explicit operands, an
// operand YDB would not keep, a field no change takes, and two operands that
// describe one topic.
func TestTopicChange_FailurePath(t *testing.T) {
	for _, input := range []string{
		`null`, `{}`, `{"before":null,"after":null}`, `{"after":{"spec":{}}}`,
		`{"before":null,"after":{"spec":{"max_active_partitions":4}}}`,
		`{"before":{"spec":{},"struct_name":"Events"},"after":null}`,
		`{"before":null,"after":{"spec":{}},"extra":true}`,
		`{"before":{"spec":{}},"after":{"spec":{}}}`,
	} {
		t.Run(input, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbdiff.TopicCodec().Decode(json.RawMessage(input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestTopicChange_Effect reports what each change does to the topic's
// messages and its consumers' positions: a drop of the topic or of a consumer,
// one YDB changes only by dropping it and adding it again included, loses
// them; a creation, and a change that only adds consumers, loses nothing; and
// any other change can shorten how long a message stays.
func TestTopicChange_Effect(t *testing.T) {
	codecs := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c", SupportedCodecs: []string{"raw"}}}}
	tests := []struct {
		name   string
		change *ydbdiff.Topic
		want   schemaext.Effect
	}{
		{name: "create", change: &ydbdiff.Topic{After: &ydbtopic.Desired{}},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE TOPIC adds a topic"}},
		{name: "drop", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{}},
			want: schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbdiff.DropTopicReason}},
		{name: "a consumer dropped", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: topicSpec("c")}, After: &ydbtopic.Desired{}},
			want: schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbdiff.DropConsumerReason}},
		{name: "a consumer dropped and added again", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: codecs}, After: &ydbtopic.Desired{Spec: topicSpec("c")}},
			want: schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbdiff.DropConsumerReason}},
		{name: "a consumer added", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{}, After: &ydbtopic.Desired{Spec: topicSpec("c")}},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "ALTER TOPIC adds consumers"}},
		{name: "a setting changed", change: &ydbdiff.Topic{Before: &ydbtopic.Observed{}, After: &ydbtopic.Desired{Spec: ydbtopic.Spec{RetentionPeriod: "PT1H"}}},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbdiff.ChangeTopicReason}},
		{name: "invalid", change: &ydbdiff.Topic{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Effect(), qt.DeepEquals, test.want)
		})
	}
}
