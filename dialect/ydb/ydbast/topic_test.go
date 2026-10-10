package ydbast_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
)

func topicRegistry(c *qt.C) schemaext.Registry {
	c.Helper()
	registry, err := schemaext.NewRegistry(
		schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.TopicCodec()},
		schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.TopicConsumerCodec()})
	c.Assert(err, qt.IsNil)
	return registry
}

// TestTopicCodecs_HappyPath round-trips each statement on a topic with its
// separate path parts; a dot stays in the part it is written in.
func TestTopicCodecs_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value schemaext.Payload
	}{
		{name: "create", value: &ydbast.Topic{Schema: "jobs.daily", Name: "events.v1",
			Change: ydbdiff.Topic{After: &ydbtopic.Desired{Spec: ydbtopic.Spec{RetentionPeriod: "PT2H"}}}}},
		{name: "alter", value: &ydbast.Topic{Name: "events", Change: ydbdiff.Topic{Before: &ydbtopic.Observed{},
			After: &ydbtopic.Desired{Spec: ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c"}}}}}}},
		{name: "drop", value: &ydbast.Topic{Schema: "app", Name: "events", Change: ydbdiff.Topic{Before: &ydbtopic.Observed{}}}},
		{name: "a consumer", value: &ydbast.TopicConsumer{Schema: "app/orders", Name: "updates",
			Consumer: ydbtopic.ConsumerSpec{Name: "audit", Important: true, SupportedCodecs: []string{"raw"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := topicRegistry(c).Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{test.value})
			c.Assert(err, qt.IsNil)
			decoded, err := topicRegistry(c).Unmarshal(c.Context(), data)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.value})
		})
	}
}

// TestTopicCodec_FailurePath refuses a topic statement wire that is
// incomplete, holds a null, carries a field no statement takes, or names no
// topic, each with the message of the check that refuses it.
func TestTopicCodec_FailurePath(t *testing.T) {
	wire := `{"schema":"app","name":"events","change":{"before":null,"after":{"spec":{}}}}`
	tests := []struct{ name, data, wantErr string }{
		{name: "missing change", data: `{"schema":"app","name":"events"}`,
			wantErr: "invalid feature value: topic operation requires exactly schema, name, and change"},
		{name: "null name", data: strings.Replace(wire, `"name":"events"`, `"name":null`, 1),
			wantErr: "invalid feature value: topic operation requires name"},
		{name: "null schema", data: strings.Replace(wire, `"schema":"app"`, `"schema":null`, 1),
			wantErr: "invalid feature value: topic operation requires schema"},
		{name: "an extra field", data: strings.Replace(wire, `"schema":"app"`, `"schema":"app","extra":1`, 1),
			wantErr: "invalid feature value: topic operation requires exactly schema, name, and change"},
		{name: "no change", data: strings.Replace(wire, `"after":{"spec":{}}`, `"after":null`, 1),
			wantErr: "invalid feature value: a topic change requires a before or after operand"},
		{name: "path in the name", data: strings.Replace(wire, `"name":"events"`, `"name":"app/events"`, 1),
			wantErr: "invalid feature value: a topic requires a schema-scoped YDB identity"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbast.TopicCodec().Decode(json.RawMessage(test.data))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestTopicConsumerCodec_FailurePath refuses a consumer statement wire that is
// incomplete, holds a null, names no topic, or carries a consumer YDB would
// refuse.
func TestTopicConsumerCodec_FailurePath(t *testing.T) {
	wire := `{"schema":"app","name":"events","consumer":{"name":"audit"}}`
	tests := []struct{ name, data string }{
		{name: "missing consumer", data: `{"schema":"app","name":"events"}`},
		{name: "a null", data: strings.Replace(wire, `"schema":"app"`, `"schema":null`, 1)},
		{name: "an extra field", data: strings.Replace(wire, `"schema":"app"`, `"schema":"app","extra":1`, 1)},
		{name: "path in the name", data: strings.Replace(wire, `"name":"events"`, `"name":"app/events"`, 1)},
		{name: "an important consumer with an availability period",
			data: strings.Replace(wire, `{"name":"audit"}`, `{"name":"audit","important":true,"availability_period":"PT1H"}`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbast.TopicConsumerCodec().Decode(json.RawMessage(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestTopicEffect classifies each statement by what it does to the topic's
// messages and its consumers' positions, and an invalid statement as unknown
// rather than additive.
func TestTopicEffect(t *testing.T) {
	tests := []struct {
		name  string
		value interface{ Effect() schemaext.Effect }
		want  schemaext.Effect
	}{
		{name: "create", value: &ydbast.Topic{Name: "events", Change: ydbdiff.Topic{After: &ydbtopic.Desired{}}},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE TOPIC adds a topic"}},
		{name: "drop", value: &ydbast.Topic{Name: "events", Change: ydbdiff.Topic{Before: &ydbtopic.Observed{}}},
			want: schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbdiff.DropTopicReason}},
		{name: "a consumer added", value: &ydbast.TopicConsumer{Name: "events", Consumer: ydbtopic.ConsumerSpec{Name: "audit"}},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "ADD CONSUMER adds a topic consumer"}},
		{name: "an invalid statement", value: &ydbast.Topic{Name: "events"}},
		{name: "an invalid consumer", value: &ydbast.TopicConsumer{Name: "events"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.value.Effect(), qt.DeepEquals, test.want)
		})
	}
}
