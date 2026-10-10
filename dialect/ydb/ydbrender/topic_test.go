package ydbrender_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbtopic"
)

func topicRegistry(c *qt.C) renderer.Extensions {
	registry, err := renderer.NewExtensions(ydbrender.TopicHandler(), ydbrender.TopicConsumerHandler())
	c.Assert(err, qt.IsNil)
	return registry
}

// topicOperation is a statement on the topic name in the directory schema
// from before to after; a nil side is the topic's absence.
func topicOperation(schema, name string, before, after *ydbtopic.Spec) *ydbast.Topic {
	operation := &ydbast.Topic{Schema: schema, Name: name}
	if before != nil {
		operation.Change.Before = &ydbtopic.Observed{Spec: *before}
	}
	if after != nil {
		operation.Change.After = &ydbtopic.Desired{Spec: *after}
	}
	return operation
}

// TestTopicHandler_HappyPath pins the statements a topic operation renders as.
// Each was applied to local-ydb 26.2.1.14 and 25.1.4.7 one statement per
// query and read back through DescribeTopic. A dot stays part of the name it
// is written in.
func TestTopicHandler_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		operation ast.ExtensionPayload
		want      []string
	}{
		{name: "a topic with a consumer", caps: capability.YDB251(),
			operation: topicOperation("app", "events", nil, &ydbtopic.Spec{MinActivePartitions: 2,
				Consumers: []ydbtopic.ConsumerSpec{{Name: "billing", Important: true}}}),
			want: []string{"CREATE TOPIC `app/events` (CONSUMER `billing` WITH (important = TRUE)) WITH (min_active_partitions = 2);"}},
		{name: "a consumer with an availability period on 26.2", caps: capability.YDB262(),
			operation: topicOperation("", "events.v1", nil, &ydbtopic.Spec{
				Consumers: []ydbtopic.ConsumerSpec{{Name: "audit", AvailabilityPeriod: "PT2H"}}}),
			want: []string{"CREATE TOPIC `events.v1` (CONSUMER `audit` WITH (availability_period = Interval('PT2H')));"}},
		{name: "a change", caps: capability.YDB251(),
			operation: topicOperation("", "events", &ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "gone"}}},
				&ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "fresh"}}}),
			want: []string{"ALTER TOPIC `events` DROP CONSUMER `gone`, ADD CONSUMER `fresh`;"}},
		{name: "a drop", caps: capability.YDB251(), operation: topicOperation("app", "events", &ydbtopic.Spec{}, nil),
			want: []string{"DROP TOPIC `app/events`;"}},
		{name: "a consumer added to a changefeed's topic", caps: capability.YDB251(),
			operation: &ydbast.TopicConsumer{Schema: "app/orders", Name: "updates", Consumer: ydbtopic.ConsumerSpec{Name: "audit", Important: true}},
			want:      []string{"ALTER TOPIC `app/orders/updates` ADD CONSUMER `audit` WITH (important = TRUE);"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := topicRegistry(c).Render(renderer.ExtensionContext{Target: "ydb", Capabilities: test.caps}, ast.StatementExtension, test.operation)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestTopicHandler_RefusesByCapability names the key a target lacks, for a
// creation, a change and a drop alike, and the availability period 25.1 does
// not take.
func TestTopicHandler_RefusesByCapability(t *testing.T) {
	without := capability.YDB262().With(capability.Topics, false)
	tests := []struct {
		name      string
		caps      capability.Capabilities
		operation ast.ExtensionPayload
		key       capability.Capability
		wantErr   string
	}{
		{name: "a creation without topics", caps: without, operation: topicOperation("", "events", nil, &ydbtopic.Spec{}),
			key: capability.Topics, wantErr: "topic events, which requires target capability topics, unavailable on this ydb target"},
		{name: "a change without topics", caps: without,
			operation: topicOperation("", "events", &ydbtopic.Spec{}, &ydbtopic.Spec{RetentionPeriod: "PT2H"}),
			key:       capability.Topics, wantErr: "topic events, which requires target capability topics, unavailable on this ydb target"},
		{name: "a drop without topics", caps: without, operation: topicOperation("", "events", &ydbtopic.Spec{}, nil),
			key: capability.Topics, wantErr: "topic events, which requires target capability topics, unavailable on this ydb target"},
		{name: "a consumer without topics", caps: without,
			operation: &ydbast.TopicConsumer{Name: "events", Consumer: ydbtopic.ConsumerSpec{Name: "audit"}},
			key:       capability.Topics, wantErr: "topic events, which requires target capability topics, unavailable on this ydb target"},
		{name: "an availability period on 25.1", caps: capability.YDB251(),
			operation: topicOperation("", "events", nil, &ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "audit", AvailabilityPeriod: "PT2H"}}}),
			key:       capability.TopicConsumerAvailabilityPeriod,
			wantErr: `consumer "audit" of topic events takes availability_period, which requires target capability ` +
				"topic_consumer_availability_period, unavailable on this ydb target"},
		{name: "a change to an availability period on 25.1", caps: capability.YDB251(),
			operation: topicOperation("", "events", &ydbtopic.Spec{},
				&ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "audit", AvailabilityPeriod: "PT2H"}}}),
			key: capability.TopicConsumerAvailabilityPeriod,
			wantErr: `consumer "audit" of topic events takes availability_period, which requires target capability ` +
				"topic_consumer_availability_period, unavailable on this ydb target"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := topicRegistry(c).Render(renderer.ExtensionContext{Target: "ydb", Capabilities: test.caps}, ast.StatementExtension, test.operation)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(test.key))
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestTopicHandler_FailurePath refuses a change YDB cannot make in place, an
// operation without a change, and a target other than YDB.
func TestTopicHandler_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		operation ast.ExtensionPayload
		wantErr   string
		wantIs    error
	}{
		{name: "fewer partitions", target: "ydb",
			operation: topicOperation("", "events", &ydbtopic.Spec{MinActivePartitions: 2}, &ydbtopic.Spec{}),
			wantErr: "topic events: it has 2 partitions and is declared with 1, and YDB never removes a partition " +
				"\\(`Invalid total groups count specified`\\); drop the topic and create it again to lower the count",
			wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "no change", target: "ydb", operation: &ydbast.Topic{Name: "events", Change: ydbdiff.Topic{}},
			wantErr: `.*a topic change requires a before or after operand`, wantIs: ptaherr.ErrInvalidSchemaDiff},
		{name: "another target", target: "postgres", operation: topicOperation("", "events", nil, &ydbtopic.Spec{}),
			wantErr: "topic events, which requires target capability topics, unavailable on this postgres target",
			wantIs:  ptaherr.ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := topicRegistry(c).Render(renderer.ExtensionContext{Target: test.target, Capabilities: capability.YDB262()}, ast.StatementExtension, test.operation)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(got, qt.IsNil)
		})
	}
}
