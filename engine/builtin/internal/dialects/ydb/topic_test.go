package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

// TestRender_Topic_HappyPath pins the statements each topic node renders as.
// Each was applied to local-ydb 26.2.1.14 and 25.1.4.7 one statement per
// query and read back through DescribeTopic.
func TestRender_Topic_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a topic with a consumer",
			caps: capability.YDB251(),
			node: ast.NewCreateTopic("app.events", ast.TopicSpec{MinActivePartitions: 2,
				Consumers: []ast.TopicConsumerSpec{{Name: "billing", Important: true}}}),
			want: "CREATE TOPIC `app/events` (CONSUMER `billing` WITH (important = TRUE)) WITH (min_active_partitions = 2);\n",
		},
		{
			name: "a consumer with an availability period on 26.2",
			caps: capability.YDB262(),
			node: ast.NewCreateTopic("events", ast.TopicSpec{
				Consumers: []ast.TopicConsumerSpec{{Name: "audit", AvailabilityPeriod: "PT2H"}}}),
			want: "CREATE TOPIC `events` (CONSUMER `audit` WITH (availability_period = Interval('PT2H')));\n",
		},
		{
			name: "a change",
			caps: capability.YDB251(),
			node: ast.NewAlterTopic("events", ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "fresh"}}},
				ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "gone"}}}),
			want: "ALTER TOPIC `events` DROP CONSUMER `gone`, ADD CONSUMER `fresh`;\n",
		},
		{
			name: "a drop",
			caps: capability.YDB251(),
			node: ast.NewDropTopic("app.events"),
			want: "DROP TOPIC `app/events`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// A topic YDB cannot hold is refused by name: through the key a line lacks,
// or with YDB's own answer where no line takes it.
func TestRender_Topic_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a target without topics",
			caps: capability.YDB262().With(capability.Topics, false),
			node: ast.NewCreateTopic("events", ast.TopicSpec{}),
			want: "topic events, which requires target capability topics, unavailable on this ydb target",
		},
		{
			name: "a change on a target without topics",
			caps: capability.YDB262().With(capability.Topics, false),
			node: ast.NewAlterTopic("events", ast.TopicSpec{RetentionPeriod: "PT2H"}, ast.TopicSpec{}),
			want: "topic events, which requires target capability topics, unavailable on this ydb target",
		},
		{
			name: "a drop on a target without topics",
			caps: capability.YDB262().With(capability.Topics, false),
			node: ast.NewDropTopic("events"),
			want: "topic events, which requires target capability topics, unavailable on this ydb target",
		},
		{
			name: "an availability period on 25.1",
			caps: capability.YDB251(),
			node: ast.NewCreateTopic("events", ast.TopicSpec{
				Consumers: []ast.TopicConsumerSpec{{Name: "audit", AvailabilityPeriod: "PT2H"}}}),
			want: `consumer "audit" of topic events takes availability_period, which requires target capability ` +
				"topic_consumer_availability_period, unavailable on this ydb target",
		},
		{
			name: "a change to an availability period on 25.1",
			caps: capability.YDB251(),
			node: ast.NewAlterTopic("events", ast.TopicSpec{
				Consumers: []ast.TopicConsumerSpec{{Name: "audit", AvailabilityPeriod: "PT2H"}}}, ast.TopicSpec{}),
			want: `consumer "audit" of topic events takes availability_period, which requires target capability ` +
				"topic_consumer_availability_period, unavailable on this ydb target",
		},
		{
			name: "fewer partitions",
			caps: capability.YDB262(),
			node: ast.NewAlterTopic("events", ast.TopicSpec{}, ast.TopicSpec{MinActivePartitions: 2}),
			want: "topic events: it has 2 partitions and is declared with 1, and YDB never removes a partition " +
				"\\(`Invalid total groups count specified`\\); drop the topic and create it again to lower the count",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
