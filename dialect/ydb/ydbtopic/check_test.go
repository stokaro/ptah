package ydbtopic_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbtopic"
)

func TestCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		spec ydbtopic.Spec
		caps capability.Capabilities
	}{
		{name: "nothing declared", caps: capability.YDB251()},
		{name: "a consumer with an availability period on 26.2", caps: capability.YDB262(),
			spec: ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c", AvailabilityPeriod: "PT1H"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.Check("app", "events", test.spec, test.caps), qt.IsNil)
		})
	}
}

func TestCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		spec  ydbtopic.Spec
		caps  capability.Capabilities
		want  ydbtopic.Refusal
	}{
		{name: "a target without topics", topic: "events", caps: capability.Postgres17(),
			want: ydbtopic.Refusal{Subject: "topic events", Key: capability.Topics}},
		{name: "a line without availability_period", topic: "events", caps: capability.YDB251(),
			spec: ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c", AvailabilityPeriod: "PT1H"}}},
			want: ydbtopic.Refusal{Subject: `consumer "c" of topic events takes availability_period`,
				Key: capability.TopicConsumerAvailabilityPeriod}},
		{name: "no name", topic: " ", caps: capability.YDB262(),
			want: ydbtopic.Refusal{Subject: "a topic", Reason: "a topic needs a name"}},
		{name: "a setting the parse refuses", topic: "events", caps: capability.YDB262(),
			spec: ydbtopic.Spec{MaxActivePartitions: 4},
			want: ydbtopic.Refusal{Subject: "topic events", Reason: "invalid max_active_partitions: it shapes " +
				"auto-partitioning, which this topic does not enable; declare auto_partitioning_strategy as scale_up, " +
				"scale_up_and_down or paused"}},
		{name: "a consumer named twice", topic: "events", caps: capability.YDB262(),
			spec: ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c"}, {Name: "c"}}},
			want: ydbtopic.Refusal{Subject: "topic events", Reason: `two of its consumers are named "c", and YDB ` +
				"names a consumer once per topic (`Consumer c defined more than once`)"}},
		{name: "a consumer the parse refuses", topic: "events", caps: capability.YDB262(),
			spec: ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c", Important: true, AvailabilityPeriod: "PT1H"}}},
			want: ydbtopic.Refusal{Subject: `topic events`, Reason: `consumer "c": invalid availability_period "PT1H": ` +
				"YDB keeps every unread record for an important consumer, so it takes no availability period as well " +
				"(`has both an important flag and a limited availability_period, which are mutually exclusive`)"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			refusal := ydbtopic.Check("", test.topic, test.spec, test.caps)
			c.Assert(refusal, qt.IsNotNil)
			c.Assert(*refusal, qt.DeepEquals, test.want)
		})
	}
}

func TestChangeRefusal_HappyPath(t *testing.T) {
	tests := []struct {
		name             string
		desired, current ydbtopic.Spec
	}{
		{name: "more partitions", desired: ydbtopic.Spec{MinActivePartitions: 3}, current: ydbtopic.Spec{MinActivePartitions: 2}},
		{name: "auto-partitioning paused", desired: ydbtopic.Spec{AutoPartitioningStrategy: "paused"},
			current: ydbtopic.Spec{AutoPartitioningStrategy: "scale_up"}},
		{name: "auto-partitioning enabled", desired: ydbtopic.Spec{AutoPartitioningStrategy: "scale_up"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.ChangeRefusal("", "events", test.desired, test.current), qt.IsNil)
		})
	}
}

func TestChangeRefusal_FailurePath(t *testing.T) {
	tests := []struct {
		name             string
		desired, current ydbtopic.Spec
		want             string
	}{
		{name: "fewer partitions", desired: ydbtopic.Spec{}, current: ydbtopic.Spec{MinActivePartitions: 2},
			want: "it has 2 partitions and is declared with 1, and YDB never removes a partition " +
				"(`Invalid total groups count specified`); drop the topic and create it again to lower the count"},
		{name: "auto-partitioning disabled", desired: ydbtopic.Spec{AutoPartitioningStrategy: "disabled"},
			current: ydbtopic.Spec{AutoPartitioningStrategy: "scale_up_and_down"},
			want: "its auto-partitioning is scale_up_and_down and is declared disabled, and YDB does not disable " +
				"auto-partitioning once it is on (`Can't disable auto partitioning.`); declare " +
				"auto_partitioning_strategy paused to stop it"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			refusal := ydbtopic.ChangeRefusal("", "events", test.desired, test.current)
			c.Assert(refusal, qt.IsNotNil)
			c.Assert(*refusal, qt.DeepEquals, ydbtopic.Refusal{Subject: "topic events", Reason: test.want})
		})
	}
}

// A rollback goes to the nearest topic YDB reaches in place: the partitions
// the forward change added stay, and auto-partitioning the forward change
// enabled is paused with the settings it ran with.
func TestRollbackTarget(t *testing.T) {
	enabled := ydbtopic.Spec{MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
		AutoPartitioningUpUtilizationPercent: 70}
	tests := []struct {
		name            string
		target, current ydbtopic.Spec
		want            ydbtopic.Spec
	}{
		{name: "a target YDB reaches", target: ydbtopic.Spec{RetentionPeriod: "PT2H"}, current: ydbtopic.Spec{},
			want: ydbtopic.Spec{RetentionPeriod: "PT2H"}},
		{name: "fewer partitions keep the current count", target: ydbtopic.Spec{MinActivePartitions: 1},
			current: ydbtopic.Spec{MinActivePartitions: 3}, want: ydbtopic.Spec{MinActivePartitions: 3}},
		{name: "auto-partitioning disabled is paused", target: ydbtopic.Spec{RetentionPeriod: "PT2H"}, current: enabled,
			want: ydbtopic.Spec{MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "paused",
				AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 30,
				AutoPartitioningStabilizationWindow: "PT5M", RetentionPeriod: "PT2H"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reached := ydbtopic.RollbackTarget(test.target, test.current)
			c.Assert(reached, qt.DeepEquals, test.want)
			c.Assert(ydbtopic.ChangeRefusal("", "events", reached, test.current), qt.IsNil)
		})
	}
}
