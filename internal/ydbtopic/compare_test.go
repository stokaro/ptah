package ydbtopic_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbtopic"
)

// A declaration resolves to what YDB gives a new topic for each setting it
// leaves out, measured on 25.1.4.7 and 26.2.1.14: the burst follows the write
// speed the topic is created with, and a topic created with a strategy splits
// at 90%, merges at 30% and waits five minutes.
func TestResolve(t *testing.T) {
	tests := []struct {
		name string
		spec ast.TopicSpec
		want ydbtopic.Settings
	}{
		{
			name: "nothing declared",
			want: ydbtopic.Settings{MinActivePartitions: 1, Strategy: "disabled", RetentionSeconds: 86400,
				WriteSpeed: 1048576, WriteBurst: 1048576},
		},
		{
			name: "a write speed without a burst",
			spec: ast.TopicSpec{PartitionWriteSpeedBytesPerSecond: 4194304},
			want: ydbtopic.Settings{MinActivePartitions: 1, Strategy: "disabled", RetentionSeconds: 86400,
				WriteSpeed: 4194304, WriteBurst: 4194304},
		},
		{
			name: "a strategy without thresholds or a maximum",
			spec: ast.TopicSpec{MinActivePartitions: 3, AutoPartitioningStrategy: "Scale_Up"},
			want: ydbtopic.Settings{MinActivePartitions: 3, Strategy: "scale_up", MaxActivePartitions: 3,
				UpUtilizationPercent: 90, DownUtilizationPercent: 30, StabilizationWindowSeconds: 300,
				RetentionSeconds: 86400, WriteSpeed: 1048576, WriteBurst: 1048576},
		},
		{
			name: "every setting",
			spec: ast.TopicSpec{MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "paused",
				AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
				AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "PT36H",
				PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
				SupportedCodecs: []string{"GZIP", "raw", "gzip"}},
			want: ydbtopic.Settings{MinActivePartitions: 2, Strategy: "paused", MaxActivePartitions: 6,
				UpUtilizationPercent: 70, DownUtilizationPercent: 10, StabilizationWindowSeconds: 120,
				RetentionSeconds: 129600, WriteSpeed: 2097152, WriteBurst: 3145728, Codecs: []string{"gzip", "raw"}},
		},
		{
			name: "the shaping settings of a disabled topic are none",
			spec: ast.TopicSpec{MaxActivePartitions: 5, AutoPartitioningUpUtilizationPercent: 70},
			want: ydbtopic.Settings{MinActivePartitions: 1, Strategy: "disabled", RetentionSeconds: 86400,
				WriteSpeed: 1048576, WriteBurst: 1048576},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.Resolve(test.spec), qt.DeepEquals, test.want)
		})
	}
}

// Two descriptions of one topic compare as YDB keeps it. Each row differs
// from the read of a topic created without settings in one setting, or in
// how it is spelled.
func TestSettingsEqual(t *testing.T) {
	read := ast.TopicSpec{MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "P1D",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576}
	tests := []struct {
		name     string
		declared ast.TopicSpec
		want     bool
	}{
		{name: "nothing declared", declared: ast.TopicSpec{}, want: true},
		{name: "each default named", declared: read, want: true},
		{name: "the retention spelled in hours", declared: ast.TopicSpec{RetentionPeriod: "PT24H"}, want: true},
		{name: "another retention", declared: ast.TopicSpec{RetentionPeriod: "PT25H"}, want: false},
		{name: "another partition count", declared: ast.TopicSpec{MinActivePartitions: 2}, want: false},
		{name: "a strategy", declared: ast.TopicSpec{AutoPartitioningStrategy: "paused"}, want: false},
		{name: "another write speed", declared: ast.TopicSpec{PartitionWriteSpeedBytesPerSecond: 2097152}, want: false},
		{name: "another burst", declared: ast.TopicSpec{PartitionWriteBurstBytes: 2097152}, want: false},
		{name: "codecs", declared: ast.TopicSpec{SupportedCodecs: []string{"raw"}}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.SettingsEqual(test.declared, read), qt.Equals, test.want)
			c.Assert(ydbtopic.Equal(test.declared, read), qt.Equals, test.want)
		})
	}
}

// The thresholds of an auto-partitioned topic compare even where both sides
// agree on the strategy: a topic given its strategy after it was created
// keeps 80% and 20%, and a declaration leaving the thresholds out asks for
// the 90% and 30% a topic created with a strategy gets.
func TestSettingsEqual_ThresholdsOfAnAutoPartitionedTopic(t *testing.T) {
	c := qt.New(t)
	given := ast.TopicSpec{MinActivePartitions: 1, MaxActivePartitions: 4, AutoPartitioningStrategy: "scale_up",
		AutoPartitioningUpUtilizationPercent: 80, AutoPartitioningDownUtilizationPercent: 20,
		AutoPartitioningStabilizationWindow: "PT5M", RetentionPeriod: "P1D",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576}
	declared := ast.TopicSpec{MaxActivePartitions: 4, AutoPartitioningStrategy: "scale_up"}

	c.Assert(ydbtopic.SettingsEqual(declared, given), qt.IsFalse)
	declared.AutoPartitioningUpUtilizationPercent, declared.AutoPartitioningDownUtilizationPercent = 80, 20
	c.Assert(ydbtopic.SettingsEqual(declared, given), qt.IsTrue)
}

func TestCompare(t *testing.T) {
	current := ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{
		{Name: "gone"},
		{Name: "kept", Important: true},
		{Name: "same", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}},
		{Name: "narrowed", SupportedCodecs: []string{"raw"}},
		{Name: "widened"},
	}}
	desired := ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{
		{Name: "fresh"},
		{Name: "kept"},
		{Name: "same", ReadFrom: "2026-01-01T03:00:00+03:00", SupportedCodecs: []string{"GZIP", "raw"}},
		{Name: "narrowed"},
		{Name: "widened", SupportedCodecs: []string{"zstd"}},
	}}
	c := qt.New(t)

	changes := ydbtopic.Compare(desired, current)

	c.Assert(changes, qt.DeepEquals, ydbtopic.ConsumerChanges{
		Added:     []string{"fresh"},
		Removed:   []string{"gone"},
		Changed:   []string{"kept", "widened"},
		Restarted: []string{"narrowed"},
	})
	c.Assert(changes.Consumers(), qt.DeepEquals, []string{"fresh", "gone", "kept", "widened", "narrowed"})
	c.Assert(ydbtopic.Equal(desired, current), qt.IsFalse)
}

// A consumer compares as YDB keeps it: a read_from at the epoch is none, and
// an availability period compares by the seconds it denotes.
func TestConsumerEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b ast.TopicConsumerSpec
		want bool
	}{
		{name: "read_from at the epoch", a: ast.TopicConsumerSpec{Name: "c"},
			b: ast.TopicConsumerSpec{Name: "c", ReadFrom: "1970-01-01T00:00:00Z"}, want: true},
		{name: "one period in two spellings", a: ast.TopicConsumerSpec{Name: "c", AvailabilityPeriod: "PT48H"},
			b: ast.TopicConsumerSpec{Name: "c", AvailabilityPeriod: "P2D"}, want: true},
		{name: "another period", a: ast.TopicConsumerSpec{Name: "c", AvailabilityPeriod: "PT1H"},
			b: ast.TopicConsumerSpec{Name: "c"}, want: false},
		{name: "important", a: ast.TopicConsumerSpec{Name: "c", Important: true},
			b: ast.TopicConsumerSpec{Name: "c"}, want: false},
		{name: "another read_from", a: ast.TopicConsumerSpec{Name: "c", ReadFrom: "2026-01-01T00:00:01Z"},
			b: ast.TopicConsumerSpec{Name: "c", ReadFrom: "2026-01-01T00:00:00Z"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.ConsumerEqual(test.a, test.b), qt.Equals, test.want)
		})
	}
}
