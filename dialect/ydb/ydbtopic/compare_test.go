package ydbtopic_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbtopic"
)

// A declaration resolves to what YDB gives a new topic for each setting it
// leaves out, measured on 25.1.4.7 and 26.2.1.14: the burst follows the write
// speed the topic is created with, and a topic created with a strategy splits
// at 90%, merges at 30% and waits five minutes.
func TestResolve(t *testing.T) {
	tests := []struct {
		name string
		spec ydbtopic.Spec
		want ydbtopic.Settings
	}{
		{
			name: "nothing declared",
			want: ydbtopic.Settings{MinActivePartitions: 1, Strategy: "disabled", RetentionSeconds: 86400,
				WriteSpeed: 1048576, WriteBurst: 1048576},
		},
		{
			name: "a write speed without a burst",
			spec: ydbtopic.Spec{PartitionWriteSpeedBytesPerSecond: 4194304},
			want: ydbtopic.Settings{MinActivePartitions: 1, Strategy: "disabled", RetentionSeconds: 86400,
				WriteSpeed: 4194304, WriteBurst: 4194304},
		},
		{
			name: "a strategy without thresholds or a maximum",
			spec: ydbtopic.Spec{MinActivePartitions: 3, AutoPartitioningStrategy: "Scale_Up"},
			want: ydbtopic.Settings{MinActivePartitions: 3, Strategy: "scale_up", MaxActivePartitions: 3,
				UpUtilizationPercent: 90, DownUtilizationPercent: 30, StabilizationWindowSeconds: 300,
				RetentionSeconds: 86400, WriteSpeed: 1048576, WriteBurst: 1048576},
		},
		{
			name: "every setting",
			spec: ydbtopic.Spec{MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "paused",
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
			spec: ydbtopic.Spec{MaxActivePartitions: 5, AutoPartitioningUpUtilizationPercent: 70},
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
	read := ydbtopic.Spec{MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "P1D",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576}
	tests := []struct {
		name     string
		declared ydbtopic.Spec
		want     bool
	}{
		{name: "nothing declared", declared: ydbtopic.Spec{}, want: true},
		{name: "each default named", declared: read, want: true},
		{name: "the retention spelled in hours", declared: ydbtopic.Spec{RetentionPeriod: "PT24H"}, want: true},
		{name: "another retention", declared: ydbtopic.Spec{RetentionPeriod: "PT25H"}, want: false},
		{name: "another partition count", declared: ydbtopic.Spec{MinActivePartitions: 2}, want: false},
		{name: "a strategy", declared: ydbtopic.Spec{AutoPartitioningStrategy: "paused"}, want: false},
		{name: "another write speed", declared: ydbtopic.Spec{PartitionWriteSpeedBytesPerSecond: 2097152}, want: false},
		{name: "another burst", declared: ydbtopic.Spec{PartitionWriteBurstBytes: 2097152}, want: false},
		{name: "codecs", declared: ydbtopic.Spec{SupportedCodecs: []string{"raw"}}, want: false},
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
	given := ydbtopic.Spec{MinActivePartitions: 1, MaxActivePartitions: 4, AutoPartitioningStrategy: "scale_up",
		AutoPartitioningUpUtilizationPercent: 80, AutoPartitioningDownUtilizationPercent: 20,
		AutoPartitioningStabilizationWindow: "PT5M", RetentionPeriod: "P1D",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576}
	declared := ydbtopic.Spec{MaxActivePartitions: 4, AutoPartitioningStrategy: "scale_up"}

	c.Assert(ydbtopic.SettingsEqual(declared, given), qt.IsFalse)
	declared.AutoPartitioningUpUtilizationPercent, declared.AutoPartitioningDownUtilizationPercent = 80, 20
	c.Assert(ydbtopic.SettingsEqual(declared, given), qt.IsTrue)
}

func TestCompare(t *testing.T) {
	current := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{
		{Name: "gone"},
		{Name: "kept", Important: true},
		{Name: "same", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}},
		{Name: "narrowed", SupportedCodecs: []string{"raw"}},
		{Name: "widened"},
	}}
	desired := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{
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
		a, b ydbtopic.ConsumerSpec
		want bool
	}{
		{name: "read_from at the epoch", a: ydbtopic.ConsumerSpec{Name: "c"},
			b: ydbtopic.ConsumerSpec{Name: "c", ReadFrom: "1970-01-01T00:00:00Z"}, want: true},
		{name: "one period in two spellings", a: ydbtopic.ConsumerSpec{Name: "c", AvailabilityPeriod: "PT48H"},
			b: ydbtopic.ConsumerSpec{Name: "c", AvailabilityPeriod: "P2D"}, want: true},
		{name: "another period", a: ydbtopic.ConsumerSpec{Name: "c", AvailabilityPeriod: "PT1H"},
			b: ydbtopic.ConsumerSpec{Name: "c"}, want: false},
		{name: "important", a: ydbtopic.ConsumerSpec{Name: "c", Important: true},
			b: ydbtopic.ConsumerSpec{Name: "c"}, want: false},
		{name: "another read_from", a: ydbtopic.ConsumerSpec{Name: "c", ReadFrom: "2026-01-01T00:00:01Z"},
			b: ydbtopic.ConsumerSpec{Name: "c", ReadFrom: "2026-01-01T00:00:00Z"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.ConsumerEqual(test.a, test.b), qt.Equals, test.want)
		})
	}
}
