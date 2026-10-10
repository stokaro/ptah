package ydbtopic_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbtopic"
)

func TestParseTopic_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ydbtopic.Spec
	}{
		{name: "nothing declared", values: map[string]string{"name": "events"}, want: ydbtopic.Spec{}},
		{
			name: "every setting",
			values: map[string]string{
				"min_active_partitions": "2", "max_active_partitions": "6", "auto_partitioning_strategy": "Scale_Up",
				"auto_partitioning_up_utilization_percent": "70", "auto_partitioning_down_utilization_percent": "10",
				"auto_partitioning_stabilization_window": "PT2M", "retention_period": "P1DT12H",
				"partition_write_speed_bytes_per_second": "2097152", "partition_write_burst_bytes": "3145728",
				"supported_codecs": " RAW, gzip ,custom",
			},
			want: ydbtopic.Spec{
				MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
				AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
				AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "P1DT12H",
				PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
				SupportedCodecs: []string{"raw", "gzip", "custom"},
			},
		},
		{
			name:   "paused keeps the shaping settings",
			values: map[string]string{"auto_partitioning_strategy": "paused", "max_active_partitions": "3"},
			want:   ydbtopic.Spec{AutoPartitioningStrategy: "paused", MaxActivePartitions: 3},
		},
		{
			name:   "a maximum equal to the minimum",
			values: map[string]string{"auto_partitioning_strategy": "scale_up_and_down", "min_active_partitions": "3", "max_active_partitions": "3"},
			want:   ydbtopic.Spec{AutoPartitioningStrategy: "scale_up_and_down", MinActivePartitions: 3, MaxActivePartitions: 3},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec, err := ydbtopic.ParseTopic(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(spec, qt.DeepEquals, test.want)
		})
	}
}

func TestParseTopic_FailurePath(t *testing.T) {
	const shaping = "it shapes auto-partitioning, which this topic does not enable; declare " +
		"auto_partitioning_strategy as scale_up, scale_up_and_down or paused"
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{name: "a zero count", values: map[string]string{"min_active_partitions": "0"},
			wantErr: `invalid min_active_partitions "0": takes a whole number above zero`},
		{name: "a count that is no number", values: map[string]string{"partition_write_burst_bytes": "1MB"},
			wantErr: `invalid partition_write_burst_bytes "1MB": takes a whole number above zero`},
		{name: "an unknown strategy", values: map[string]string{"auto_partitioning_strategy": "up"},
			wantErr: `invalid auto_partitioning_strategy "up": takes one of disabled, scale_up, scale_up_and_down, paused`},
		{name: "a percentage over 100", values: map[string]string{"auto_partitioning_strategy": "scale_up", "auto_partitioning_up_utilization_percent": "101"},
			wantErr: `invalid auto_partitioning_up_utilization_percent "101": takes a whole percentage from 1 to 100`},
		{name: "a fraction of a second", values: map[string]string{"retention_period": "PT1.5S"},
			wantErr: `invalid retention_period "PT1.5S": YDB keeps whole seconds, and drops the fraction of this one`},
		{name: "no time at all", values: map[string]string{"retention_period": "PT0S"},
			wantErr: `invalid retention_period "PT0S": is no positive time, and YDB takes only a positive interval`},
		{name: "not a duration", values: map[string]string{"retention_period": "36h"},
			wantErr: `invalid retention_period "36h": takes an ISO 8601 duration such as PT12H or P1D`},
		{name: "a codec YDB keeps as no list", values: map[string]string{"supported_codecs": "raw,kafka_batch"},
			wantErr: `invalid supported_codecs "raw,kafka_batch": takes a comma-separated list of raw, gzip, lzop, zstd, custom; ` +
				`YDB keeps a list naming any other codec as no list at all`},
		{name: "a codec named twice", values: map[string]string{"supported_codecs": "raw,RAW"},
			wantErr: `invalid supported_codecs "raw,RAW": names codec raw twice`},
		{name: "a maximum without a strategy", values: map[string]string{"max_active_partitions": "5"},
			wantErr: "invalid max_active_partitions: " + shaping},
		{name: "a threshold with the strategy disabled", values: map[string]string{"auto_partitioning_strategy": "disabled", "auto_partitioning_down_utilization_percent": "10"},
			wantErr: "invalid auto_partitioning_down_utilization_percent: " + shaping},
		{name: "a window without a strategy", values: map[string]string{"auto_partitioning_stabilization_window": "PT1M"},
			wantErr: "invalid auto_partitioning_stabilization_window: " + shaping},
		{name: "a maximum below the minimum", values: map[string]string{"auto_partitioning_strategy": "scale_up", "min_active_partitions": "3", "max_active_partitions": "2"},
			wantErr: `invalid max_active_partitions "2": it is below min_active_partitions, 3, which YDB refuses (` + "`Invalid total partition count specified`)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec, err := ydbtopic.ParseTopic(test.values)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(spec, qt.DeepEquals, ydbtopic.Spec{})
		})
	}
}

func TestParseConsumer_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ydbtopic.ConsumerSpec
	}{
		{name: "a name alone", values: map[string]string{"name": " billing ", "topic": "events"},
			want: ydbtopic.ConsumerSpec{Name: "billing"}},
		{name: "important", values: map[string]string{"name": "billing", "important": "TRUE"},
			want: ydbtopic.ConsumerSpec{Name: "billing", Important: true}},
		{name: "a read_from in another zone", values: map[string]string{"name": "audit", "read_from": "2026-01-01T03:00:00+03:00"},
			want: ydbtopic.ConsumerSpec{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z"}},
		{name: "read_from at the epoch is no read_from", values: map[string]string{"name": "audit", "read_from": "1970-01-01T00:00:00Z"},
			want: ydbtopic.ConsumerSpec{Name: "audit"}},
		{name: "codecs and an availability period", values: map[string]string{"name": "audit", "supported_codecs": "zstd,raw", "availability_period": "PT2H"},
			want: ydbtopic.ConsumerSpec{Name: "audit", SupportedCodecs: []string{"zstd", "raw"}, AvailabilityPeriod: "PT2H"}},
		{name: "codecs in lower case", values: map[string]string{"name": "audit", "supported_codecs": "RAW, gzip"},
			want: ydbtopic.ConsumerSpec{Name: "audit", SupportedCodecs: []string{"raw", "gzip"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			consumer, err := ydbtopic.ParseConsumer(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(consumer, qt.DeepEquals, test.want)
		})
	}
}

func TestParseConsumer_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{name: "no name", values: map[string]string{"name": " "}, wantErr: "invalid name: a consumer needs a name"},
		{name: "a slash in the name", values: map[string]string{"name": "a/b"},
			wantErr: `invalid name "a/b": a consumer's name cannot hold a slash`},
		{name: "important that is no boolean", values: map[string]string{"name": "c", "important": "yes"},
			wantErr: `invalid important "yes": takes true or false`},
		{name: "a read_from that is no time", values: map[string]string{"name": "c", "read_from": "2026-01-01"},
			wantErr: `invalid read_from "2026-01-01": takes an RFC 3339 time such as 2026-01-01T00:00:00Z`},
		{name: "a read_from with a fraction", values: map[string]string{"name": "c", "read_from": "2026-01-01T00:00:00.5Z"},
			wantErr: `invalid read_from "2026-01-01T00:00:00.5Z": YDB keeps whole seconds, and drops the fraction of this one`},
		{name: "an unknown codec", values: map[string]string{"name": "c", "supported_codecs": "snappy"},
			wantErr: `invalid supported_codecs "snappy": takes a comma-separated list of raw, gzip, lzop, zstd, custom; ` +
				`YDB keeps a list naming any other codec as no list at all`},
		{name: "a codec twice", values: map[string]string{"name": "c", "supported_codecs": "raw,RAW"},
			wantErr: `invalid supported_codecs "raw,RAW": names codec raw twice`},
		{name: "important with an availability period", values: map[string]string{"name": "c", "important": "true", "availability_period": "PT1H"},
			wantErr: `invalid availability_period "PT1H": YDB keeps every unread record for an important consumer, so it takes no ` +
				"availability period as well (`has both an important flag and a limited availability_period, which are mutually exclusive`)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			consumer, err := ydbtopic.ParseConsumer(test.values)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(consumer, qt.DeepEquals, ydbtopic.ConsumerSpec{})
		})
	}
}

func TestSeconds_HappyPath(t *testing.T) {
	tests := []struct {
		text string
		want uint64
	}{
		{text: "PT1S", want: 1},
		{text: "PT36H", want: 129600},
		{text: "P1DT12H", want: 129600},
		{text: "P1W", want: 604800},
		{text: " PT2M ", want: 120},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			seconds, err := ydbtopic.Seconds(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(seconds, qt.Equals, test.want)
			c.Assert(ydbtopic.FormatSeconds(seconds), qt.Not(qt.Equals), "")
		})
	}
}

func TestSeconds_FailurePath(t *testing.T) {
	tests := []struct {
		text    string
		wantErr string
	}{
		{text: "-PT1S", wantErr: "is no positive time, and YDB takes only a positive interval"},
		{text: "PT0.5S", wantErr: "YDB keeps whole seconds, and drops the fraction of this one"},
		{text: "P1M", wantErr: "takes an ISO 8601 duration such as PT12H or P1D"},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			seconds, err := ydbtopic.Seconds(test.text)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(seconds, qt.Equals, uint64(0))
		})
	}
}

// The codec numbers are the ones DescribeTopic reported for each codec a
// statement named, measured on 25.1.4.7 and 26.2.1.14.
func TestCodecName(t *testing.T) {
	tests := []struct {
		number int32
		name   string
		known  bool
	}{
		{number: 1, name: "raw", known: true},
		{number: 2, name: "gzip", known: true},
		{number: 3, name: "lzop", known: true},
		{number: 4, name: "zstd", known: true},
		{number: 10000, name: "custom", known: true},
		{number: 5, name: "", known: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			name, known := ydbtopic.CodecName(test.number)
			c.Assert(name, qt.Equals, test.name)
			c.Assert(known, qt.Equals, test.known)
		})
	}
}
