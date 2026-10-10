package ydbtopic_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbtopic"
)

func TestCreateStatement(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		topic  string
		spec   ydbtopic.Spec
		want   string
	}{
		{name: "nothing declared", topic: "events", want: "CREATE TOPIC `events`;"},
		{name: "in a directory", schema: "app", topic: "events", want: "CREATE TOPIC `app/events`;"},
		{name: "a dotted name in a directory", schema: "app", topic: "order.events", want: "CREATE TOPIC `app/order.events`;"},
		{
			name:  "consumers and settings",
			topic: "events",
			spec: ydbtopic.Spec{
				MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
				AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
				AutoPartitioningStabilizationWindow: "PT120S", RetentionPeriod: "PT36H",
				PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
				SupportedCodecs: []string{"RAW", "gzip"},
				Consumers: []ydbtopic.ConsumerSpec{
					{Name: "billing", Important: true},
					{Name: "it's", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw"}, AvailabilityPeriod: "P2D"},
					{Name: "plain"},
				},
			},
			want: "CREATE TOPIC `events` (CONSUMER `billing` WITH (important = TRUE), " +
				"CONSUMER `it's` WITH (read_from = Timestamp('2026-01-01T00:00:00Z'), supported_codecs = 'raw', " +
				"availability_period = Interval('P2D')), CONSUMER `plain`) WITH (min_active_partitions = 2, " +
				"max_active_partitions = 6, auto_partitioning_strategy = 'scale_up', " +
				"auto_partitioning_up_utilization_percent = 70, auto_partitioning_down_utilization_percent = 10, " +
				"auto_partitioning_stabilization_window = Interval('PT2M'), retention_period = Interval('P1DT12H'), " +
				"partition_write_speed_bytes_per_second = 2097152, partition_write_burst_bytes = 3145728, " +
				"supported_codecs = 'raw,gzip');",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.CreateStatement(test.schema, test.topic, test.spec), qt.Equals, test.want)
		})
	}
}

func TestDropStatement(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbtopic.DropStatement("app", "events"), qt.Equals, "DROP TOPIC `app/events`;")
}

// A change names every setting of the topic once any of them differs,
// because a setting the statement leaves out keeps the value the topic was
// made with rather than the one the declaration resolves to. Each row changes
// one setting whose neighbor a statement naming only the change would leave
// behind.
func TestAlterStatements_NamesEverySetting(t *testing.T) {
	created := ydbtopic.Spec{MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "P1D",
		PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 2097152}
	tests := []struct {
		name     string
		declared ydbtopic.Spec
		want     string
	}{
		{
			name:     "a write speed changed alone keeps the burst following it",
			declared: ydbtopic.Spec{PartitionWriteSpeedBytesPerSecond: 4194304},
			want: "ALTER TOPIC `events` SET (min_active_partitions = 1, auto_partitioning_strategy = 'disabled', " +
				"retention_period = Interval('P1D'), partition_write_speed_bytes_per_second = 4194304, " +
				"partition_write_burst_bytes = 4194304, supported_codecs = '');",
		},
		{
			name:     "a strategy given later names the thresholds a new topic takes",
			declared: ydbtopic.Spec{PartitionWriteSpeedBytesPerSecond: 2097152, AutoPartitioningStrategy: "scale_up", MaxActivePartitions: 4},
			want: "ALTER TOPIC `events` SET (min_active_partitions = 1, auto_partitioning_strategy = 'scale_up', " +
				"max_active_partitions = 4, auto_partitioning_up_utilization_percent = 90, " +
				"auto_partitioning_down_utilization_percent = 30, auto_partitioning_stabilization_window = Interval('PT5M'), " +
				"retention_period = Interval('P1D'), partition_write_speed_bytes_per_second = 2097152, " +
				"partition_write_burst_bytes = 2097152, supported_codecs = '');",
		},
		{
			name:     "codecs taken away name the empty list",
			declared: ydbtopic.Spec{PartitionWriteSpeedBytesPerSecond: 2097152, RetentionPeriod: "PT2H"},
			want: "ALTER TOPIC `events` SET (min_active_partitions = 1, auto_partitioning_strategy = 'disabled', " +
				"retention_period = Interval('PT2H'), partition_write_speed_bytes_per_second = 2097152, " +
				"partition_write_burst_bytes = 2097152, supported_codecs = '');",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.AlterStatements("", "events", test.declared, created), qt.DeepEquals, []string{test.want})
		})
	}
}

// Consumers change in the statement that changes the settings: dropped ones
// first, then the ones changed in place, then the added ones. A consumer YDB
// changes only by dropping it is dropped there and added again by a second
// statement, since one ALTER TOPIC naming a consumer twice is refused.
func TestAlterStatements_Consumers(t *testing.T) {
	current := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{
		{Name: "gone"},
		{Name: "kept", Important: true},
		{Name: "dated", AvailabilityPeriod: "PT2H"},
		{Name: "narrowed", SupportedCodecs: []string{"raw"}},
	}}
	desired := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{
		{Name: "kept", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"zstd"}},
		{Name: "dated"},
		{Name: "narrowed", Important: true},
		{Name: "fresh", Important: true},
	}}
	c := qt.New(t)

	statements := ydbtopic.AlterStatements("", "events", desired, current)

	c.Assert(statements, qt.DeepEquals, []string{
		"ALTER TOPIC `events` DROP CONSUMER `gone`, DROP CONSUMER `narrowed`, " +
			"ALTER CONSUMER `kept` SET (important = FALSE, read_from = Timestamp('2026-01-01T00:00:00Z'), supported_codecs = 'zstd'), " +
			"ALTER CONSUMER `dated` SET (important = FALSE, read_from = Timestamp('1970-01-01T00:00:00Z'), " +
			"availability_period = Interval('PT0S')), " +
			"ADD CONSUMER `fresh` WITH (important = TRUE);",
		"ALTER TOPIC `events` ADD CONSUMER `narrowed` WITH (important = TRUE);",
	})
}

func TestAlterStatements_NothingToChange(t *testing.T) {
	c := qt.New(t)
	read := ydbtopic.Spec{MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "P1D",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576,
		Consumers: []ydbtopic.ConsumerSpec{{Name: "c"}}}
	c.Assert(ydbtopic.AlterStatements("", "events", ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c"}}}, read), qt.IsNil)
}
