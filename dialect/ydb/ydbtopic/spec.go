package ydbtopic

// Spec is a YDB topic's settings and its consumers: what `CREATE TOPIC
// <path> (CONSUMER ...) WITH (...)` takes. A topic is a persistent message
// queue at a path of YDB's scheme tree, and each consumer keeps its own
// position in it.
//
// Each setting field names the `WITH` option it carries, and a field left at
// its zero value declares nothing: a setting nobody declared is the value YDB
// gives a new topic. [Resolve] reads both sides of a comparison that way, so
// a spec naming a setting at its default and one leaving it out describe the
// same topic. The intervals are ISO 8601 durations
// YDB keeps in whole seconds.
type Spec struct {
	// MinActivePartitions is `min_active_partitions`, the number of
	// partitions writers write to. YDB adds partitions and never removes
	// one, so the count only grows.
	MinActivePartitions uint64 `json:"min_active_partitions,omitempty"`
	// MaxActivePartitions is `max_active_partitions`, the most partitions
	// auto-partitioning splits the topic into. It has a meaning only while
	// AutoPartitioningStrategy is not `disabled`.
	MaxActivePartitions uint64 `json:"max_active_partitions,omitempty"`
	// AutoPartitioningStrategy is `auto_partitioning_strategy`: `disabled`,
	// `scale_up`, `scale_up_and_down` or `paused`.
	AutoPartitioningStrategy string `json:"auto_partitioning_strategy,omitempty"`
	// AutoPartitioningUpUtilizationPercent is
	// `auto_partitioning_up_utilization_percent`, the share of a partition's
	// write speed above which it splits.
	AutoPartitioningUpUtilizationPercent uint32 `json:"auto_partitioning_up_utilization_percent,omitempty"`
	// AutoPartitioningDownUtilizationPercent is
	// `auto_partitioning_down_utilization_percent`, the share below which
	// partitions merge.
	AutoPartitioningDownUtilizationPercent uint32 `json:"auto_partitioning_down_utilization_percent,omitempty"`
	// AutoPartitioningStabilizationWindow is
	// `auto_partitioning_stabilization_window`, how long a load has to last
	// before the partition count follows it.
	AutoPartitioningStabilizationWindow string `json:"auto_partitioning_stabilization_window,omitempty"`
	// RetentionPeriod is `retention_period`, how long the topic keeps a
	// message whether or not it was read.
	RetentionPeriod string `json:"retention_period,omitempty"`
	// PartitionWriteSpeedBytesPerSecond is
	// `partition_write_speed_bytes_per_second`, the write quota of one
	// partition.
	PartitionWriteSpeedBytesPerSecond uint64 `json:"partition_write_speed_bytes_per_second,omitempty"`
	// PartitionWriteBurstBytes is `partition_write_burst_bytes`, the burst a
	// partition takes above its quota.
	PartitionWriteBurstBytes uint64 `json:"partition_write_burst_bytes,omitempty"`
	// SupportedCodecs is `supported_codecs`, the codecs a writer may use:
	// raw, gzip, lzop, zstd and custom. Empty takes any.
	SupportedCodecs []string `json:"supported_codecs,omitempty"`
	// Consumers are the topic's consumers. Each keeps its position in the
	// topic, which dropping the consumer loses.
	Consumers []ConsumerSpec `json:"consumers,omitempty"`
}

// Clone returns an independent copy, so a spec handed to a comparator or a
// planner cannot be changed through the lists it shares with the schema it
// came from.
func (s Spec) Clone() Spec {
	out := s
	if s.SupportedCodecs != nil {
		out.SupportedCodecs = append([]string(nil), s.SupportedCodecs...)
	}
	if s.Consumers != nil {
		out.Consumers = make([]ConsumerSpec, len(s.Consumers))
		for i, consumer := range s.Consumers {
			out.Consumers[i] = consumer.Clone()
		}
	}
	return out
}

// ConsumerSpec is one consumer of a YDB topic, a changefeed's topic
// included: a named reader that keeps its own position in the topic. The settings are the ones `ALTER TOPIC ...
// ADD CONSUMER ... WITH (...)` takes, and a field left at its zero value
// declares nothing.
type ConsumerSpec struct {
	// Name is the consumer's name, unique within its topic.
	Name string `json:"name"`
	// Important is `important`: the topic keeps a record this consumer has
	// not read even after the retention period.
	Important bool `json:"important,omitempty"`
	// ReadFrom is `read_from`, an RFC 3339 time: a partition this consumer
	// has not read yet is read from the first record written at or after
	// it. Empty reads from the beginning.
	ReadFrom string `json:"read_from,omitempty"`
	// SupportedCodecs is `supported_codecs`, the codecs this consumer can
	// read: raw, gzip, lzop, zstd and custom. Empty takes any.
	SupportedCodecs []string `json:"supported_codecs,omitempty"`
	// AvailabilityPeriod is `availability_period`, an ISO 8601 interval for
	// which the topic keeps a record this consumer has not read beyond the
	// retention period. Empty sets none.
	AvailabilityPeriod string `json:"availability_period,omitempty"`
}

// Clone returns an independent copy of the consumer.
func (c ConsumerSpec) Clone() ConsumerSpec {
	out := c
	if c.SupportedCodecs != nil {
		out.SupportedCodecs = append([]string(nil), c.SupportedCodecs...)
	}
	return out
}
