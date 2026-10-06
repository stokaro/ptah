package ast

// TopicSpec is a YDB topic's settings and its consumers: what `CREATE TOPIC
// <path> (CONSUMER ...) WITH (...)` takes. A topic is a persistent message
// queue at a path of YDB's scheme tree, and each consumer keeps its own
// position in it.
//
// Each setting field names the `WITH` option it carries, and a field left at
// its zero value declares nothing: a setting nobody declared is the value YDB
// gives a new topic. [ptah.run/internal/ydbtopic] reads both sides of a
// comparison that way, so a spec naming a setting at its default and one
// leaving it out describe the same topic. The intervals are ISO 8601 durations
// YDB keeps in whole seconds.
type TopicSpec struct {
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
	Consumers []TopicConsumerSpec `json:"consumers,omitempty"`
}

// Clone returns an independent copy, so a spec handed to a comparator or a
// planner cannot be changed through the lists it shares with the schema it
// came from.
func (s TopicSpec) Clone() TopicSpec {
	out := s
	if s.SupportedCodecs != nil {
		out.SupportedCodecs = append([]string(nil), s.SupportedCodecs...)
	}
	if s.Consumers != nil {
		out.Consumers = make([]TopicConsumerSpec, len(s.Consumers))
		for i, consumer := range s.Consumers {
			out.Consumers[i] = consumer.Clone()
		}
	}
	return out
}

// CreateTopicNode creates a YDB topic with its consumers and settings: `CREATE
// TOPIC <path> (CONSUMER ...) WITH (...)`. The YDB renderer writes it; every
// other renderer refuses it, because no other engine has a topic Ptah models.
type CreateTopicNode struct {
	// Name is the topic's canonical reference: the directory and the name,
	// as [ptah.run/internal/tableref.Canonical] joins a table's.
	Name string
	// Spec is the topic's settings and consumers.
	Spec TopicSpec
}

// NewCreateTopic creates a CREATE TOPIC node for the topic name carrying spec.
func NewCreateTopic(name string, spec TopicSpec) *CreateTopicNode {
	return &CreateTopicNode{Name: name, Spec: spec.Clone()}
}

// Accept implements the Node interface for CreateTopicNode.
func (n *CreateTopicNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// AlterTopicNode changes a YDB topic in place: its settings, and its
// consumers, added, changed and dropped, through `ALTER TOPIC`. The YDB
// renderer writes it; every other renderer refuses it.
//
// It carries the topic before the change as well as after it, because the
// statement is the difference between the two: a consumer only Previous names
// is dropped, and a setting the declaration leaves out is set to the value YDB
// gives a new topic rather than reset, since `RESET` changes nothing on
// 26.2.1.14 and is a parse error on 25.1.4.7.
type AlterTopicNode struct {
	// Name is the topic's canonical reference.
	Name string
	// Spec is the topic as it is to be.
	Spec TopicSpec
	// Previous is the topic as the database holds it.
	Previous TopicSpec
}

// NewAlterTopic creates an ALTER TOPIC node that moves the topic name from
// previous to spec.
func NewAlterTopic(name string, spec, previous TopicSpec) *AlterTopicNode {
	return &AlterTopicNode{Name: name, Spec: spec.Clone(), Previous: previous.Clone()}
}

// Accept implements the Node interface for AlterTopicNode.
func (n *AlterTopicNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropTopicNode drops a YDB topic with every message it holds and every
// consumer's position in it: `DROP TOPIC <path>`. The YDB renderer writes it;
// every other renderer refuses it.
type DropTopicNode struct {
	// Name is the topic's canonical reference.
	Name string
}

// NewDropTopic creates a DROP TOPIC node for the topic name.
func NewDropTopic(name string) *DropTopicNode {
	return &DropTopicNode{Name: name}
}

// Accept implements the Node interface for DropTopicNode.
func (n *DropTopicNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// AddTopicConsumerNode adds one consumer to a YDB topic, including a
// changefeed's topic. It leaves the topic's settings and other consumers alone.
type AddTopicConsumerNode struct {
	// Name is the topic's canonical reference.
	Name string
	// Consumer is the consumer to add.
	Consumer TopicConsumerSpec
}

// NewAddTopicConsumer creates an ADD CONSUMER node with an independent spec.
func NewAddTopicConsumer(name string, consumer TopicConsumerSpec) *AddTopicConsumerNode {
	return &AddTopicConsumerNode{Name: name, Consumer: consumer.Clone()}
}

// Accept implements the Node interface for AddTopicConsumerNode.
func (n *AddTopicConsumerNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
