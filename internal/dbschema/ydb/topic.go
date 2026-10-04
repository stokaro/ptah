package ydb

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/internal/ydbtopic"
)

// topic adds one described topic.
func (r *Reader) topic(ctx context.Context, source Source, schema, name string, db *catalog.Database) error {
	path := r.absolute(schema, name)
	described, err := source.DescribeTopic(ctx, path)
	if err != nil {
		return err
	}
	spec, err := decodeTopic(described)
	if err != nil {
		return fmt.Errorf("YDB topic %s: %w", path, err)
	}
	db.Topics = append(db.Topics, catalog.Topic{Name: name, Schema: schema, Spec: spec})
	return nil
}

// decodeTopic reads a topic's description as the settings and consumers it
// holds.
//
// A field the pinned protocol buffers do not know is refused by number, and
// so is a setting Ptah does not model that the topic holds at another value
// than a topic Ptah creates: read as absent, it would be left out of every
// declaration made from the read, and a topic created from that declaration
// would differ in silence. Measured on 25.1.4.7 and 26.2.1.14, a topic YQL
// creates holds none of them -- YQL keeps no storage limit, no partition count
// limit and no metering mode outside a serverless database -- so only a topic
// another tool made, or one in a serverless database, is refused. The
// attributes are left out: YDB's own begin with an underscore and change by
// line (`_timestamp_type` on 26.2.1.14, `__max_partition_message_groups_
// seqno_stored` on 25.1.4.7), and no statement Ptah writes changes one.
func decodeTopic(described *Ydb_Topic.DescribeTopicResult) (ast.TopicSpec, error) {
	if err := refuseUnknownFields("its description", described); err != nil {
		return ast.TopicSpec{}, err
	}
	if err := refuseUnmodeledTopicSettings(described); err != nil {
		return ast.TopicSpec{}, err
	}
	var spec ast.TopicSpec
	partitioning := described.GetPartitioningSettings()
	auto := partitioning.GetAutoPartitioningSettings()
	for subject, message := range map[string]protoreflect.ProtoMessage{
		"its partitioning":                       partitioning,
		"its auto-partitioning":                  auto,
		"its auto-partitioning write speed rule": auto.GetPartitionWriteSpeed(),
	} {
		if err := refuseUnknownFields(subject, message); err != nil {
			return ast.TopicSpec{}, err
		}
	}
	spec.MinActivePartitions = uint64(max(partitioning.GetMinActivePartitions(), 0))
	strategy, err := topicStrategy(auto.GetStrategy())
	if err != nil {
		return ast.TopicSpec{}, err
	}
	spec.AutoPartitioningStrategy = strategy
	if strategy != ydbtopic.StrategyDisabled {
		speed := auto.GetPartitionWriteSpeed()
		spec.MaxActivePartitions = uint64(max(partitioning.GetMaxActivePartitions(), 0))
		spec.AutoPartitioningUpUtilizationPercent = uint32(max(speed.GetUpUtilizationPercent(), 0))
		spec.AutoPartitioningDownUtilizationPercent = uint32(max(speed.GetDownUtilizationPercent(), 0))
		if spec.AutoPartitioningStabilizationWindow, err = topicInterval("its stabilization window",
			speed.GetStabilizationWindow()); err != nil {
			return ast.TopicSpec{}, err
		}
	}
	if spec.RetentionPeriod, err = topicInterval("its retention period", described.GetRetentionPeriod()); err != nil {
		return ast.TopicSpec{}, err
	}
	spec.PartitionWriteSpeedBytesPerSecond = uint64(max(described.GetPartitionWriteSpeedBytesPerSecond(), 0))
	spec.PartitionWriteBurstBytes = uint64(max(described.GetPartitionWriteBurstBytes(), 0))
	if spec.SupportedCodecs, err = topicCodecs("its", described.GetSupportedCodecs()); err != nil {
		return ast.TopicSpec{}, err
	}
	for _, consumer := range described.GetConsumers() {
		decoded, err := decodeTopicConsumer(consumer)
		if err != nil {
			return ast.TopicSpec{}, fmt.Errorf("consumer %q: %w", consumer.GetName(), err)
		}
		spec.Consumers = append(spec.Consumers, decoded)
	}
	return spec, nil
}

// refuseUnmodeledTopicSettings refuses a topic holding a setting Ptah does
// not model at another value than a topic YQL creates holds.
func refuseUnmodeledTopicSettings(described *Ydb_Topic.DescribeTopicResult) error {
	settings := []struct {
		name string
		set  bool
	}{
		{"retention_storage_mb", described.GetRetentionStorageMb() != 0},
		// The field is deprecated in favor of max_active_partitions, and a
		// topic another tool made may still hold it; reading it is how such a
		// topic is refused rather than read without it.
		{"partition_count_limit", described.GetPartitioningSettings().GetPartitionCountLimit() != 0}, //nolint:staticcheck // SA1019: the deprecated field is the one being refused
		{"metering_mode", described.GetMeteringMode() != Ydb_Topic.MeteringMode_METERING_MODE_UNSPECIFIED},
		{"partition_total_read_speed_bytes_per_second", described.GetPartitionTotalReadSpeedBytesPerSecond() != 0},
		{"partition_consumer_read_speed_bytes_per_second", described.GetPartitionConsumerReadSpeedBytesPerSecond() != 0},
		{"metrics_level", described.MetricsLevel != nil},
	}
	for _, setting := range settings {
		if setting.set {
			return fmt.Errorf("it holds %s, which Ptah does not model and no YQL statement sets; "+
				"read a schema that does not hold the topic", setting.name)
		}
	}
	return nil
}

// topicStrategy names an auto-partitioning strategy as YQL spells it. YDB
// reports a topic created without one as disabled, and an unspecified
// strategy is the same.
func topicStrategy(strategy Ydb_Topic.AutoPartitioningStrategy) (string, error) {
	switch strategy {
	case Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_UNSPECIFIED,
		Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_DISABLED:
		return ydbtopic.StrategyDisabled, nil
	case Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_SCALE_UP:
		return ydbtopic.StrategyScaleUp, nil
	case Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN:
		return ydbtopic.StrategyScaleUpAndDown, nil
	case Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_PAUSED:
		return ydbtopic.StrategyPaused, nil
	default:
		return "", fmt.Errorf("its auto-partitioning strategy %d is not one this build of Ptah reads", int32(strategy))
	}
}

// topicInterval writes a duration YDB reports as the ISO 8601 interval a
// declaration takes, or empty for none. A fraction of a second is refused,
// because a declaration keeps whole seconds and would change the setting.
func topicInterval(subject string, duration *durationpb.Duration) (string, error) {
	if duration == nil {
		return "", nil
	}
	if duration.GetNanos() != 0 || duration.GetSeconds() < 0 {
		return "", fmt.Errorf("%s is %s, which is not a whole number of seconds a declaration can hold",
			subject, duration.AsDuration())
	}
	if duration.GetSeconds() == 0 {
		return "", nil
	}
	return ydbtopic.FormatSeconds(uint64(duration.GetSeconds())), nil // #nosec G115 -- a negative duration is refused above
}

// topicCodecs names the codecs of a codec list, owner's being "its" for the
// topic's own list.
func topicCodecs(owner string, codecs *Ydb_Topic.SupportedCodecs) ([]string, error) {
	if err := refuseUnknownFields(owner+" codec list", codecs); err != nil {
		return nil, err
	}
	var names []string
	for _, number := range codecs.GetCodecs() {
		name, known := ydbtopic.CodecName(number)
		if !known {
			return nil, fmt.Errorf("%s codec list names codec %d, which Ptah does not declare", owner, number)
		}
		names = append(names, name)
	}
	return names, nil
}

// streamingConsumerType is the field ydb_topic.proto gives a streaming
// consumer in the consumer_type oneof, which the pinned protocol buffers do
// not know. 26.1.1.22 and 26.2.1.14 report it, empty, for every consumer YQL
// creates, and 25.1.4.7 does not report it at all; both are the one kind of
// consumer Ptah models. Field 10 is a shared consumer, which YDB 26.2.1.14
// keeps behind a flag (`shared consumers is disabled`).
const (
	streamingConsumerType protowire.Number = 9
	sharedConsumerType    protowire.Number = 10
)

// decodeTopicConsumer reads one consumer.
func decodeTopicConsumer(described *Ydb_Topic.Consumer) (ast.TopicConsumerSpec, error) {
	for _, number := range unknownFields(described) {
		switch number {
		case streamingConsumerType:
			if !emptyField(described, number) {
				return ast.TopicConsumerSpec{}, fmt.Errorf("its streaming consumer type carries settings this " +
					"build of Ptah does not read")
			}
		case sharedConsumerType:
			return ast.TopicConsumerSpec{}, fmt.Errorf("it is a shared consumer, which Ptah does not model")
		default:
			return ast.TopicConsumerSpec{}, fmt.Errorf("its description carries field %d, which this build of "+
				"Ptah does not read", number)
		}
	}
	consumer := ast.TopicConsumerSpec{Name: described.GetName(), Important: described.GetImportant()}
	if readFrom := described.GetReadFrom(); readFrom != nil {
		if readFrom.GetNanos() != 0 {
			return ast.TopicConsumerSpec{}, fmt.Errorf("its read_from %s is not a whole second",
				readFrom.AsTime().Format(time.RFC3339Nano))
		}
		consumer.ReadFrom = ydbtopic.FormatReadFrom(readFrom.AsTime())
	}
	var err error
	if consumer.SupportedCodecs, err = topicCodecs("its", described.GetSupportedCodecs()); err != nil {
		return ast.TopicConsumerSpec{}, err
	}
	if consumer.AvailabilityPeriod, err = topicInterval("its availability period",
		described.GetAvailabilityPeriod()); err != nil {
		return ast.TopicConsumerSpec{}, err
	}
	return consumer, nil
}

// refuseUnknownFields refuses a message carrying a field the pinned protocol
// buffers do not know, naming subject.
func refuseUnknownFields(subject string, message protoreflect.ProtoMessage) error {
	if unknown := unknownFields(message); len(unknown) > 0 {
		return fmt.Errorf("%s carries field %s, which this build of Ptah does not read", subject, joinNumbers(unknown))
	}
	return nil
}

// emptyField reports whether every occurrence of the unknown field number in
// message is a length-delimited value of no length: an empty message.
func emptyField(message protoreflect.ProtoMessage, number protowire.Number) bool {
	unknown := message.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		found, wireType, length := protowire.ConsumeTag(unknown)
		if length < 0 {
			return false
		}
		unknown = unknown[length:]
		if found == number {
			value, valueLength := protowire.ConsumeBytes(unknown)
			if wireType != protowire.BytesType || valueLength < 0 || len(value) != 0 {
				return false
			}
		}
		valueLength := protowire.ConsumeFieldValue(found, wireType, unknown)
		if valueLength < 0 {
			return false
		}
		unknown = unknown[valueLength:]
	}
	return true
}
