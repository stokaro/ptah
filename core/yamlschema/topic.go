package yamlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
)

// topicSpec is one YDB topic in a YAML document: its directory, its settings
// spelled as YDB spells them, and its consumers in the order they are
// written. Each setting is a pointer, so a setting the document leaves out is
// told from one written with an empty value, which is refused rather than
// read as no setting.
type topicSpec struct {
	Name                   stringScalar                  `yaml:"name"`
	Schema                 stringScalar                  `yaml:"schema"`
	MinActivePartitions    *stringScalar                 `yaml:"min_active_partitions"`
	MaxActivePartitions    *stringScalar                 `yaml:"max_active_partitions"`
	Strategy               *stringScalar                 `yaml:"auto_partitioning_strategy"`
	UpUtilizationPercent   *stringScalar                 `yaml:"auto_partitioning_up_utilization_percent"`
	DownUtilizationPercent *stringScalar                 `yaml:"auto_partitioning_down_utilization_percent"`
	StabilizationWindow    *stringScalar                 `yaml:"auto_partitioning_stabilization_window"`
	RetentionPeriod        *stringScalar                 `yaml:"retention_period"`
	WriteSpeed             *stringScalar                 `yaml:"partition_write_speed_bytes_per_second"`
	WriteBurst             *stringScalar                 `yaml:"partition_write_burst_bytes"`
	SupportedCodecs        *stringList                   `yaml:"supported_codecs"`
	Consumers              orderedMap[topicConsumerSpec] `yaml:"consumers"`
}

// topicConsumerSpec is one consumer of a topic, keyed by its name.
type topicConsumerSpec struct {
	Important          *stringScalar `yaml:"important"`
	ReadFrom           *stringScalar `yaml:"read_from"`
	SupportedCodecs    *stringList   `yaml:"supported_codecs"`
	AvailabilityPeriod *stringScalar `yaml:"availability_period"`
}

// values are the topic's settings keyed by attribute name, as
// [ydbtopic.ParseTopic] reads them.
func (spec topicSpec) values() map[string]string {
	values := make(map[string]string)
	for attribute, value := range map[string]*stringScalar{
		ydbtopic.AttributeMinActivePartitions:    spec.MinActivePartitions,
		ydbtopic.AttributeMaxActivePartitions:    spec.MaxActivePartitions,
		ydbtopic.AttributeStrategy:               spec.Strategy,
		ydbtopic.AttributeUpUtilizationPercent:   spec.UpUtilizationPercent,
		ydbtopic.AttributeDownUtilizationPercent: spec.DownUtilizationPercent,
		ydbtopic.AttributeStabilizationWindow:    spec.StabilizationWindow,
		ydbtopic.AttributeRetentionPeriod:        spec.RetentionPeriod,
		ydbtopic.AttributeWriteSpeed:             spec.WriteSpeed,
		ydbtopic.AttributeWriteBurst:             spec.WriteBurst,
	} {
		if value != nil {
			values[attribute] = string(*value)
		}
	}
	if spec.SupportedCodecs != nil {
		values[ydbtopic.AttributeSupportedCodecs] = strings.Join(*spec.SupportedCodecs, ",")
	}
	return values
}

// values are the consumer's settings keyed by attribute name, as
// [ydbtopic.ParseConsumer] reads them.
func (spec topicConsumerSpec) values(name string) map[string]string {
	values := map[string]string{ydbtopic.AttributeName: name}
	for attribute, value := range map[string]*stringScalar{
		ydbtopic.AttributeImportant:          spec.Important,
		ydbtopic.AttributeReadFrom:           spec.ReadFrom,
		ydbtopic.AttributeAvailabilityPeriod: spec.AvailabilityPeriod,
	} {
		if value != nil {
			values[attribute] = string(*value)
		}
	}
	if spec.SupportedCodecs != nil {
		values[ydbtopic.AttributeSupportedCodecs] = strings.Join(*spec.SupportedCodecs, ",")
	}
	return values
}

// addTopics reads the document's topics, each checked by the rules the
// annotation parser reads a topic with.
func (d document) addTopics(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Topics) {
		spec := d.Topics[key]
		topic, err := ydbtopic.ParseTopic(spec.values())
		if err != nil {
			return fmt.Errorf("topic %q: %w", key, err)
		}
		for _, entry := range spec.Consumers {
			consumer, err := ydbtopic.ParseConsumer(entry.Value.values(entry.Name))
			if err != nil {
				return fmt.Errorf("topic %q, consumer %q: %w", key, entry.Name, err)
			}
			topic.Consumers = append(topic.Consumers, consumer)
		}
		db.FeatureObjects, err = ydbtopic.Declare(db.FeatureObjects, string(spec.Schema), valueOrDefault(spec.Name, key), "", topic)
		if err != nil {
			return fmt.Errorf("topic %q: %w", key, err)
		}
	}
	return nil
}
