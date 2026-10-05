package ydbtopic

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// Refusal says why a topic cannot be written or changed on a target: Key is
// the capability it needs and the target lacks, or empty when YDB refuses it
// whatever the target, and Reason then says why.
type Refusal struct {
	// Subject names what is refused.
	Subject string
	// Key is the capability the declaration needs, empty for a refusal YDB
	// makes on every line.
	Key capability.Capability
	// Reason is why YDB refuses it, for a refusal without a key.
	Reason string
}

// Check reports why the topic name, declared as spec, cannot be created on a
// target holding caps, or nil when it can.
//
// It holds what a declaration's parse cannot know -- the target, through the
// topics key and the availability period's key -- and holds a spec built by
// hand to the parse's rules, because nothing else checks it: the settings
// YDB would keep differently, and the consumers, each once and each as the
// parse takes it.
func Check(name string, spec ast.TopicSpec, caps capability.Capabilities) *Refusal {
	subject := "topic " + name
	if !caps.Has(capability.Topics) {
		return &Refusal{Subject: subject, Key: capability.Topics}
	}
	if strings.TrimSpace(name) == "" {
		return &Refusal{Subject: "a topic", Reason: "a topic needs a name"}
	}
	if err := checkSettings(spec); err != nil {
		return &Refusal{Subject: subject, Reason: err.Error()}
	}
	names := make(map[string]bool, len(spec.Consumers))
	for _, consumer := range spec.Consumers {
		if names[consumer.Name] {
			return &Refusal{Subject: subject, Reason: fmt.Sprintf("two of its consumers are named %q, and YDB "+
				"names a consumer once per topic (`Consumer %s defined more than once`)", consumer.Name, consumer.Name)}
		}
		names[consumer.Name] = true
		if _, err := ParseConsumer(consumerValues(consumer)); err != nil {
			return &Refusal{Subject: consumerSubject(name, consumer.Name), Reason: err.Error()}
		}
		if consumer.AvailabilityPeriod != "" && !caps.Has(capability.TopicConsumerAvailabilityPeriod) {
			return &Refusal{Subject: consumerSubject(name, consumer.Name) + " takes availability_period",
				Key: capability.TopicConsumerAvailabilityPeriod}
		}
	}
	return nil
}

// ChangeRefusal reports why YDB cannot move the topic name from current to
// desired in place, or nil when it can. Measured on 25.1.4.7 and 26.2.1.14:
// a topic keeps every partition it has (`Invalid total groups count
// specified: 1 vs 2 (current)`), and keeps auto-partitioning once it is
// enabled (`Can't disable auto partitioning.`). Either change drops the
// topic and every message in it, so it is refused rather than planned.
func ChangeRefusal(name string, desired, current ast.TopicSpec) *Refusal {
	want, have := Resolve(desired), Resolve(current)
	subject := "topic " + name
	switch {
	case want.MinActivePartitions < have.MinActivePartitions:
		return &Refusal{Subject: subject, Reason: fmt.Sprintf("it has %d partitions and is declared with %d, and YDB "+
			"never removes a partition (`Invalid total groups count specified`); drop the topic and create it again "+
			"to lower the count", have.MinActivePartitions, want.MinActivePartitions)}
	case have.AutoPartitioned() && !want.AutoPartitioned():
		return &Refusal{Subject: subject, Reason: fmt.Sprintf("its auto-partitioning is %s and is declared disabled, "+
			"and YDB does not disable auto-partitioning once it is on (`Can't disable auto partitioning.`); declare "+
			"%s %s to stop it", have.Strategy, AttributeStrategy, StrategyPaused)}
	default:
		return nil
	}
}

// checkSettings holds a spec's settings to the rules [ParseTopic] reads them
// with.
func checkSettings(spec ast.TopicSpec) error {
	if _, err := ParseTopic(settingValues(spec)); err != nil {
		return err
	}
	return nil
}

// settingValues writes a spec's settings back as the attribute values
// [ParseTopic] reads.
func settingValues(spec ast.TopicSpec) map[string]string {
	values := make(map[string]string)
	counts := []struct {
		attribute string
		value     uint64
	}{
		{AttributeMinActivePartitions, spec.MinActivePartitions},
		{AttributeMaxActivePartitions, spec.MaxActivePartitions},
		{AttributeWriteSpeed, spec.PartitionWriteSpeedBytesPerSecond},
		{AttributeWriteBurst, spec.PartitionWriteBurstBytes},
		{AttributeUpUtilizationPercent, uint64(spec.AutoPartitioningUpUtilizationPercent)},
		{AttributeDownUtilizationPercent, uint64(spec.AutoPartitioningDownUtilizationPercent)},
	}
	for _, count := range counts {
		if count.value != 0 {
			values[count.attribute] = strconv.FormatUint(count.value, 10)
		}
	}
	texts := []struct {
		attribute string
		value     string
	}{
		{AttributeStrategy, spec.AutoPartitioningStrategy},
		{AttributeStabilizationWindow, spec.AutoPartitioningStabilizationWindow},
		{AttributeRetentionPeriod, spec.RetentionPeriod},
	}
	for _, text := range texts {
		if text.value != "" {
			values[text.attribute] = text.value
		}
	}
	if len(spec.SupportedCodecs) > 0 {
		values[AttributeSupportedCodecs] = strings.Join(spec.SupportedCodecs, ",")
	}
	return values
}

// consumerValues writes a consumer back as the attribute values
// [ParseConsumer] reads.
func consumerValues(consumer ast.TopicConsumerSpec) map[string]string {
	values := map[string]string{AttributeName: consumer.Name}
	if consumer.Important {
		values[AttributeImportant] = "true"
	}
	if consumer.ReadFrom != "" {
		values[AttributeReadFrom] = consumer.ReadFrom
	}
	if len(consumer.SupportedCodecs) > 0 {
		values[AttributeSupportedCodecs] = strings.Join(consumer.SupportedCodecs, ",")
	}
	if consumer.AvailabilityPeriod != "" {
		values[AttributeAvailabilityPeriod] = consumer.AvailabilityPeriod
	}
	return values
}

// RollbackTarget is the nearest topic to target that YDB can reach from
// current in place, for a rollback: target with its partition count raised to
// current's, since YDB never removes a partition, and with auto-partitioning
// paused rather than disabled where current has it on, since YDB does not
// disable it again. A target YDB reaches as it stands is returned unchanged.
//
// A rollback is the one place the nearest topic is planned rather than the
// change refused: the forward change was the author's, and refusing its
// rollback would refuse the migration that carries it. The statement the
// rollback renders names the partition count and the strategy it keeps, so a
// reader of the down migration sees what it leaves.
func RollbackTarget(target, current ast.TopicSpec) ast.TopicSpec {
	reached := target.Clone()
	want, have := Resolve(target), Resolve(current)
	if want.MinActivePartitions < have.MinActivePartitions {
		reached.MinActivePartitions = have.MinActivePartitions
	}
	if have.AutoPartitioned() && !want.AutoPartitioned() {
		reached.AutoPartitioningStrategy = StrategyPaused
		reached.MaxActivePartitions = have.MaxActivePartitions
		reached.AutoPartitioningUpUtilizationPercent = have.UpUtilizationPercent
		reached.AutoPartitioningDownUtilizationPercent = have.DownUtilizationPercent
		reached.AutoPartitioningStabilizationWindow = FormatSeconds(have.StabilizationWindowSeconds)
	}
	if reached.MaxActivePartitions != 0 && reached.MaxActivePartitions < reached.MinActivePartitions {
		reached.MaxActivePartitions = reached.MinActivePartitions
	}
	return reached
}
