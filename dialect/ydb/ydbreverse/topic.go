package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicService reverses topic changes. A created topic is dropped, and a
// dropped one is created again with the settings and consumers it had. A
// change is undone in place as far as YDB allows: a topic keeps every
// partition the forward change gave it, and keeps auto-partitioning once it
// was enabled, paused rather than disabled (see [ydbtopic.RollbackTarget]).
// Messages and consumer positions are never restored, and each loss is
// reported.
type TopicService struct{}

// ReverseChanges returns one reversal per change, in input order.
func (TopicService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "topics", capability.Topics, reverseTopic)
}

func reverseTopic(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbtopic.ValidateIdentity(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := record.Value.(*ydbdiff.Topic)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a topic change", schemaext.ErrInvalidValue)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	path := ydbtopic.Display(record.Subject.Schema.Source, record.Subject.Name.Source)
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbtopic.Kind}
	if change.After != nil {
		projection.Value = change.After.Observed()
	}
	reversal := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject},
		ForwardState: []schemaext.ProjectedValue{projection}}
	switch {
	case change.Before == nil:
		reversal.Change.Value = &ydbdiff.Topic{Before: change.After.Observed()}
		reversal.Strategy = "drop the created topic"
		reversal.Limitations = []string{fmt.Sprintf("dropping topic %s discards every message written to it and every consumer's position", path)}
	case change.After == nil:
		reversal.Change.Value = &ydbdiff.Topic{After: change.Before.Desired()}
		reversal.Strategy = "create the dropped topic again with its settings and consumers"
		reversal.Limitations = []string{fmt.Sprintf("the messages topic %s held and its consumers' positions were dropped; "+
			"the rollback creates it empty", path)}
	default:
		reversal.Strategy = "restore the topic's settings and consumers in place"
		reversal.Limitations = topicChangeLimitations(path, change)
		target := ydbtopic.RollbackTarget(change.Before.Spec, change.After.Spec)
		if !ydbtopic.Equal(target, change.After.Spec) {
			reversal.Change.Value = &ydbdiff.Topic{Before: change.After.Observed(), After: &ydbtopic.Desired{Spec: target}}
		}
		if len(reversal.Limitations) == 0 && reversal.Change.Value == nil {
			return schemaext.Reversal{}, fmt.Errorf("%w: topic operands contain no change", schemaext.ErrInvalidValue)
		}
	}
	return reversal, nil
}

// topicChangeLimitations names what undoing an in-place change cannot
// restore: partitions YDB never removes, auto-partitioning it does not
// disable again, and the positions of consumers the change or its rollback
// drops.
func topicChangeLimitations(path string, change *ydbdiff.Topic) []string {
	var limitations []string
	before, after := ydbtopic.Resolve(change.Before.Spec), ydbtopic.Resolve(change.After.Spec)
	if after.MinActivePartitions > before.MinActivePartitions {
		limitations = append(limitations, fmt.Sprintf("topic %s keeps the %d partitions the change gave it, since YDB never removes a partition",
			path, after.MinActivePartitions))
	}
	if after.AutoPartitioned() && !before.AutoPartitioned() {
		limitations = append(limitations, fmt.Sprintf("topic %s keeps auto-partitioning, paused, since YDB does not disable it once it is on", path))
	}
	consumers := change.Consumers()
	for _, consumer := range consumers.Removed {
		limitations = append(limitations, fmt.Sprintf("consumer %q of topic %s was dropped; the rollback adds it again without its position", consumer, path))
	}
	for _, consumer := range consumers.Added {
		limitations = append(limitations, fmt.Sprintf("consumer %q the change added to topic %s is dropped by the rollback, with its position", consumer, path))
	}
	for _, consumer := range consumers.Restarted {
		limitations = append(limitations, fmt.Sprintf("consumer %q of topic %s was dropped and added again; its earlier position is lost", consumer, path))
	}
	return limitations
}
