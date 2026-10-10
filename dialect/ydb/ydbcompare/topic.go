package ydbcompare

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicService compares standalone YDB topics. A topic both sides hold is
// compared through [ydbtopic.Equal]: each side's settings resolve to the value
// a new topic takes for the ones it leaves out, and consumers compare by name,
// so a declaration naming a default and one leaving it out are the same topic.
type TopicService struct{}

type topic struct {
	ref     objectidentity.ID
	desired *ydbtopic.Desired
	current *ydbtopic.Observed
}

// CompareObjects plans a creation only where the read established absence,
// since CREATE TOPIC carries no guard, and a removal only where the desired
// source claims to describe the topic. An incomplete observation never
// establishes absence or a destructive change, and it is reported only where
// the desired source makes a claim it would decide. A target without the
// topics capability is refused only when the comparison plans a statement.
func (TopicService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	topics, err := captureStandaloneInputs(ctx, request, ydbtopic.Kind, "topic", collectTopics)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	values := make([]standalonePresence, 0, len(topics))
	for _, value := range topics {
		values = append(values, standalonePresence{ref: value.ref, desired: value.desired != nil, current: value.current != nil})
	}
	coverage, err := standaloneCoverage(request, values, ydbtopic.Kind, "topics", ydbtopic.Coverage)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result, err := completeStandaloneComparison(ctx, request, topics, coverage, ydbtopic.Kind, "topic",
		func(value topic) objectidentity.ID { return value.ref }, compareTopic)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	held := make(map[objectidentity.Key]bool)
	for key, value := range topics {
		held[key] = value.current != nil
	}
	var drops []objectidentity.ID
	for _, change := range result.Changes {
		if value, ok := change.Value.(*ydbdiff.Topic); ok && value.After == nil {
			drops = append(drops, change.Subject)
		}
	}
	if err := refuseDottedLimits(request.Desired.Coverage, "topic", held, drops, ydbtopic.Display); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	// The changes come out in path order, so the refusal names the first
	// topic a statement would create, change or drop. The planner holds each
	// statement to the rest of the target's rules.
	if len(result.Changes) > 0 {
		ref := result.Changes[0].Subject
		if err := ydbtopic.Check(ref.Schema.Source, ref.Name.Source, ydbtopic.Spec{}, request.Capabilities).Err(request.Target); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
	}
	return result, nil
}

func compareTopic(request schemaext.ObjectComparisonRequest, value topic, result *schemaext.ObjectComparisonResult) error {
	desiredKnowledge := request.Desired.Coverage.Lookup(ydbtopic.Kind, value.ref)
	if value.desired != nil && standaloneLimited(request.Desired.Coverage, ydbtopic.Kind, value.ref) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbtopic.Kind, Subject: value.ref,
			Reason: "the desired source cannot describe the topic"})
		return nil
	}
	// A desired source that neither declares the topic nor describes its
	// namespace asks for nothing here, so an unread topic has nothing to
	// decide; one read in full is kept.
	claimed := value.desired != nil || !unknown(desiredKnowledge)
	currentKnowledge := request.Current.Coverage.Lookup(ydbtopic.Kind, value.ref)
	if standaloneLimited(request.Current.Coverage, ydbtopic.Kind, value.ref) || (value.current == nil && unknown(currentKnowledge)) {
		if claimed {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbtopic.Kind, Subject: value.ref,
				Reason: "the topic or its absence was not established: " + currentKnowledge.Reason})
		}
		return nil
	}
	if !claimed {
		if value.current != nil {
			var err error
			result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: value.ref, Value: value.current.Desired()})
			return err
		}
		return nil
	}
	switch {
	case value.desired == nil && value.current == nil:
		return nil
	case value.desired != nil && value.current != nil && ydbtopic.Equal(value.desired.Spec, value.current.Spec):
		return nil
	default:
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: value.ref,
			Value: &ydbdiff.Topic{Before: value.current, After: value.desired}})
		return nil
	}
}

func collectTopics(ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation, topics map[objectidentity.Key]topic) error {
	return collectStandalone(ctx, state, direction, ydbtopic.Kind, "topic", ydbtopic.Codecs(), ydbtopic.ValidateIdentity,
		func(ref objectidentity.ID, desired *ydbtopic.Desired, current *ydbtopic.Observed) {
			value := topics[ref.Key()]
			value.ref = ref
			if desired != nil {
				value.desired = desired
			}
			if current != nil {
				value.current = current
			}
			topics[ref.Key()] = value
		})
}
