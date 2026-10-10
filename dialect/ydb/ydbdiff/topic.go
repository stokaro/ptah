package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicKind identifies a change to one standalone YDB topic.
const TopicKind schemaext.Kind = "ptah.run/ydb/topic-change"

// The reasons a topic change reports, which the safety report repeats.
const (
	// DropTopicReason is why dropping a topic is destructive.
	DropTopicReason = "DROP TOPIC removes the topic, every message it holds and every consumer's position in it"
	// DropConsumerReason is why a change that drops a consumer, or adds one
	// again, is destructive.
	DropConsumerReason = "DROP CONSUMER removes a topic consumer and its position in the topic"
	// ChangeTopicReason is why another change of a topic needs review.
	ChangeTopicReason = "ALTER TOPIC can shorten how long the topic keeps a message, or move where a consumer reads from"
)

// Topic captures both operands of a change to one topic. A nil Before means
// the database holds no topic at the path; a nil After means the declaration
// drops it. A nil side is established absence, never an unread topic.
type Topic struct {
	Before *ydbtopic.Observed `json:"before"`
	After  *ydbtopic.Desired  `json:"after"`
}

// Kind returns the stable change identity.
func (*Topic) Kind() schemaext.Kind { return TopicKind }

// CloneChange returns independent before and after snapshots.
func (v *Topic) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Topic)(nil)
	}
	cloned := &Topic{}
	if v.Before != nil {
		cloned.Before = &ydbtopic.Observed{Spec: v.Before.Spec.Clone()}
	}
	if v.After != nil {
		cloned.After = &ydbtopic.Desired{Spec: v.After.Spec.Clone(), StructName: v.After.StructName}
	}
	return cloned
}

// Validate refuses a change without operands, an operand YDB would not keep
// as written, and two operands that describe the same topic.
func (v *Topic) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a topic change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbtopic.Validate(v.Before.Spec); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
	}
	if v.After != nil {
		if err := v.After.Validate(); err != nil {
			return err
		}
	}
	if v.Before != nil && v.After != nil && ydbtopic.Equal(v.After.Spec, v.Before.Spec) {
		return fmt.Errorf("%w: topic operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}

// Consumers reports how the consumers of a topic both operands hold differ.
func (v *Topic) Consumers() ydbtopic.ConsumerChanges {
	if v == nil || v.Before == nil || v.After == nil {
		return ydbtopic.ConsumerChanges{}
	}
	return ydbtopic.Compare(v.After.Spec, v.Before.Spec)
}

// Effect records what the change does to the topic's messages and to its
// consumers' positions in it. Dropping the topic loses both. A change that
// drops a consumer, or drops and adds one YDB cannot change in place, loses
// that consumer's position. A change that only adds consumers loses nothing,
// and any other change can shorten how long a message stays or move where a
// consumer reads from.
func (v *Topic) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE TOPIC adds a topic"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: DropTopicReason}
	}
	consumers := v.Consumers()
	switch {
	case len(consumers.Removed)+len(consumers.Restarted) > 0:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: DropConsumerReason}
	case ydbtopic.SettingsEqual(v.After.Spec, v.Before.Spec) && len(consumers.Changed) == 0:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "ALTER TOPIC adds consumers"}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ChangeTopicReason}
	}
}

// TopicCodec describes the complete before/after wire for one topic change.
func TopicCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := topicValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	definition := `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]}}}`
	models := ydbtopic.Codecs()
	return schemaext.Codec{Prototype: &Topic{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(definition, models[1].Definition, models[0].Definition)),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := topicValue(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneChange(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			before, after, err := decodeStandaloneOperands[*ydbtopic.Observed, *ydbtopic.Desired](data, "topic", ydbtopic.Codecs())
			if err != nil {
				return nil, err
			}
			return topicValue(&Topic{Before: before, After: after})
		},
	}
}

func topicValue(payload schemaext.Payload) (*Topic, error) {
	value, ok := payload.(*Topic)
	if !ok {
		return nil, fmt.Errorf("%w: expected a topic change, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}
