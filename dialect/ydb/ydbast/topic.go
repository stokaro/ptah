package ydbast

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicKind identifies one statement on a standalone YDB topic.
const TopicKind schemaext.Kind = "ptah.run/ydb/topic-operation"

// TopicConsumerKind identifies the addition of one consumer to a topic.
const TopicConsumerKind schemaext.Kind = "ptah.run/ydb/topic-consumer-operation"

// Topic creates, changes or drops one standalone topic. A missing before
// operand requests CREATE TOPIC, a missing after one DROP TOPIC, and both
// ALTER TOPIC, written from the difference between them. Directory and leaf
// names stay separate, so a dot is part of a name.
type Topic struct {
	Schema string        `json:"schema"`
	Name   string        `json:"name"`
	Change ydbdiff.Topic `json:"change"`
}

// Kind returns the stable operation identity.
func (*Topic) Kind() schemaext.Kind { return TopicKind }

// CloneExtension returns an independent operation and both captured operands.
func (v *Topic) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*Topic)(nil)
	}
	change, _ := v.Change.CloneChange().(*ydbdiff.Topic)
	return &Topic{Schema: v.Schema, Name: v.Name, Change: *change}
}

// Subject returns the schema-scoped identity of the topic.
func (v *Topic) Subject() objectidentity.ID {
	return ydbtopic.Ref(v.Schema, v.Name)
}

// Path is the topic's path relative to the database root, as messages name it.
func (v *Topic) Path() string {
	return ydbtopic.Display(v.Schema, v.Name)
}

// Validate refuses an invalid path and invalid or empty operands.
func (v *Topic) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: topic operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ydbtopic.ValidateIdentity(v.Subject()); err != nil {
		return err
	}
	return v.Change.Validate()
}

// Effect records what the statement does to the topic's messages and its
// consumers' positions.
func (v *Topic) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}

// TopicConsumer adds one consumer to a topic: `ALTER TOPIC <path> ADD
// CONSUMER ...`. A YQL schema file declares a consumer of a topic or of a
// changefeed's topic this way; the topic's path is its directory and its leaf.
type TopicConsumer struct {
	Schema   string                `json:"schema"`
	Name     string                `json:"name"`
	Consumer ydbtopic.ConsumerSpec `json:"consumer"`
}

// Kind returns the stable operation identity.
func (*TopicConsumer) Kind() schemaext.Kind { return TopicConsumerKind }

// CloneExtension returns an independent operation.
func (v *TopicConsumer) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*TopicConsumer)(nil)
	}
	return &TopicConsumer{Schema: v.Schema, Name: v.Name, Consumer: v.Consumer.Clone()}
}

// Path is the topic's path relative to the database root.
func (v *TopicConsumer) Path() string {
	return ydbtopic.Display(v.Schema, v.Name)
}

// Validate refuses an invalid path and a consumer a declaration cannot carry.
func (v *TopicConsumer) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: topic consumer operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ydbtopic.ValidateIdentity(ydbtopic.Ref(v.Schema, v.Name)); err != nil {
		return err
	}
	if err := ydbtopic.Validate(ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{v.Consumer}}); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return nil
}

// Effect records that adding a consumer loses nothing.
func (v *TopicConsumer) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	return schemaext.Effect{Impact: schemaext.Additive, Reason: "ADD CONSUMER adds a topic consumer"}
}
