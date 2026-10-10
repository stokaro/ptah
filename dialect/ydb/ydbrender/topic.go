package ydbrender

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicHandler renders one statement on a standalone topic: CREATE TOPIC
// with the consumers and the settings the declaration names, ALTER TOPIC
// written from the difference between the two operands, or DROP TOPIC. A
// declaration the target cannot hold, or a change YDB cannot make in place, is
// refused before anything is written.
func TopicHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.Topic{}, ast.StatementExtension, validateTopic, renderTopic)
}

// TopicConsumerHandler renders the addition of one consumer to a topic:
// `ALTER TOPIC <path> ADD CONSUMER ...`.
func TopicConsumerHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.TopicConsumer{}, ast.StatementExtension, validateTopicConsumer, renderTopicConsumer)
}

func validateTopic(ctx renderer.ExtensionContext, value *ydbast.Topic) error {
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	if err := topicTarget(ctx, value.Path()); err != nil {
		return err
	}
	// A drop is checked with an empty spec, which holds it to the topics key
	// alone.
	var spec ydbtopic.Spec
	if value.Change.After != nil {
		spec = value.Change.After.Spec
	}
	if err := ydbtopic.Check(value.Schema, value.Name, spec, ctx.Capabilities).Err(ctx.Target); err != nil {
		return err
	}
	if value.Change.Before != nil && value.Change.After != nil {
		return ydbtopic.ChangeRefusal(value.Schema, value.Name, value.Change.After.Spec, value.Change.Before.Spec).Err(ctx.Target)
	}
	return nil
}

func renderTopic(_ renderer.ExtensionContext, value *ydbast.Topic) ([]string, error) {
	switch {
	case value.Change.Before == nil:
		return []string{ydbtopic.CreateStatement(value.Schema, value.Name, value.Change.After.Spec)}, nil
	case value.Change.After == nil:
		return []string{ydbtopic.DropStatement(value.Schema, value.Name)}, nil
	default:
		return ydbtopic.AlterStatements(value.Schema, value.Name, value.Change.After.Spec, value.Change.Before.Spec), nil
	}
}

func validateTopicConsumer(ctx renderer.ExtensionContext, value *ydbast.TopicConsumer) error {
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	if err := topicTarget(ctx, value.Path()); err != nil {
		return err
	}
	spec := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{value.Consumer}}
	return ydbtopic.Check(value.Schema, value.Name, spec, ctx.Capabilities).Err(ctx.Target)
}

func renderTopicConsumer(_ renderer.ExtensionContext, value *ydbast.TopicConsumer) ([]string, error) {
	spec := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{value.Consumer}}
	return ydbtopic.AlterStatements(value.Schema, value.Name, spec, ydbtopic.Spec{}), nil
}

// topicTarget refuses a target other than YDB, and a YDB line without the
// topics key, by the capability.
func topicTarget(ctx renderer.ExtensionContext, path string) error {
	if ctx.Target != "ydb" {
		return (&ydbtopic.Refusal{Subject: "topic " + path, Key: capability.Topics}).Err(ctx.Target)
	}
	return nil
}
