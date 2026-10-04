package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"slices"
	"strings"

	ptahast "ptah.run/core/ast"
	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbtopic"
)

// pendingTopicConsumer is a consumer annotation waiting for the topic it
// reads, which the same file may declare after it.
type pendingTopicConsumer struct {
	schema   string
	topic    string
	consumer ptahast.TopicConsumerSpec
	ctx      annotationErrorContext
}

// parseTopicComment reads a YDB topic declaration.
//
// There is no dialect scope here, for the reason a synonym has none: a topic
// is a YDB object and nothing else, and every other target refuses one.
func (s *schemaParseState) parseTopicComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:topic", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	spec, err := ydbtopic.ParseTopic(kv)
	if err != nil {
		return topicAttributeError(ctx, "ptah:schema:topic", err)
	}
	s.topics = append(s.topics, schemamodel.Topic{
		StructName: structName,
		Name:       strings.TrimSpace(kv[ydbtopic.AttributeName]),
		Schema:     strings.Trim(strings.TrimSpace(kv[ydbtopic.AttributeSchema]), "/"),
		Spec:       spec,
	})
	return nil
}

// parseTopicConsumerComment reads a consumer of a YDB topic, attached to its
// topic once the whole file is read.
func (s *schemaParseState) parseTopicConsumerComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:topic:consumer", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	consumer, err := ydbtopic.ParseConsumer(kv)
	if err != nil {
		return topicAttributeError(ctx, "ptah:schema:topic:consumer", err)
	}
	s.topicConsumers = append(s.topicConsumers, pendingTopicConsumer{
		schema:   strings.Trim(strings.TrimSpace(kv[ydbtopic.AttributeSchema]), "/"),
		topic:    strings.TrimSpace(kv[ydbtopic.AttributeTopic]),
		consumer: consumer,
		ctx:      ctx,
	})
	return nil
}

// attachTopicConsumers gives each topic of the file the consumers declared
// for it. A consumer of a topic the file does not declare is refused rather
// than dropped, because a reader nobody creates is a declaration with no
// effect, and so is a second consumer of one name, which YDB refuses
// (`Consumer c defined more than once`).
func (s *schemaParseState) attachTopicConsumers() error {
	for _, pending := range s.topicConsumers {
		index := slices.IndexFunc(s.topics, func(topic schemamodel.Topic) bool {
			return topic.Name == pending.topic && topic.Schema == pending.schema
		})
		if index < 0 {
			return topicPlacementError(pending.ctx, fmt.Sprintf("the file declares no topic %q for consumer %q",
				schemamodel.Topic{Name: pending.topic, Schema: pending.schema}.QualifiedName(), pending.consumer.Name))
		}
		topic := &s.topics[index]
		if slices.ContainsFunc(topic.Spec.Consumers, func(have ptahast.TopicConsumerSpec) bool {
			return have.Name == pending.consumer.Name
		}) {
			return topicPlacementError(pending.ctx, fmt.Sprintf("topic %q declares consumer %q twice",
				topic.QualifiedName(), pending.consumer.Name))
		}
		topic.Spec.Consumers = append(topic.Spec.Consumers, pending.consumer)
	}
	return nil
}

// topicAttributeError reports a value a topic or consumer declaration cannot
// carry, naming the attribute.
func topicAttributeError(ctx annotationErrorContext, directive string, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbtopic.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
	}
	return parseErr
}

// topicPlacementError reports a consumer annotation that names no topic of
// its file, or names one twice.
func topicPlacementError(ctx annotationErrorContext, reason string) error {
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: "ptah:schema:topic:consumer",
		Attribute: ydbtopic.AttributeTopic,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%s on %s at %s", reason, ctx.directive, ctx.location),
	}
}
