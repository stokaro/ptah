package yqlparse

import (
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbtopic"
)

func (p *parser) topic() *ast.CreateTopicNode {
	name := p.path()
	var consumers []ast.TopicConsumerSpec
	seen := make(map[string]bool)
	if p.accept("(") {
		for !p.done() {
			p.wantWord("CONSUMER")
			consumer := p.consumer()
			if seen[consumer.Name] {
				p.failf("consumer %q is declared twice", consumer.Name)
			}
			seen[consumer.Name] = true
			consumers = append(consumers, consumer)
			if !p.accept(",") {
				break
			}
		}
		p.want(")")
	}
	spec, err := ydbtopic.ParseTopic(p.declarationSettings(topicSetting))
	if err != nil {
		p.failf("%v", err)
	}
	spec.Consumers = consumers
	return ast.NewCreateTopic(name, spec)
}

func (p *parser) consumer() ast.TopicConsumerSpec {
	name := decodedName(p.identifier())
	values := p.declarationSettings(consumerSetting)
	values[ydbtopic.AttributeName] = name
	spec, err := ydbtopic.ParseConsumer(values)
	if err != nil {
		p.failf("%v", err)
	}
	if spec.Name != name {
		p.failf("consumer name %q cannot be represented exactly", name)
	}
	return spec
}

func (p *parser) declarationSettings(setting func(string) string) map[string]string {
	values := make(map[string]string)
	if !p.word("WITH") {
		return values
	}
	p.pos++
	raw := p.options()
	for _, key := range slices.Sorted(maps.Keys(raw)) {
		value := raw[key]
		constructor := setting(key)
		switch constructor {
		case "unsupported":
			p.failf("unsupported declaration setting %q", key)
		case "":
			values[key] = scalar(value)
		default:
			literal := newParser(value)
			literal.wantWord(constructor)
			literal.want("(")
			decoded, ok := stringLiteral(literal.expression(), "String")
			literal.want(")")
			if literal.err != nil || !literal.done() || !ok {
				p.failf("%s requires %s with a string literal", key, constructor)
			}
			values[key] = decoded
		}
	}
	return values
}

// topicSetting is the syntax boundary; ParseTopic and ParseConsumer validate
// values but intentionally ignore unrelated annotation attributes.
func topicSetting(key string) string {
	switch key {
	case ydbtopic.AttributeMinActivePartitions, ydbtopic.AttributeMaxActivePartitions,
		ydbtopic.AttributeStrategy, ydbtopic.AttributeUpUtilizationPercent,
		ydbtopic.AttributeDownUtilizationPercent, ydbtopic.AttributeWriteSpeed, ydbtopic.AttributeWriteBurst,
		ydbtopic.AttributeSupportedCodecs:
		return ""
	case ydbtopic.AttributeStabilizationWindow, ydbtopic.AttributeRetentionPeriod:
		return "Interval"
	default:
		return "unsupported"
	}
}

func consumerSetting(key string) string {
	switch key {
	case ydbtopic.AttributeImportant, ydbtopic.AttributeSupportedCodecs:
		return ""
	case ydbtopic.AttributeReadFrom:
		return "Timestamp"
	case ydbtopic.AttributeAvailabilityPeriod:
		return "Interval"
	default:
		return "unsupported"
	}
}
