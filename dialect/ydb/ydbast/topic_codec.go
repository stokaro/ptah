package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicCodec records the explicit topic operation wire: the path and both
// operands of the change.
func TopicCodec() schemaext.Codec {
	return pathChangeCodec("topic", &Topic{}, ydbdiff.TopicCodec(),
		func(value *Topic) (string, string, *ydbdiff.Topic) { return value.Schema, value.Name, &value.Change },
		func(schema, name string, change ydbdiff.Topic) *Topic {
			return &Topic{Schema: schema, Name: name, Change: change}
		})
}

// TopicConsumerCodec records the explicit consumer addition wire: the topic's
// path and the consumer.
func TopicConsumerCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := topicConsumerOperation(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return schemaext.Codec{Prototype: &TopicConsumer{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["schema","name","consumer"],"additionalProperties":false,` +
			`"properties":{"schema":{"type":"string"},"name":{"type":"string","minLength":1},"consumer":` + ydbtopic.ConsumerDefinition + `}}`),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := topicConsumerOperation(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			if err := ydbtopic.RefuseNulls(data); err != nil {
				return nil, err
			}
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if len(fields) != 3 || len(fields["consumer"]) == 0 || len(fields["name"]) == 0 || len(fields["schema"]) == 0 {
				return nil, fmt.Errorf("%w: topic consumer operation requires exactly schema, name, and consumer", schemaext.ErrInvalidValue)
			}
			value, err := schemaext.DecodeJSON[*TopicConsumer](data)
			if err != nil {
				return nil, err
			}
			return topicConsumerOperation(value)
		},
	}
}

func topicConsumerOperation(payload schemaext.Payload) (*TopicConsumer, error) {
	value, ok := payload.(*TopicConsumer)
	if !ok {
		return nil, fmt.Errorf("%w: expected a topic consumer operation, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}
