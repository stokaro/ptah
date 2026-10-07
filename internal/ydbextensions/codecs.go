package ydbextensions

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
)

// The complete wire definition is data owned by the feature. Its hash travels
// with every envelope, independently of this package's Go import path.
//
//go:embed changefeed-codecs.json
var changefeedDefinition []byte

// Codecs returns the explicit current operation codecs. These describe the
// model only; they grant no target capability and perform no server discovery.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		operationCodec(&ydbast.AddChangefeed{}, decodeOperation[*ydbast.AddChangefeed]),
		operationCodec(&ydbast.DropChangefeed{}, decodeOperation[*ydbast.DropChangefeed]),
		operationCodec(&ydbast.AlterChangefeedTopic{}, decodeOperation[*ydbast.AlterChangefeedTopic]),
	}
}

func operationCodec(prototype ast.ExtensionPayload, decode func(json.RawMessage) (schemaext.Payload, error)) schemaext.Codec {
	return schemaext.Codec{
		Prototype: prototype, Representation: schemaext.Operation, Version: 1,
		Definition: slices.Clone(changefeedDefinition), Clone: cloneOperation,
		Encode: encodeOperation, Decode: decode, Canonical: canonicalOperation,
	}
}

func cloneOperation(payload schemaext.Payload) (schemaext.Payload, error) {
	operation, ok := payload.(ast.ExtensionPayload)
	if !ok {
		return nil, fmt.Errorf("%w: expected a YDB operation, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ast.CloneExtensionPayload(operation)
}

func encodeOperation(payload schemaext.Payload) (json.RawMessage, error) {
	if err := requiredOperationFields(payload); err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}

func decodeOperation[T ast.ExtensionPayload](data json.RawMessage) (schemaext.Payload, error) {
	value, err := schemaext.DecodeJSON[T](data)
	if err != nil {
		return nil, err
	}
	if err := schemaext.ValidatePayload(value); err != nil {
		return nil, err
	}
	if err := requiredOperationFields(value); err != nil {
		return nil, err
	}
	return value, nil
}

func requiredOperationFields(payload schemaext.Payload) error {
	switch operation := payload.(type) {
	case *ydbast.AddChangefeed:
		return requiredFeedFields(operation.Changefeed)
	case *ydbast.DropChangefeed:
		return requiredFeedName(operation.Name)
	case *ydbast.AlterChangefeedTopic:
		if err := requiredFeedFields(operation.Previous); err != nil {
			return err
		}
		return requiredFeedFields(operation.Changefeed)
	default:
		return fmt.Errorf("%w: unknown YDB operation %T", schemaext.ErrInvalidValue, payload)
	}
}

func requiredFeedFields(feed ast.ChangefeedSpec) error {
	if err := requiredFeedName(feed.Name); err != nil {
		return err
	}
	if feed.Mode == "" || feed.Format == "" {
		return fmt.Errorf("%w: a changefeed requires mode and format", schemaext.ErrInvalidValue)
	}
	if err := validText(feed.Mode, feed.Format, feed.ResolvedTimestamps, feed.RetentionPeriod); err != nil {
		return err
	}
	seen := make(map[string]bool, len(feed.Consumers))
	for _, consumer := range feed.Consumers {
		if strings.TrimSpace(consumer.Name) == "" || seen[consumer.Name] {
			return fmt.Errorf("%w: a consumer requires a unique nonempty name", schemaext.ErrInvalidValue)
		}
		if err := validText(consumer.Name, consumer.ReadFrom, consumer.AvailabilityPeriod); err != nil {
			return err
		}
		if err := validText(consumer.SupportedCodecs...); err != nil {
			return err
		}
		seen[consumer.Name] = true
	}
	return nil
}

// encoding/json replaces invalid string bytes. Refuse them before encoding so
// an artifact cannot silently describe a different value from the input.
func validText(values ...string) error {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return fmt.Errorf("%w: a changefeed contains invalid UTF-8", schemaext.ErrInvalidValue)
		}
	}
	return nil
}

func requiredFeedName(name string) error {
	if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '/') {
		return fmt.Errorf("%w: a changefeed needs a name without a slash", schemaext.ErrInvalidValue)
	}
	return validText(name)
}

func canonicalOperation(payload schemaext.Payload) (json.RawMessage, error) {
	snapshot, err := cloneOperation(payload)
	if err != nil {
		return nil, err
	}
	switch operation := snapshot.(type) {
	case *ydbast.AddChangefeed:
		canonicalFeed(&operation.Changefeed)
	case *ydbast.AlterChangefeedTopic:
		canonicalFeed(&operation.Changefeed)
		canonicalFeed(&operation.Previous)
	}
	return encodeOperation(snapshot)
}

// Consumers and supported codecs are sets. All other fields retain their
// declaration spelling; applying server defaults is comparison, not encoding.
func canonicalFeed(feed *ast.ChangefeedSpec) {
	for i := range feed.Consumers {
		slices.Sort(feed.Consumers[i].SupportedCodecs)
	}
	slices.SortFunc(feed.Consumers, func(a, b ast.TopicConsumerSpec) int { return strings.Compare(a.Name, b.Name) })
}
