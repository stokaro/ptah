package ydbextensions

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
)

// Codecs returns the explicit current operation codecs. These describe the
// model only; they grant no target capability and perform no server discovery.
func Codecs() []schemaext.Codec {
	models := append(ydbschema.Codecs(), ydbcoordination.Codecs()...)
	models = append(models, ydbstreaming.Codecs()...)
	models = append(models, ydbworkload.Codecs()...)
	return append(append(models, ydbdiff.Codecs()...), []schemaext.Codec{
		ydbast.CoordinationCodec(),
		ydbast.StreamingCodec(),
		ydbast.ResourcePoolCodec(),
		ydbast.ResourcePoolClassifierCodec(),
		ydbast.DefaultPoolSettingsCodec(),
		operationCodec(&ydbast.AddChangefeed{}, decodeOperation[*ydbast.AddChangefeed]),
		operationCodec(&ydbast.DropChangefeed{}, decodeOperation[*ydbast.DropChangefeed]),
		operationCodec(&ydbast.AlterChangefeedTopic{}, decodeOperation[*ydbast.AlterChangefeedTopic]),
	}...)
}

func operationCodec(prototype ast.ExtensionPayload, decode func(json.RawMessage) (schemaext.Payload, error)) schemaext.Codec {
	return schemaext.Codec{
		Prototype: prototype, Representation: schemaext.Operation, Version: 1,
		Definition: ydbschema.WireDefinition(), Clone: cloneOperation,
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
		return ydbschema.ValidateChangefeed(operation.Changefeed)
	case *ydbast.DropChangefeed:
		return ydbschema.ValidateChangefeedName(operation.Name)
	case *ydbast.AlterChangefeedTopic:
		if err := ydbschema.ValidateChangefeed(operation.Previous); err != nil {
			return err
		}
		return ydbschema.ValidateChangefeed(operation.Changefeed)
	default:
		return fmt.Errorf("%w: unknown YDB operation %T", schemaext.ErrInvalidValue, payload)
	}
}

func canonicalOperation(payload schemaext.Payload) (json.RawMessage, error) {
	snapshot, err := cloneOperation(payload)
	if err != nil {
		return nil, err
	}
	switch operation := snapshot.(type) {
	case *ydbast.AddChangefeed:
		ydbschema.CanonicalChangefeed(&operation.Changefeed)
	case *ydbast.AlterChangefeedTopic:
		ydbschema.CanonicalChangefeed(&operation.Changefeed)
		ydbschema.CanonicalChangefeed(&operation.Previous)
	}
	return encodeOperation(snapshot)
}
