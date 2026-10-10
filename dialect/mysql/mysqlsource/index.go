package mysqlsource

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// IndexService decodes and encodes index options as string-valued platform
// properties of the mysql and mariadb targets: `platform.mysql.parser`. It
// performs no inspection. Its zero value is ready for concurrent use.
type IndexService struct{}

// ParserProperty is the index property key of a FULLTEXT parser.
const ParserProperty = "parser"

// IndexDefinitions returns independent property ownership declarations: the
// key parser.
func IndexDefinitions() []schemaext.PropertyDefinition {
	return []schemaext.PropertyDefinition{{Kind: mysqlschema.IndexKind, Keys: []string{ParserProperty}}}
}

// DecodeProperties decodes an ordered batch into DesiredIndex values. A
// property with an empty value states nothing. An unknown property and a
// parser the model refuses wrap schemaext.ErrInvalidValue. Any failure or
// cancellation returns no partial batch.
func (IndexService) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := validateRequest(ctx, request.Target, request.Format, schemaext.IndexPlatformProperties); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fragment.Kind != mysqlschema.IndexKind {
			return nil, fmt.Errorf("%w: MySQL source kind %q", schemaext.ErrInvalidValue, fragment.Kind)
		}
		index := &mysqlschema.DesiredIndex{}
		for key, value := range fragment.Properties {
			if key != ParserProperty {
				return nil, fmt.Errorf("%w: unknown MySQL index property %q", schemaext.ErrInvalidValue, key)
			}
			index.Parser = value
		}
		if err := mysqlschema.ValidateDesiredIndex(index); err != nil {
			return nil, err
		}
		result = append(result, index)
	}
	return result, ctx.Err()
}

// EncodeProperties writes a declared parser as its property. Unknown model
// types and invalid options wrap schemaext.ErrInvalidValue. Any failure or
// cancellation returns no partial batch.
func (IndexService) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := validateRequest(ctx, request.Target, request.Format, schemaext.IndexPlatformProperties); err != nil {
		return nil, err
	}
	result := make([]schemaext.PropertyFragment, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		index, ok := value.(*mysqlschema.DesiredIndex)
		if !ok {
			return nil, fmt.Errorf("%w: MySQL source expected desired index options, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := mysqlschema.ValidateDesiredIndex(index); err != nil {
			return nil, err
		}
		fragment := schemaext.PropertyFragment{Kind: mysqlschema.IndexKind, Properties: make(map[string]string)}
		if index.Parser != "" {
			fragment.Properties[ParserProperty] = index.Parser
		}
		result = append(result, fragment)
	}
	return result, ctx.Err()
}
