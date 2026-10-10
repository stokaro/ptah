package chsource

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// IndexService preserves skipping-index setting intent through index platform
// properties. It performs neither default resolution nor catalog inspection.
// The zero value is ready for registration with IndexDefinitions.
type IndexService struct{}

type indexSettings struct {
	indexType   chschema.Setting
	granularity chschema.Setting
}

func indexProperties(settings *indexSettings) []property {
	return []property{{"type", &settings.indexType}, {"granularity", &settings.granularity}}
}

// IndexDefinitions returns independent property ownership for skipping-index
// type and granularity. A bare key carries explicit intent; .state=default
// selects a creation default. Missing keys remain unmanaged. Granularity is a
// positive decimal uint64. Explicit empty strings and zero are invalid. The
// definition absorbs the common index type into `type`, because on ClickHouse
// a declared index type is the data-skipping type.
func IndexDefinitions() []schemaext.PropertyDefinition {
	result := definitions(chschema.IndexKind, indexProperties(&indexSettings{}))
	result[0].Absorbs = []schemaext.Absorption{{Attribute: schemaext.IndexTypeAttribute, Key: "type"}}
	return result
}

// DecodeProperties decodes an ordered batch of index platform properties into
// DesiredIndex values without changing input maps. Unknown properties, invalid
// values or states, and mixed value/state spellings wrap schemaext.ErrInvalidValue.
// The target must be clickhouse and the format must be IndexPlatformProperties.
// Nil context is invalid. Any error or cancellation returns no partial batch;
// an empty batch still validates the context, target, and format.
func (IndexService) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := validateRequest(ctx, request.Target, request.Format, schemaext.IndexPlatformProperties); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := decodeIndex(fragment)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func decodeIndex(fragment schemaext.PropertyFragment) (*chschema.DesiredIndex, error) {
	if fragment.Kind != chschema.IndexKind {
		return nil, fmt.Errorf("%w: ClickHouse index source kind %q", schemaext.ErrInvalidValue, fragment.Kind)
	}
	var settings indexSettings
	if err := decodeSettings(fragment.Properties, indexProperties(&settings)); err != nil {
		return nil, err
	}
	value := &chschema.DesiredIndex{IndexType: settings.indexType, Granularity: chschema.GranularitySetting{State: settings.granularity.State}}
	if settings.granularity.State == chschema.Explicit {
		granularity, err := strconv.ParseUint(strings.TrimSpace(settings.granularity.Value), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: ClickHouse index granularity must be a positive decimal uint64", schemaext.ErrInvalidValue)
		}
		value.Granularity.Value = granularity
	}
	if err := chschema.ValidateDesiredIndex(value); err != nil {
		return nil, err
	}
	return value, nil
}

// EncodeProperties preserves unmanaged, default, and explicit settings, including
// full uint64 granularity. It accepts only DesiredIndex values; wrong models and
// invalid settings wrap schemaext.ErrInvalidValue. Returned maps are independent.
// Context, target, format, and atomic-batch behavior match DecodeProperties.
func (IndexService) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := validateRequest(ctx, request.Target, request.Format, schemaext.IndexPlatformProperties); err != nil {
		return nil, err
	}
	result := make([]schemaext.PropertyFragment, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		index, ok := value.(*chschema.DesiredIndex)
		if !ok {
			return nil, fmt.Errorf("%w: ClickHouse source expected desired index, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := chschema.ValidateDesiredIndex(index); err != nil {
			return nil, err
		}
		settings := indexSettings{indexType: index.IndexType, granularity: chschema.Setting{State: index.Granularity.State}}
		if index.Granularity.State == chschema.Explicit {
			settings.granularity.Value = strconv.FormatUint(index.Granularity.Value, 10)
		}
		result = append(result, encodeSettings(chschema.IndexKind, indexProperties(&settings)))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
