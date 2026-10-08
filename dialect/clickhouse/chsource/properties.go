// Package chsource encodes ClickHouse table intent in source property groups.
// Source syntax is decoded by a frontend; table-setting semantics stay here.
package chsource

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// Service preserves omitted, default, and explicit table-setting intent through
// string-valued platform properties. It performs no inspection or resolution.
type Service struct{}

type property struct {
	name  string
	value *chschema.Setting
}

// One vocabulary supplies registration, encoding, and decoding. Separate lists
// would let a newly supported key escape the runtime's ownership validation.
func tableProperties(table *chschema.DesiredTable) []property {
	return []property{
		{"engine", &table.Engine}, {"order_by", &table.OrderBy}, {"primary_key", &table.PrimaryKey},
		{"partition_by", &table.PartitionBy}, {"sample_by", &table.SampleBy}, {"ttl", &table.TTL}, {"settings", &table.Settings},
	}
}

// Definitions returns independent property ownership declarations. A bare key
// carries an explicit value, including empty. The same key followed by .state
// carries the value default; the two spellings cannot occur together. Omitted
// settings emit neither property. Go annotations prefix these keys with
// platform.clickhouse.; YAML places them in the ClickHouse platform group.
func Definitions() []schemaext.PropertyDefinition {
	var table chschema.DesiredTable
	definition := schemaext.PropertyDefinition{Kind: chschema.TableKind}
	for _, property := range tableProperties(&table) {
		definition.Keys = append(definition.Keys, property.name, property.name+".state")
	}
	return []schemaext.PropertyDefinition{definition}
}

func validateRequest(ctx context.Context, target string, format schemaext.PropertyFormat) error {
	if ctx == nil {
		return fmt.Errorf("%w: ClickHouse source requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target != "clickhouse" {
		return fmt.Errorf("%w: ClickHouse source target %q", ptaherr.ErrUnsupportedDialect, target)
	}
	if format != schemaext.TablePlatformProperties {
		return fmt.Errorf("%w: ClickHouse source format %q", ptaherr.ErrUnsupportedFeature, format)
	}
	return nil
}

// DecodeProperties decodes an ordered batch into DesiredTable values. Unknown
// properties, unsupported states, and conflicting state/value declarations wrap
// schemaext.ErrInvalidValue. Any failure or cancellation returns no partial batch.
func (Service) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := validateRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := decodeTable(fragment)
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

func decodeTable(fragment schemaext.PropertyFragment) (*chschema.DesiredTable, error) {
	if fragment.Kind != chschema.TableKind {
		return nil, fmt.Errorf("%w: ClickHouse source kind %q", schemaext.ErrInvalidValue, fragment.Kind)
	}
	result := &chschema.DesiredTable{}
	consumed := 0
	for _, property := range tableProperties(result) {
		value, explicit := fragment.Properties[property.name]
		state, stated := fragment.Properties[property.name+".state"]
		switch {
		case stated:
			if explicit || state != string(chschema.Default) {
				return nil, fmt.Errorf("%w: ClickHouse property %s requires either a value or .state=default", schemaext.ErrInvalidValue, property.name)
			}
			*property.value = chschema.Setting{State: chschema.Default}
			consumed++
		case explicit:
			*property.value = chschema.Setting{State: chschema.Explicit, Value: value}
			consumed++
		}
	}
	if consumed != len(fragment.Properties) {
		return nil, fmt.Errorf("%w: unknown ClickHouse table source property", schemaext.ErrInvalidValue)
	}
	if err := chschema.ValidateDesired(result); err != nil {
		return nil, err
	}
	return result, nil
}

// EncodeProperties preserves every desired setting, including explicit empty
// values and default requests. Unknown model types and invalid settings wrap
// schemaext.ErrInvalidValue. Returned maps do not alias inputs or each other.
// Any failure or cancellation returns no partial batch.
func (Service) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := validateRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.PropertyFragment, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		table, ok := value.(*chschema.DesiredTable)
		if !ok {
			return nil, fmt.Errorf("%w: ClickHouse source expected desired table, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := chschema.ValidateDesired(table); err != nil {
			return nil, err
		}
		fragment := schemaext.PropertyFragment{Kind: chschema.TableKind, Properties: make(map[string]string)}
		for _, property := range tableProperties(table) {
			switch property.value.State {
			case chschema.Default:
				fragment.Properties[property.name+".state"] = string(chschema.Default)
			case chschema.Explicit:
				fragment.Properties[property.name] = property.value.Value
			}
		}
		result = append(result, fragment)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
