package mysqlsource

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// Service decodes and encodes table options as string-valued platform
// properties of the mysql and mariadb targets. It performs no inspection.
// Its zero value is ready for concurrent use.
type Service struct{}

// tableProperties is the one vocabulary that registration, encoding and
// decoding share, so a newly supported key cannot escape ownership checks.
func tableProperties(table *mysqlschema.DesiredTable) map[string]*string {
	return map[string]*string{"engine": &table.Engine, "auto_increment": &table.AutoIncrement, "charset": &table.Charset}
}

// Definitions returns independent property ownership declarations: the keys
// engine, auto_increment and charset. Go annotations prefix the keys with
// platform.mysql. or platform.mariadb.; YAML places them in the target's
// platform group. A table's common engine, the bare `engine` of a
// declaration, stays common: an engine stated here is written over it.
func Definitions() []schemaext.PropertyDefinition {
	var table mysqlschema.DesiredTable
	return []schemaext.PropertyDefinition{{Kind: mysqlschema.TableKind, Keys: slices.Sorted(maps.Keys(tableProperties(&table)))}}
}

func validateRequest(ctx context.Context, target string, format schemaext.PropertyFormat) error {
	if ctx == nil {
		return fmt.Errorf("%w: MySQL source requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target != platform.MySQL && target != platform.MariaDB {
		return fmt.Errorf("%w: MySQL source target %q", ptaherr.ErrUnsupportedDialect, target)
	}
	if format != schemaext.TablePlatformProperties {
		return fmt.Errorf("%w: MySQL source format %q", ptaherr.ErrUnsupportedFeature, format)
	}
	return nil
}

// DecodeProperties decodes an ordered batch into DesiredTable values. A
// property with an empty value states nothing, as the option left out does.
// An unknown property, and options the model refuses, wrap
// schemaext.ErrInvalidValue. Any failure or cancellation returns no partial
// batch.
func (Service) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := validateRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fragment.Kind != mysqlschema.TableKind {
			return nil, fmt.Errorf("%w: MySQL source kind %q", schemaext.ErrInvalidValue, fragment.Kind)
		}
		table := &mysqlschema.DesiredTable{}
		properties := tableProperties(table)
		for _, key := range slices.Sorted(maps.Keys(fragment.Properties)) {
			target, known := properties[key]
			if !known {
				return nil, fmt.Errorf("%w: unknown MySQL table property %q", schemaext.ErrInvalidValue, key)
			}
			*target = fragment.Properties[key]
		}
		if err := mysqlschema.ValidateDesiredTable(table); err != nil {
			return nil, err
		}
		result = append(result, table)
	}
	return result, ctx.Err()
}

// EncodeProperties writes each declared option as its property and leaves an
// option the declaration leaves out unwritten. Unknown model types and invalid
// options wrap schemaext.ErrInvalidValue. Returned maps do not alias inputs.
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
		table, ok := value.(*mysqlschema.DesiredTable)
		if !ok {
			return nil, fmt.Errorf("%w: MySQL source expected desired table options, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := mysqlschema.ValidateDesiredTable(table); err != nil {
			return nil, err
		}
		fragment := schemaext.PropertyFragment{Kind: mysqlschema.TableKind, Properties: make(map[string]string)}
		for key, option := range tableProperties(table) {
			if *option != "" {
				fragment.Properties[key] = *option
			}
		}
		result = append(result, fragment)
	}
	return result, ctx.Err()
}
