package ydbsource

import (
	"context"
	"fmt"
	"maps"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/rowdeletion"
	"ptah.run/internal/ydbttl"
)

// ttlProperties are the properties a YDB TTL takes, in the order a refusal
// lists them.
var ttlProperties = []string{rowdeletion.ColumnProperty, rowdeletion.IntervalProperty, rowdeletion.UnitProperty}

// TTLDefinitions returns independent property ownership declarations for a
// table's TTL: row_deletion_column, row_deletion_interval as an ISO 8601
// duration such as P30D, and row_deletion_unit for an integer column. The
// owner claims every key that begins with row_deletion in any case, so a
// misspelled or miscased property is refused by name rather than left as a
// table option nothing reads. Go annotations prefix these keys with
// platform.ydb.; YAML places them in the ydb platform group.
func TTLDefinitions() []schemaext.PropertyDefinition {
	return []schemaext.PropertyDefinition{{Kind: ydbschema.TTLKind, Keys: append([]string(nil), ttlProperties...), Prefixes: []string{rowdeletion.PropertyPrefix}}}
}

// TTLService carries a table's TTL through string-valued platform properties.
// It performs no inspection.
type TTLService struct{}

func validateTTLRequest(ctx context.Context, target string, format schemaext.PropertyFormat) error {
	if ctx == nil {
		return fmt.Errorf("%w: YDB source requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target != platform.YDB {
		return fmt.Errorf("%w: YDB source target %q", ptaherr.ErrUnsupportedDialect, target)
	}
	if format != schemaext.TablePlatformProperties {
		return fmt.Errorf("%w: YDB source format %q", ptaherr.ErrUnsupportedFeature, format)
	}
	return nil
}

// DecodeProperties decodes an ordered batch into DesiredTTL values. The unit
// is read in any case and kept in capitals. An unknown property, a blank
// value, a TTL without its column or its interval, and an interval or unit YDB
// refuses wrap schemaext.ErrInvalidValue. Any failure or cancellation returns
// no partial batch.
func (TTLService) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := validateTTLRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fragment.Kind != ydbschema.TTLKind {
			return nil, fmt.Errorf("%w: YDB source kind %q", schemaext.ErrInvalidValue, fragment.Kind)
		}
		declaration, err := rowdeletion.Decode(maps.Clone(fragment.Properties), ttlProperties)
		if err != nil {
			return nil, err
		}
		unit, err := ydbttl.Unit(declaration.Unit)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", schemaext.ErrInvalidValue, rowdeletion.UnitProperty, err)
		}
		value := &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: declaration.Column, Interval: declaration.Interval, Unit: unit}}
		if err := ydbschema.ValidateDesiredTTL(value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// EncodeProperties writes the column, the interval and any unit of a valid
// declaration. Unknown model types and invalid declarations wrap
// schemaext.ErrInvalidValue. Returned maps do not alias inputs or each other.
// Any failure or cancellation returns no partial batch.
func (TTLService) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := validateTTLRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.PropertyFragment, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		declared, ok := value.(*ydbschema.DesiredTTL)
		if !ok {
			return nil, fmt.Errorf("%w: YDB source expected a desired TTL, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := ydbschema.ValidateDesiredTTL(declared); err != nil {
			return nil, err
		}
		policy := declared.Policy
		result = append(result, schemaext.PropertyFragment{
			Kind:       ydbschema.TTLKind,
			Properties: rowdeletion.Encode(rowdeletion.Declaration{Column: policy.Column, Interval: policy.Interval, Unit: policy.Unit}),
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
