package mysqlsource

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// The column property keys, as a Go annotation writes them after
// `platform.<target>.` and a YAML document under a column's `platform` group.
const (
	CharsetProperty  = "charset"
	OnUpdateProperty = "on_update"
)

// ColumnService decodes and encodes the column settings of
// [mysqlschema.ColumnSettingsKind] in [schemaext.ColumnPlatformProperties].
// It serves the targets [mysqlschema.Targets] names.
type ColumnService struct{}

// ColumnDefinitions declares the column property keys the settings own.
func ColumnDefinitions() []schemaext.PropertyDefinition {
	return []schemaext.PropertyDefinition{{Kind: mysqlschema.ColumnSettingsKind, Keys: []string{CharsetProperty, OnUpdateProperty}}}
}

func validateColumnRequest(ctx context.Context, target string, format schemaext.PropertyFormat) error {
	if ctx == nil {
		return fmt.Errorf("%w: MySQL source requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !slices.Contains(mysqlschema.Targets(), target) {
		return fmt.Errorf("%w: MySQL source target %q", ptaherr.ErrUnsupportedDialect, target)
	}
	if format != schemaext.ColumnPlatformProperties {
		return fmt.Errorf("%w: MySQL column source format %q", ptaherr.ErrUnsupportedFeature, format)
	}
	return nil
}

// DecodeProperties reads each fragment into a [mysqlschema.DesiredColumnSettings].
// A value is trimmed; a value that is empty after trimming is refused, since
// leaving the key out is how a declaration states nothing.
func (ColumnService) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := validateColumnRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if fragment.Kind != mysqlschema.ColumnSettingsKind {
			return nil, fmt.Errorf("%w: MySQL column source kind %q", schemaext.ErrInvalidValue, fragment.Kind)
		}
		declared := &mysqlschema.DesiredColumnSettings{}
		for key, value := range fragment.Properties {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				return nil, fmt.Errorf("%w: MySQL column property %q is empty; leave it out instead", schemaext.ErrInvalidValue, key)
			}
			switch key {
			case CharsetProperty:
				declared.Charset = trimmed
			case OnUpdateProperty:
				declared.OnUpdate = trimmed
			default:
				return nil, fmt.Errorf("%w: MySQL column property %q", schemaext.ErrInvalidValue, key)
			}
		}
		if err := mysqlschema.ValidateDesiredColumnSettings(declared); err != nil {
			return nil, err
		}
		result = append(result, declared)
	}
	return result, ctx.Err()
}

// EncodeProperties writes each declaration as the keys it states.
func (ColumnService) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := validateColumnRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.PropertyFragment, 0, len(request.Values))
	for _, value := range request.Values {
		declared, ok := value.(*mysqlschema.DesiredColumnSettings)
		if !ok {
			return nil, fmt.Errorf("%w: MySQL column source expected desired column settings, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := mysqlschema.ValidateDesiredColumnSettings(declared); err != nil {
			return nil, err
		}
		fragment := schemaext.PropertyFragment{Kind: mysqlschema.ColumnSettingsKind, Properties: make(map[string]string)}
		if declared.Charset != "" {
			fragment.Properties[CharsetProperty] = declared.Charset
		}
		if declared.OnUpdate != "" {
			fragment.Properties[OnUpdateProperty] = declared.OnUpdate
		}
		result = append(result, fragment)
	}
	return result, ctx.Err()
}
