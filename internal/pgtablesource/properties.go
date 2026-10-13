// Package pgtablesource reads and writes PostgreSQL table features through
// source contracts. It performs no database inspection.
package pgtablesource

import (
	"context"
	"fmt"
	"strconv"

	"ptah.run/core/annotation"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgtable"
)

// Service carries table persistence through string-valued platform properties.
// Its zero value is ready for concurrent use.
type Service struct{}

// Definitions claims the PostgreSQL table property unlogged. Go annotations
// spell it platform.postgres.unlogged; YAML uses the postgres platform group.
func Definitions() []schemaext.PropertyDefinition {
	return []schemaext.PropertyDefinition{{Kind: pgtable.PersistenceKind, Keys: []string{"unlogged"}}}
}

func sourceContext(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: PostgreSQL table source requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if platform.NormalizeDialect(target) != platform.Postgres {
		return fmt.Errorf("%w: PostgreSQL table source target %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

func propertyContext(ctx context.Context, target string, format schemaext.PropertyFormat) error {
	if err := sourceContext(ctx, target); err != nil {
		return err
	}
	if format != schemaext.TablePlatformProperties {
		return fmt.Errorf("%w: PostgreSQL table property format %q", ptaherr.ErrUnsupportedFeature, format)
	}
	return nil
}

// DecodeProperties preserves explicit true and false declarations. Each
// fragment must contain only unlogged, spelled true or false. A malformed
// batch, unsupported target or cancellation returns no partial values.
func (Service) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := propertyContext(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		text, found := fragment.Properties["unlogged"]
		if fragment.Kind != pgtable.PersistenceKind || len(fragment.Properties) != 1 || !found || text != "true" && text != "false" {
			return nil, fmt.Errorf("%w: PostgreSQL table persistence requires only unlogged=true or unlogged=false", schemaext.ErrInvalidValue)
		}
		result = append(result, &pgtable.DesiredPersistence{Unlogged: text == "true"})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// EncodeProperties preserves explicit default declarations as unlogged=false.
// Invalid models, unsupported targets and cancellation return no partial batch.
// Returned maps are independent of each other and of the input values.
func (Service) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := propertyContext(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.PropertyFragment, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		desired, ok := value.(*pgtable.DesiredPersistence)
		if !ok {
			return nil, fmt.Errorf("%w: PostgreSQL table properties require a declaration, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := pgtable.ValidateDesiredPersistence(desired); err != nil {
			return nil, err
		}
		result = append(result, schemaext.PropertyFragment{Kind: pgtable.PersistenceKind,
			Properties: map[string]string{"unlogged": strconv.FormatBool(desired.Unlogged)}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Coverage records that the source can declare persistence for every table.
// An absent facet requests the logged default.
func Coverage() (schemaext.Coverage, error) {
	return pgtable.PersistenceCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// Annotations declares the owner's coverage through table platform properties.
// Service decodes those properties after the caller selects the target.
func Annotations() annotation.Extension {
	return annotation.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.PersistenceKind}, Coverage: annotation.Unlimited(Coverage), Encoder: annotationEncoder{}, Encodes: []schemaext.Kind{pgtable.PersistenceKind}}
}

// YAML declares coverage for the table's PostgreSQL platform properties.
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.PersistenceKind}, Coverage: Coverage}
}

// The annotation encoder owns coverage refusal even when no table carries a
// facet. Property conversion alone cannot account for an unknown observation.
type annotationEncoder struct{}

func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if ctx == nil {
		return annotation.EncodeResult{}, fmt.Errorf("%w: table persistence annotations require a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return annotation.EncodeResult{}, err
	}
	if err := exportCoverage(request.Coverage); err != nil {
		return annotation.EncodeResult{}, err
	}
	result := annotation.EncodeResult{}
	for _, slot := range request.Slots {
		if err := sourceContext(ctx, request.Target); err != nil {
			return annotation.EncodeResult{}, err
		}
		if slot.Slot.Directive != "ptah:schema:table" || slot.Slot.PrimaryKey {
			return annotation.EncodeResult{}, fmt.Errorf("%w: table persistence requires a table declaration, got %s", schemaext.ErrInvalidValue, slot.Slot)
		}
		for _, target := range slot.Facets.TargetScope(pgtable.PersistenceKind) {
			if !pgtable.SupportsPersistence(target) {
				return annotation.EncodeResult{}, fmt.Errorf("%w: Go table persistence cannot preserve target scope %q", ptaherr.ErrUnsupportedFeature, target)
			}
		}
		value, _, err := pgtable.PersistenceOf(slot.Facets)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Attributes = append(result.Attributes, annotation.SlotAttributes{Slot: slot.Slot,
			Attributes: []annotation.EncodedAttribute{{Name: "platform.postgres.unlogged", Value: strconv.FormatBool(value.Unlogged)}}})
	}
	if err := ctx.Err(); err != nil {
		return annotation.EncodeResult{}, err
	}
	return result, nil
}
