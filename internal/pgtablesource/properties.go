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
	"ptah.run/dialect/postgres/pgtablerender"
)

// Service carries table persistence and partition keys through platform properties.
// Its zero value is ready for concurrent use.
type Service struct{}

// Definitions claims the unlogged and partition_by table properties. Go and YAML
// sources place them in the selected target's platform group.
func Definitions() []schemaext.PropertyDefinition {
	return []schemaext.PropertyDefinition{{Kind: pgtable.PersistenceKind, Keys: []string{"unlogged"}}, {Kind: pgtable.PartitionKind, Keys: []string{"partition_by"}}}
}

func sourceContext(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: PostgreSQL table source requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !pgtable.SupportsPartition(target) {
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

// DecodeProperties reads explicit persistence values and complete partition keys.
// Each fragment carries one property. A malformed batch, unsupported target or
// cancellation returns no partial values.
func (Service) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := propertyContext(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fragment.Kind == pgtable.PartitionKind {
			text, found := fragment.Properties["partition_by"]
			if !found || len(fragment.Properties) != 1 {
				return nil, partitionSyntax("table partition property requires only partition_by")
			}
			value, err := ParsePartitionSQL(text)
			if err != nil {
				return nil, err
			}
			result = append(result, value)
			continue
		}
		text, found := fragment.Properties["unlogged"]
		if !pgtable.SupportsPersistence(request.Target) || fragment.Kind != pgtable.PersistenceKind || len(fragment.Properties) != 1 || !found || text != "true" && text != "false" {
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
		if partition, ok := value.(*pgtable.DesiredPartition); ok {
			text, err := pgtablerender.PartitionSQL(partition)
			if err != nil {
				return nil, err
			}
			result = append(result, schemaext.PropertyFragment{Kind: pgtable.PartitionKind, Properties: map[string]string{"partition_by": text}})
			continue
		}
		desired, ok := value.(*pgtable.DesiredPersistence)
		if !ok || !pgtable.SupportsPersistence(request.Target) {
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

// Coverage records that the source can describe persistence and partitioning
// for every table. Missing facets request an ordinary logged table.
func Coverage() (schemaext.Coverage, error) {
	persistence, err := pgtable.PersistenceCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	partition, err := pgtable.PartitionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return persistence.Combine(partition)
}

// Annotations declares the owner's coverage through table platform properties.
// Service decodes those properties after the caller selects the target.
func Annotations() annotation.Extension {
	return annotation.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.PersistenceKind, pgtable.PartitionKind}, Coverage: annotation.Unlimited(Coverage),
		Encoder: annotationEncoder{}, Encodes: []schemaext.Kind{pgtable.PersistenceKind, pgtable.PartitionKind}}
}

// YAML declares coverage for the table's PostgreSQL platform properties.
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.PersistenceKind, pgtable.PartitionKind}, Coverage: Coverage}
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
		attributes, err := encodeTableSlot(request.Target, slot)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Attributes = append(result.Attributes, attributes...)
	}
	if err := ctx.Err(); err != nil {
		return annotation.EncodeResult{}, err
	}
	return result, nil
}

func encodeTableSlot(target string, slot annotation.SlotFacets) ([]annotation.SlotAttributes, error) {
	var attributes []annotation.SlotAttributes
	if slot.Slot.Directive != "ptah:schema:table" || slot.Slot.PrimaryKey {
		return nil, fmt.Errorf("%w: table persistence requires a table declaration, got %s", schemaext.ErrInvalidValue, slot.Slot)
	}
	for _, target := range slot.Facets.TargetScope(pgtable.PersistenceKind) {
		if !pgtable.SupportsPersistence(target) {
			return nil, fmt.Errorf("%w: Go table persistence cannot preserve target scope %q", ptaherr.ErrUnsupportedFeature, target)
		}
	}
	partition, found, err := pgtable.PartitionOf(slot.Facets)
	if err != nil {
		return nil, err
	}
	if found {
		for _, bound := range slot.Facets.TargetScope(pgtable.PartitionKind) {
			if platform.NormalizeDialect(bound) != platform.NormalizeDialect(target) {
				return nil, fmt.Errorf("%w: Go table partition cannot preserve target scope %q", ptaherr.ErrUnsupportedFeature, bound)
			}
		}
		text, err := pgtablerender.PartitionSQL(&pgtable.DesiredPartition{Type: partition.Type, Parts: partition.Parts})
		if err != nil {
			return nil, err
		}
		attributes = append(attributes, annotation.SlotAttributes{Slot: slot.Slot, Attributes: []annotation.EncodedAttribute{{Name: "platform." + platform.NormalizeDialect(target) + ".partition_by", Value: text}}})
	}
	value, found, err := pgtable.PersistenceOf(slot.Facets)
	if err != nil {
		return nil, err
	}
	if !found {
		return attributes, nil
	}
	if !pgtable.SupportsPersistence(target) {
		return nil, fmt.Errorf("%w: Go table persistence on %s", ptaherr.ErrUnsupportedFeature, target)
	}
	attributes = append(attributes, annotation.SlotAttributes{Slot: slot.Slot,
		Attributes: []annotation.EncodedAttribute{{Name: "platform.postgres.unlogged", Value: strconv.FormatBool(value.Unlogged)}}})
	return attributes, nil
}
