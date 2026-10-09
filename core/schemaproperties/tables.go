// Package schemaproperties moves source properties through the selected
// feature owners. It owns attachment and target scope, not property semantics.
package schemaproperties

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Runtime supplies source ownership, codecs, and explicit target selection.
// Implementations must enforce PropertyService's complete-batch contract.
type Runtime interface {
	schemaext.PropertyRuntime
	schemaext.TargetResolver
}

type propertyBatch struct {
	database    *schemamodel.Database
	target      schemaext.TargetSelection
	definitions []schemaext.PropertyDefinition
	format      schemaext.PropertyFormat
	owners      []propertyOwner
}

type propertyOwner struct {
	label      string
	name       string
	facets     *schemaext.Facets
	properties *map[string]map[string]string
	commonType *string
}

func capture(ctx context.Context, db *schemamodel.Database, target string, format schemaext.PropertyFormat, runtime Runtime) (propertyBatch, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return propertyBatch{}, err
	}
	if db == nil {
		return propertyBatch{}, fmt.Errorf("%w: source properties require a schema", schemaext.ErrInvalidValue)
	}
	selected, err := runtime.ResolveTarget(target)
	if err != nil {
		return propertyBatch{}, err
	}
	formats, err := runtime.PropertyFormats(selected.Name())
	if err != nil {
		return propertyBatch{}, err
	}
	var definitions []schemaext.PropertyDefinition
	if slices.Contains(formats, format) {
		definitions, err = runtime.PropertyDefinitions(selected.Name(), format)
		if err != nil {
			return propertyBatch{}, err
		}
	}
	result := *db
	owners := clonePropertyOwners(&result, format)
	return propertyBatch{database: &result, target: selected, definitions: definitions, format: format, owners: owners}, nil
}

func clonePropertyOwners(db *schemamodel.Database, format schemaext.PropertyFormat) []propertyOwner {
	var owners []propertyOwner
	if format == schemaext.IndexPlatformProperties {
		db.Indexes = slices.Clone(db.Indexes)
		for i := range db.Indexes {
			db.Indexes[i] = db.Indexes[i].Clone()
			index := &db.Indexes[i]
			owners = append(owners, propertyOwner{"index", index.Name, &index.Facets, &index.Overrides, &index.Type})
		}
		return owners
	}
	db.Tables = slices.Clone(db.Tables)
	for i := range db.Tables {
		db.Tables[i] = db.Tables[i].Clone()
		table := &db.Tables[i]
		owners = append(owners, propertyOwner{"table", table.QualifiedName(), &table.Facets, &table.Overrides, nil})
	}
	return owners
}

// DecodeTables replaces claimed properties with desired table facets. It
// decodes only the selected target's property groups; other targets and keys
// remain untouched. Missing properties create no intent. Duplicate alias keys
// and mixed typed/property declarations are refused, including empty values.
// New facets retain the canonical target binding. Inputs are not mutated;
// table data is copied and other schema data remains shared and read-only.
// Errors and cancellation return no schema and confer no catalog coverage.
func DecodeTables(ctx context.Context, db *schemamodel.Database, target string, runtime Runtime) (*schemamodel.Database, error) {
	return decode(ctx, db, target, schemaext.TablePlatformProperties, runtime)
}

func decode(ctx context.Context, db *schemamodel.Database, target string, format schemaext.PropertyFormat, runtime Runtime) (*schemamodel.Database, error) {
	batch, err := capture(ctx, db, target, format, runtime)
	if err != nil {
		return nil, err
	}
	var fragments []schemaext.PropertyFragment
	var owners []int
	for i, owner := range batch.owners {
		for _, definition := range batch.definitions {
			properties, err := takeProperties(owner, batch.target, definition)
			if err != nil {
				return nil, err
			}
			if len(properties) == 0 {
				continue
			}
			if slices.Contains(owner.facets.DeclaredKinds(), definition.Kind) {
				return nil, fmt.Errorf("%w: %s %q declares both properties and facet %q", schemaext.ErrDuplicate, owner.label, owner.name, definition.Kind)
			}
			fragments = append(fragments, schemaext.PropertyFragment{Kind: definition.Kind, Properties: properties})
			owners = append(owners, i)
		}
	}
	if err := batch.decode(ctx, runtime, fragments, owners); err != nil {
		return nil, err
	}
	return batch.database, nil
}

func (b propertyBatch) decode(ctx context.Context, runtime Runtime, fragments []schemaext.PropertyFragment, owners []int) error {
	if len(fragments) == 0 {
		return ctx.Err()
	}
	values, err := runtime.DecodeProperties(ctx, schemaext.PropertyDecodeRequest{
		Target: b.target.Name(), Format: b.format, Fragments: fragments,
	})
	if err != nil {
		return err
	}
	if len(values) != len(owners) {
		return fmt.Errorf("%w: property decoder changed the value count", schemaext.ErrInvalidValue)
	}
	for i, value := range values {
		if value == nil || value.Kind() != fragments[i].Kind {
			return fmt.Errorf("%w: property decoder changed an ordered kind", schemaext.ErrInvalidValue)
		}
		owner := b.owners[owners[i]]
		*owner.facets, err = owner.facets.With(value)
		if err != nil {
			return err
		}
		*owner.facets, err = owner.facets.WithTargetScope(value.Kind(), b.target.Name())
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func takeProperties(owner propertyOwner, target schemaext.TargetSelection, definition schemaext.PropertyDefinition) (map[string]string, error) {
	properties := make(map[string]string)
	if owner.commonType != nil && *owner.commonType != "" && slices.Contains(definition.Keys, "type") {
		properties["type"] = *owner.commonType
		*owner.commonType = ""
	}
	for _, name := range slices.Sorted(maps.Keys(*owner.properties)) {
		if !target.Includes([]string{name}) {
			continue
		}
		for _, key := range definition.Keys {
			value, found := (*owner.properties)[name][key]
			if !found {
				continue
			}
			if _, duplicate := properties[key]; duplicate {
				return nil, fmt.Errorf("%w: %s %q repeats source property %q", schemaext.ErrDuplicate, owner.label, owner.name, key)
			}
			properties[key] = value
			delete((*owner.properties)[name], key)
		}
		if len((*owner.properties)[name]) == 0 {
			delete(*owner.properties, name)
		}
	}
	return properties, nil
}
