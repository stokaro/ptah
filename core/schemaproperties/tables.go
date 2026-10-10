// Package schemaproperties moves source properties through the selected
// feature owners. It owns attachment and target scope, not property semantics.
package schemaproperties

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/ptaherr"
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
	// exclusive reports that only feature owners read this format, so a key
	// of the selected target that no owner claims is refused.
	exclusive bool
}

type propertyOwner struct {
	label      string
	name       string
	facets     *schemaext.Facets
	properties *map[string]map[string]string
	// indexType is the common index type an owner's definition may absorb,
	// and nil for an owner that has none. Only a declared absorption moves a
	// value out of it.
	indexType *string
}

// commonField returns the owner's common declaration field for attribute, or
// nil when the owner has no such field.
func (o propertyOwner) commonField(attribute schemaext.CommonAttribute) *string {
	if attribute == schemaext.IndexTypeAttribute {
		return o.indexType
	}
	return nil
}

// selection is a resolved target and the property definitions it has for one
// format. It is computed before any schema data is copied.
type selection struct {
	target      schemaext.TargetSelection
	definitions []schemaext.PropertyDefinition
}

func selectTarget(ctx context.Context, db *schemamodel.Database, target string, format schemaext.PropertyFormat, runtime Runtime) (selection, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return selection{}, err
	}
	if db == nil {
		return selection{}, fmt.Errorf("%w: source properties require a schema", schemaext.ErrInvalidValue)
	}
	selected, err := runtime.ResolveTarget(target)
	if err != nil {
		return selection{}, err
	}
	formats, err := runtime.PropertyFormats(selected.Name())
	if err != nil {
		return selection{}, err
	}
	result := selection{target: selected}
	if slices.Contains(formats, format) {
		result.definitions, err = runtime.PropertyDefinitions(selected.Name(), format)
		if err != nil {
			return selection{}, err
		}
	}
	return result, nil
}

// capture copies the format's owners so the batch can change them without
// touching the caller's schema. Other schema data stays shared and read-only.
func capture(db *schemamodel.Database, chosen selection, format schemaext.PropertyFormat) propertyBatch {
	result := *db
	isolateOwners(&result, format)
	owners, exclusive := ownerSlots(&result, format)
	return propertyBatch{database: &result, target: chosen.target, definitions: chosen.definitions, format: format, owners: owners, exclusive: exclusive}
}

// isolateOwners replaces the slice that holds one format's owners with
// independent copies of them.
func isolateOwners(db *schemamodel.Database, format schemaext.PropertyFormat) {
	if format == schemaext.IndexPlatformProperties {
		db.Indexes = slices.Clone(db.Indexes)
		for i := range db.Indexes {
			db.Indexes[i] = db.Indexes[i].Clone()
		}
		return
	}
	db.Tables = slices.Clone(db.Tables)
	for i := range db.Tables {
		db.Tables[i] = db.Tables[i].Clone()
	}
}

// ownerSlots lists the model slots that hold one format's properties, and
// whether nothing but feature owners reads them. This is the core's knowledge
// of the model; which common fields an owner absorbs is the owner's
// declaration, never inferred here.
func ownerSlots(db *schemamodel.Database, format schemaext.PropertyFormat) ([]propertyOwner, bool) {
	if format == schemaext.IndexPlatformProperties {
		owners := make([]propertyOwner, 0, len(db.Indexes))
		for i := range db.Indexes {
			index := &db.Indexes[i]
			owners = append(owners, propertyOwner{"index", index.Name, &index.Facets, &index.Overrides, &index.Type})
		}
		// Index properties have no reader besides their feature owners.
		return owners, true
	}
	owners := make([]propertyOwner, 0, len(db.Tables))
	for i := range db.Tables {
		table := &db.Tables[i]
		owners = append(owners, propertyOwner{"table", table.QualifiedName(), &table.Facets, &table.Overrides, nil})
	}
	// Table properties also carry common target options with readers of
	// their own, such as a MySQL engine, so unclaimed keys stay for them.
	return owners, false
}

// needsDecoding reports whether any owner holds properties, or a common field
// a selected definition absorbs. Without either, decoding changes nothing, and
// the copy is returned without running the batch.
func needsDecoding(owners []propertyOwner, definitions []schemaext.PropertyDefinition) bool {
	return slices.ContainsFunc(owners, func(owner propertyOwner) bool {
		if len(*owner.properties) > 0 {
			return true
		}
		return slices.ContainsFunc(definitions, func(definition schemaext.PropertyDefinition) bool {
			return slices.ContainsFunc(definition.Absorbs, func(absorption schemaext.Absorption) bool {
				field := owner.commonField(absorption.Attribute)
				return field != nil && *field != ""
			})
		})
	})
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
	chosen, err := selectTarget(ctx, db, target, format, runtime)
	if err != nil {
		return nil, err
	}
	batch := capture(db, chosen, format)
	if !needsDecoding(batch.owners, batch.definitions) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return batch.database, nil
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
		if err := batch.refuseUnclaimed(owner); err != nil {
			return nil, err
		}
	}
	if err := batch.decode(ctx, runtime, fragments, owners); err != nil {
		return nil, err
	}
	return batch.database, nil
}

// refuseUnclaimed reports a key of the selected target that no owner claimed,
// for a format nothing else reads: it would be accepted and then ignored.
func (b propertyBatch) refuseUnclaimed(owner propertyOwner) error {
	if !b.exclusive {
		return nil
	}
	for _, name := range slices.Sorted(maps.Keys(*owner.properties)) {
		if !b.target.Includes([]string{name}) {
			continue
		}
		if keys := slices.Sorted(maps.Keys((*owner.properties)[name])); len(keys) > 0 {
			return fmt.Errorf("%w: %s %q declares property %q for %q, which no feature owner of target %q claims",
				ptaherr.ErrUnsupportedFeature, owner.label, owner.name, keys[0], name, b.target.Name())
		}
	}
	return nil
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
	for _, absorption := range definition.Absorbs {
		if field := owner.commonField(absorption.Attribute); field != nil && *field != "" {
			properties[absorption.Key] = *field
			*field = ""
		}
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
