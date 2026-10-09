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

// EncodeTables exports table facets as properties for one selected target.
// It refuses excluded facets, bindings that include another target, unsupported
// kinds, and properties already claimed by an encoded facet. Unrestricted
// facets become properties scoped to the selected target. Other schema slots
// remain unchanged; their exporters must represent or refuse them separately.
// Inputs are not mutated; tables are copied and other data is shared read-only.
// Errors or cancellation return no schema, including after a partial batch.
func EncodeTables(ctx context.Context, db *schemamodel.Database, target string, runtime Runtime) (*schemamodel.Database, error) {
	return encode(ctx, db, target, schemaext.TablePlatformProperties, runtime)
}

func encode(ctx context.Context, db *schemamodel.Database, target string, format schemaext.PropertyFormat, runtime Runtime) (*schemamodel.Database, error) {
	chosen, err := selectTarget(ctx, db, target, format, runtime)
	if err != nil {
		return nil, err
	}
	slots, _ := ownerSlots(db, format)
	if !slices.ContainsFunc(slots, func(owner propertyOwner) bool { return len(owner.facets.DeclaredKinds()) > 0 }) {
		return db, ctx.Err()
	}
	batch := capture(db, chosen, format)
	var values []schemaext.Value
	var owners []int
	for i, owner := range batch.owners {
		for _, kind := range owner.facets.DeclaredKinds() {
			value, err := batch.exportValue(owner, kind)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			owners = append(owners, i)
		}
	}
	if err := batch.encode(ctx, runtime, values, owners); err != nil {
		return nil, err
	}
	return batch.database, nil
}

func (b propertyBatch) encode(ctx context.Context, runtime Runtime, values []schemaext.Value, owners []int) error {
	if len(values) == 0 {
		return ctx.Err()
	}
	fragments, err := runtime.EncodeProperties(ctx, schemaext.PropertyEncodeRequest{
		Target: b.target.Name(), Format: b.format, Values: values,
	})
	if err != nil {
		return err
	}
	if len(fragments) != len(values) {
		return fmt.Errorf("%w: property encoder changed the fragment count", schemaext.ErrInvalidValue)
	}
	for i, fragment := range fragments {
		if fragment.Kind != values[i].Kind() {
			return fmt.Errorf("%w: property encoder changed an ordered kind", schemaext.ErrInvalidValue)
		}
		if len(fragment.Properties) == 0 {
			return fmt.Errorf("%w: empty property fragment would lose facet %q", ptaherr.ErrUnsupportedFeature, fragment.Kind)
		}
		owner := b.owners[owners[i]]
		if *owner.properties == nil {
			*owner.properties = make(map[string]map[string]string)
		}
		if (*owner.properties)[b.target.Name()] == nil {
			(*owner.properties)[b.target.Name()] = make(map[string]string)
		}
		maps.Copy((*owner.properties)[b.target.Name()], fragment.Properties)
		*owner.facets = owner.facets.Without(fragment.Kind)
	}
	return ctx.Err()
}

func (b propertyBatch) exportValue(owner propertyOwner, kind schemaext.Kind) (schemaext.Value, error) {
	value, found, err := owner.facets.Get(kind)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%w: %s %q has an excluded facet %q", ptaherr.ErrUnsupportedFeature, owner.label, owner.name, kind)
	}
	for _, target := range owner.facets.TargetScope(kind) {
		if !b.target.Includes([]string{target}) {
			return nil, fmt.Errorf("%w: %s %q facet %q has a binding outside target %q", ptaherr.ErrUnsupportedFeature, owner.label, owner.name, kind, b.target.Name())
		}
	}
	index := slices.IndexFunc(b.definitions, func(definition schemaext.PropertyDefinition) bool { return definition.Kind == kind })
	if index < 0 {
		return nil, fmt.Errorf("%w: %s %q facet %q has no source property codec", ptaherr.ErrUnsupportedFeature, owner.label, owner.name, kind)
	}
	properties, err := takeProperties(owner, b.target, b.definitions[index])
	if err != nil {
		return nil, err
	}
	if len(properties) != 0 {
		return nil, fmt.Errorf("%w: %s %q declares both properties and facet %q", schemaext.ErrDuplicate, owner.label, owner.name, kind)
	}
	return value, nil
}
