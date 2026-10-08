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
	batch, err := capture(ctx, db, target, runtime)
	if err != nil {
		return nil, err
	}
	var values []schemaext.Value
	var tables []int
	for i := range batch.database.Tables {
		table := &batch.database.Tables[i]
		for _, kind := range table.Facets.DeclaredKinds() {
			value, err := batch.exportValue(table, kind)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			tables = append(tables, i)
		}
	}
	if err := batch.encode(ctx, runtime, values, tables); err != nil {
		return nil, err
	}
	return batch.database, nil
}

func (b tableBatch) encode(ctx context.Context, runtime Runtime, values []schemaext.Value, tables []int) error {
	if len(values) == 0 {
		return ctx.Err()
	}
	fragments, err := runtime.EncodeProperties(ctx, schemaext.PropertyEncodeRequest{
		Target: b.target.Name(), Format: schemaext.TablePlatformProperties, Values: values,
	})
	if err != nil {
		return err
	}
	if len(fragments) != len(values) {
		return fmt.Errorf("%w: table property encoder changed the fragment count", schemaext.ErrInvalidValue)
	}
	for i, fragment := range fragments {
		if fragment.Kind != values[i].Kind() {
			return fmt.Errorf("%w: table property encoder changed an ordered kind", schemaext.ErrInvalidValue)
		}
		if len(fragment.Properties) == 0 {
			return fmt.Errorf("%w: empty table property fragment would lose facet %q", ptaherr.ErrUnsupportedFeature, fragment.Kind)
		}
		table := &b.database.Tables[tables[i]]
		if table.Overrides == nil {
			table.Overrides = make(map[string]map[string]string)
		}
		if table.Overrides[b.target.Name()] == nil {
			table.Overrides[b.target.Name()] = make(map[string]string)
		}
		maps.Copy(table.Overrides[b.target.Name()], fragment.Properties)
		table.Facets = table.Facets.Without(fragment.Kind)
	}
	return ctx.Err()
}

func (b tableBatch) exportValue(table *schemamodel.Table, kind schemaext.Kind) (schemaext.Value, error) {
	value, found, err := table.Facets.Get(kind)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%w: table %q has an excluded facet %q", ptaherr.ErrUnsupportedFeature, table.QualifiedName(), kind)
	}
	for _, target := range table.Facets.TargetScope(kind) {
		if !b.target.Includes([]string{target}) {
			return nil, fmt.Errorf("%w: table %q facet %q has a binding outside target %q", ptaherr.ErrUnsupportedFeature, table.QualifiedName(), kind, b.target.Name())
		}
	}
	index := slices.IndexFunc(b.definitions, func(definition schemaext.PropertyDefinition) bool { return definition.Kind == kind })
	if index < 0 {
		return nil, fmt.Errorf("%w: table %q facet %q has no source property codec", ptaherr.ErrUnsupportedFeature, table.QualifiedName(), kind)
	}
	properties, err := takeProperties(table, b.target, b.definitions[index])
	if err != nil {
		return nil, err
	}
	if len(properties) != 0 {
		return nil, fmt.Errorf("%w: table %q declares both properties and facet %q", schemaext.ErrDuplicate, table.QualifiedName(), kind)
	}
	return value, nil
}
