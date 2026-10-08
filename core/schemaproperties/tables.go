// Package schemaproperties moves table source properties through the selected
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

type tableBatch struct {
	database    *schemamodel.Database
	target      schemaext.TargetSelection
	definitions []schemaext.PropertyDefinition
}

func capture(ctx context.Context, db *schemamodel.Database, target string, runtime Runtime) (tableBatch, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return tableBatch{}, err
	}
	if db == nil {
		return tableBatch{}, fmt.Errorf("%w: table properties require a schema", schemaext.ErrInvalidValue)
	}
	selected, err := runtime.ResolveTarget(target)
	if err != nil {
		return tableBatch{}, err
	}
	formats, err := runtime.PropertyFormats(selected.Name())
	if err != nil {
		return tableBatch{}, err
	}
	var definitions []schemaext.PropertyDefinition
	if slices.Contains(formats, schemaext.TablePlatformProperties) {
		definitions, err = runtime.PropertyDefinitions(selected.Name(), schemaext.TablePlatformProperties)
		if err != nil {
			return tableBatch{}, err
		}
	}
	result := *db
	result.Tables = make([]schemamodel.Table, len(db.Tables))
	for i, table := range db.Tables {
		result.Tables[i] = table.Clone()
	}
	return tableBatch{database: &result, target: selected, definitions: definitions}, nil
}

// DecodeTables replaces claimed properties with desired table facets. It
// decodes only the selected target's property groups; other targets and keys
// remain untouched. Missing properties create no intent. Duplicate alias keys
// and mixed typed/property declarations are refused, including empty values.
// New facets retain the canonical target binding. Inputs are not mutated;
// table data is copied and other schema data remains shared and read-only.
// Errors and cancellation return no schema and confer no catalog coverage.
func DecodeTables(ctx context.Context, db *schemamodel.Database, target string, runtime Runtime) (*schemamodel.Database, error) {
	batch, err := capture(ctx, db, target, runtime)
	if err != nil {
		return nil, err
	}
	var fragments []schemaext.PropertyFragment
	var tables []int
	for i := range batch.database.Tables {
		table := &batch.database.Tables[i]
		for _, definition := range batch.definitions {
			properties, err := takeProperties(table, batch.target, definition)
			if err != nil {
				return nil, err
			}
			if len(properties) == 0 {
				continue
			}
			if slices.Contains(table.Facets.DeclaredKinds(), definition.Kind) {
				return nil, fmt.Errorf("%w: table %q declares both properties and facet %q", schemaext.ErrDuplicate, table.QualifiedName(), definition.Kind)
			}
			fragments = append(fragments, schemaext.PropertyFragment{Kind: definition.Kind, Properties: properties})
			tables = append(tables, i)
		}
	}
	if err := batch.decode(ctx, runtime, fragments, tables); err != nil {
		return nil, err
	}
	return batch.database, nil
}

func (b tableBatch) decode(ctx context.Context, runtime Runtime, fragments []schemaext.PropertyFragment, tables []int) error {
	if len(fragments) == 0 {
		return ctx.Err()
	}
	values, err := runtime.DecodeProperties(ctx, schemaext.PropertyDecodeRequest{
		Target: b.target.Name(), Format: schemaext.TablePlatformProperties, Fragments: fragments,
	})
	if err != nil {
		return err
	}
	if len(values) != len(tables) {
		return fmt.Errorf("%w: table property decoder changed the value count", schemaext.ErrInvalidValue)
	}
	for i, value := range values {
		if value == nil || value.Kind() != fragments[i].Kind {
			return fmt.Errorf("%w: table property decoder changed an ordered kind", schemaext.ErrInvalidValue)
		}
		table := &b.database.Tables[tables[i]]
		table.Facets, err = table.Facets.With(value)
		if err != nil {
			return err
		}
		table.Facets, err = table.Facets.WithTargetScope(value.Kind(), b.target.Name())
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func takeProperties(table *schemamodel.Table, target schemaext.TargetSelection, definition schemaext.PropertyDefinition) (map[string]string, error) {
	properties := make(map[string]string)
	for _, name := range slices.Sorted(maps.Keys(table.Overrides)) {
		if !target.Includes([]string{name}) {
			continue
		}
		for _, key := range definition.Keys {
			value, found := table.Overrides[name][key]
			if !found {
				continue
			}
			if _, duplicate := properties[key]; duplicate {
				return nil, fmt.Errorf("%w: table %q repeats property %q through target aliases", schemaext.ErrDuplicate, table.QualifiedName(), key)
			}
			properties[key] = value
			delete(table.Overrides[name], key)
		}
		if len(table.Overrides[name]) == 0 {
			delete(table.Overrides, name)
		}
	}
	return properties, nil
}
