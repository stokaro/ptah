// Package valuecontext binds model interpretation before source selection or
// dependency collection. It never changes declaration identities.
package valuecontext

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Source returns a bound source without changing the caller's common objects
// or feature collections. Only facet carriers need new slice storage: all
// other common facts remain read-only during binding.
func Source(ctx context.Context, runtime schemaext.ValueContextRuntime, target, address string, database *schemamodel.Database) (*schemamodel.Database, error) {
	if database == nil {
		return nil, nil
	}
	result := *database
	result.Schemas = slices.Clone(database.Schemas)
	result.Tables = slices.Clone(database.Tables)
	result.Fields = slices.Clone(database.Fields)
	result.Indexes = slices.Clone(database.Indexes)
	result.Constraints = slices.Clone(database.Constraints)
	result.Enums = slices.Clone(database.Enums)
	result.Functions = slices.Clone(database.Functions)
	result.Sequences = slices.Clone(database.Sequences)
	result.Views = slices.Clone(database.Views)
	result.MaterializedViews = slices.Clone(database.MaterializedViews)
	result.Triggers = slices.Clone(database.Triggers)
	result.Roles = slices.Clone(database.Roles)
	if err := BindSource(ctx, runtime, target, address, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// BindSource binds the selected target's owned values in database. The caller
// owns database. Failure leaves all collections unchanged; success retains
// source identities, target scopes, and coverage. Without an address, parsing
// retains unresolved values until an explicit binding or consumer refusal.
func BindSource(ctx context.Context, runtime schemaext.ValueContextRuntime, target, address string, database *schemamodel.Database) error {
	if ctx == nil {
		return fmt.Errorf("%w: database binding requires a context", schemaext.ErrInvalidValue)
	}
	if database == nil || address == "" || runtime == nil {
		return ctx.Err()
	}
	bound, err := runtime.ResolveDatabaseContext(ctx, schemaext.DatabaseContextRequest{Target: target, Address: address})
	if err != nil {
		return err
	}
	if bound.IsZero() {
		return nil
	}
	objects, err := Bind(ctx, runtime, target, bound, schemaext.Desired, database.FeatureObjects, database.FacetSlots())
	if err != nil {
		return err
	}
	database.FeatureObjects = objects
	return nil
}

// Bind replaces values only after the complete owner batch succeeds. Values
// outside the selected target retain their captured state and source bindings.
func Bind(ctx context.Context, runtime schemaext.ValueContextRuntime, target string, bound schemaext.DatabaseContext, representation schemaext.Representation, objects schemaext.Objects, slots []*schemaext.Facets) (schemaext.Objects, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return schemaext.Objects{}, err
	}
	selection, err := runtime.ResolveTarget(target)
	if err != nil {
		return schemaext.Objects{}, err
	}
	inputs, values, objectIndexes, facetIndexes, err := selectedValues(selection, objects, slots)
	if err != nil {
		return schemaext.Objects{}, err
	}
	kinds := make([]schemaext.Kind, len(values))
	for i, value := range values {
		kinds[i] = value.Kind()
	}
	resolved, err := runtime.BindValueContexts(ctx, schemaext.ValueContextRequest{Target: selection.Name(), Context: bound, Representation: representation, Values: values})
	if err != nil {
		return schemaext.Objects{}, err
	}
	if len(resolved) != len(values) {
		return schemaext.Objects{}, fmt.Errorf("%w: context binding changed the source inventory", schemaext.ErrInvalidValue)
	}
	for i, value := range resolved {
		if err := schemaext.ValidatePayload(value); err != nil {
			return schemaext.Objects{}, err
		}
		if value.Kind() != kinds[i] {
			return schemaext.Objects{}, fmt.Errorf("%w: context binding reordered the source inventory", schemaext.ErrInvalidValue)
		}
	}
	for i, index := range objectIndexes {
		inputs[index].Value = resolved[i]
	}
	result, err := schemaext.NewObjects(inputs...)
	if err != nil {
		return schemaext.Objects{}, err
	}
	groups := make([]schemaext.Facets, len(slots))
	for i, slot := range slots {
		groups[i] = *slot
	}
	for i, slot := range facetIndexes {
		groups[slot], err = groups[slot].Replace(resolved[len(objectIndexes)+i])
		if err != nil {
			return schemaext.Objects{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.Objects{}, err
	}
	for i, group := range groups {
		*slots[i] = group
	}
	return result, nil
}

func selectedValues(selection schemaext.TargetSelection, objects schemaext.Objects, slots []*schemaext.Facets) ([]schemaext.Object, []schemaext.Value, []int, []int, error) {
	inputs, err := objects.All()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var values []schemaext.Value
	var objectIndexes []int
	for i, object := range inputs {
		if selection.Includes(object.Targets) {
			objectIndexes = append(objectIndexes, i)
			values = append(values, object.Value)
		}
	}
	var facetIndexes []int
	for i, slot := range slots {
		items, err := slot.Values()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		for _, value := range items {
			if selection.Includes(slot.TargetScope(value.Kind())) {
				facetIndexes = append(facetIndexes, i)
				values = append(values, value)
			}
		}
	}
	return inputs, values, objectIndexes, facetIndexes, nil
}
